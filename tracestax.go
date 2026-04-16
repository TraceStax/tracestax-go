// Package tracestax provides a Go SDK for the TraceStax worker-monitoring platform.
//
// Create a Client with New, call Start to begin background delivery, instrument
// your job framework with the provided middleware helpers, and call Close when
// the process shuts down.
package tracestax

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"runtime"
	"strconv"
	"sync"
	"sync/atomic"
	"time"
)

const (
	sdkVersion      = "0.2.0"
	defaultEndpoint = "https://ingest.tracestax.com"
	channelBuffer   = 10_000

	circuitOpenThreshold = 3
	circuitCooldown      = 30 * time.Second
	maxFlushInterval     = 60 * time.Second
)

// TaskEvent is the canonical payload sent for every job lifecycle event.
type TaskEvent struct {
	Framework  string      `json:"framework"`
	Language   string      `json:"language"`
	SDKVersion string      `json:"sdk_version"`
	Type       string      `json:"type"` // "task_event"
	Worker     WorkerInfo  `json:"worker"`
	Task       TaskInfo    `json:"task"`
	Status     string      `json:"status"`
	Metrics    MetricsInfo `json:"metrics"`
	Error      *ErrorInfo  `json:"error,omitempty"`
}

// HeartbeatPayload is sent on worker start / periodic keep-alive.
type HeartbeatPayload struct {
	Framework  string     `json:"framework"`
	Language   string     `json:"language"`
	SDKVersion string     `json:"sdk_version"`
	Timestamp  string     `json:"timestamp"`
	Worker     WorkerInfo `json:"worker"`
	Shutdown   bool       `json:"shutdown,omitempty"`
}

// HeartbeatDirectives contains server-issued instructions from the heartbeat response.
type HeartbeatDirectives struct {
	PauseIngest bool                     `json:"pause_ingest"`
	PauseUntilMs *int64                  `json:"pause_until_ms"`
	Commands    []HeartbeatCommand       `json:"commands"`
}

// HeartbeatCommand is a single server-issued command (e.g. "thread_dump").
type HeartbeatCommand struct {
	ID   string `json:"id"`
	Type string `json:"type"`
}

// DumpPayload is posted to /v1/dump after a thread_dump command.
type DumpPayload struct {
	CmdID      string `json:"cmd_id"`
	WorkerKey  string `json:"worker_key"`
	DumpText   string `json:"dump_text"`
	Language   string `json:"language"`
	SDKVersion string `json:"sdk_version"`
	CapturedAt string `json:"captured_at"`
}

// SnapshotPayload carries a point-in-time view of a single queue's depth and
// activity and is sent to /v1/snapshot.
type SnapshotPayload struct {
	Framework        string  `json:"framework"`
	Language         string  `json:"language"`
	SDKVersion       string  `json:"sdk_version"`
	Timestamp        string  `json:"timestamp"`
	QueueName        string  `json:"queue_name"`
	Depth            int     `json:"depth"`
	ActiveCount      int     `json:"active_count"`
	FailedCount      int     `json:"failed_count"`
	ThroughputPerMin float64 `json:"throughput_per_min,omitempty"`
}

// WorkerInfo describes the process running the jobs.
type WorkerInfo struct {
	Key         string   `json:"key"`
	Hostname    string   `json:"hostname"`
	PID         int      `json:"pid"`
	Concurrency int      `json:"concurrency"`
	Queues      []string `json:"queues"`
}

// TaskInfo describes the individual job being executed.
type TaskInfo struct {
	Name     string `json:"name"`
	ID       string `json:"id"`
	Queue    string `json:"queue"`
	Attempt  int    `json:"attempt"`
	ParentID string `json:"parent_id,omitempty"`
	ChainID  string `json:"chain_id,omitempty"`
}

// MetricsInfo carries timing measurements for the job.
type MetricsInfo struct {
	DurationMS float64 `json:"duration_ms"`
}

// ErrorInfo carries error details when a job fails.
type ErrorInfo struct {
	Type       string `json:"type"`
	Message    string `json:"message"`
	StackTrace string `json:"stack_trace,omitempty"`
}

// envelope is the union type pushed onto the internal channel so that both
// task events and heartbeats can be queued through a single channel.
type envelope struct {
	path string // "/v1/ingest" or "/v1/heartbeat"
	body any
}

// circuitState represents the circuit breaker state.
type circuitState int

const (
	circuitClosed   circuitState = iota
	circuitOpen
	circuitHalfOpen
)

// Client is the core TraceStax SDK client. It owns a buffered channel that
// decouples instrumentation hot-paths from HTTP I/O.
type Client struct {
	apiKey    string
	endpoint  string
	framework string // e.g. "asynq", "river", "faktory", "machinery"
	workerKey string // set by caller for thread dump identification
	httpClient *http.Client
	ch         chan envelope
	done       chan struct{}
	startOnce  sync.Once // guards Start() — prevents duplicate drain goroutines
	closeOnce  sync.Once // guards Close() — prevents double-close channel panic
	enabled    bool // RUN-140: when false, all operations are no-ops
	dryRun     bool // RUN-140: when true, log to stderr instead of sending

	// Resilience state
	mu                  sync.Mutex
	consecutiveFailures int
	circuit             circuitState
	circuitOpenedAt     time.Time
	pauseUntil          time.Time
	// Grows exponentially on consecutive failures (up to maxFlushInterval),
	// halves back toward zero on success. The drain goroutine sleeps for this
	// duration between dispatches so a dead backend is not hammered.
	currentDrainDelay   time.Duration

	// Metrics (accessed via atomic ops — no lock needed)
	droppedEvents int64
}

// ClientStats is a snapshot of the client's internal health metrics.
type ClientStats struct {
	QueueLen            int
	DroppedEvents       int64
	CircuitState        string
	ConsecutiveFailures int
}

// Option is a functional option for Client configuration.
type Option func(*Client)

// WithEndpoint overrides the default TraceStax ingest endpoint.
func WithEndpoint(u string) Option {
	return func(c *Client) { c.endpoint = u }
}

// WithTimeout overrides the HTTP request timeout (default: 10 s).
func WithTimeout(d time.Duration) Option {
	return func(c *Client) { c.httpClient.Timeout = d }
}

// WithEnabled explicitly enables or disables the client (RUN-140).
func WithEnabled(v bool) Option {
	return func(c *Client) { c.enabled = v }
}

// WithDryRun logs payloads to stderr instead of sending them (RUN-140).
func WithDryRun(v bool) Option {
	return func(c *Client) { c.dryRun = v }
}

// WithFramework sets the framework identifier (e.g. "asynq", "river", "faktory").
func WithFramework(f string) Option {
	return func(c *Client) { c.framework = f }
}

// WithWorkerKey sets the worker key used in thread dump payloads.
func WithWorkerKey(k string) Option {
	return func(c *Client) { c.workerKey = k }
}

// New creates a new Client. Call Start to begin background delivery.
// Respects TRACESTAX_ENABLED and TRACESTAX_DRY_RUN environment variables.
func New(apiKey string, opts ...Option) *Client {
	c := &Client{
		apiKey:    apiKey,
		endpoint:  defaultEndpoint,
		framework: "asynq",
		httpClient: &http.Client{
			Timeout: 10 * time.Second,
		},
		ch:      make(chan envelope, channelBuffer),
		done:    make(chan struct{}),
		enabled: os.Getenv("TRACESTAX_ENABLED") != "false",
		dryRun:  os.Getenv("TRACESTAX_DRY_RUN") == "true",
	}
	for _, o := range opts {
		o(c)
	}
	if c.apiKey == "" && c.enabled && !c.dryRun {
		fmt.Fprintln(os.Stderr, "tracestax: warning: empty API key — events will be rejected by the server")
	}
	return c
}

// Start launches the background goroutine that drains the channel and forwards
// payloads to the TraceStax API. It is safe to call more than once (subsequent
// calls are no-ops after the first).
func (c *Client) Start() {
	c.startOnce.Do(func() { go c.drain() })
}

// Close signals the background goroutine to stop and blocks until the channel
// is fully flushed or the flush deadline (5 s) expires. Safe to call more than
// once — subsequent calls are no-ops.
func (c *Client) Close() {
	c.closeOnce.Do(func() { close(c.done) })

	// Drain any remaining items in the channel ourselves, since the
	// background goroutine may have already exited.
	deadline := time.After(5 * time.Second)
	for {
		select {
		case <-deadline:
			return
		case env, ok := <-c.ch:
			if !ok {
				return // channel closed
			}
			if _, err := c.postForJSON(env.path, env.body); err != nil {
				fmt.Fprintln(os.Stderr, "tracestax:", err)
			}
		default:
			// Channel empty — we're done
			return
		}
	}
}

// send enqueues a payload for async delivery. If the channel is full the
// payload is silently dropped to avoid blocking the caller.
func (c *Client) send(path string, body any) {
	if !c.enabled {
		return
	}
	if c.dryRun {
		data, err := json.Marshal(body)
		if err != nil {
			fmt.Fprintf(os.Stderr, "[tracestax dry-run] %s: [payload not serializable: %v]\n", path, err)
		} else {
			fmt.Fprintf(os.Stderr, "[tracestax dry-run] %s: %s\n", path, data)
		}
		return
	}

	// Check circuit breaker — drop immediately if open
	c.mu.Lock()
	state := c.circuit
	openedAt := c.circuitOpenedAt
	c.mu.Unlock()

	if state == circuitOpen {
		if time.Since(openedAt) < circuitCooldown {
			return // silent drop
		}
		c.mu.Lock()
		c.circuit = circuitHalfOpen
		c.mu.Unlock()
	}

	env := envelope{path: path, body: body}
	select {
	case c.ch <- env:
	default:
		// Channel full — trim the oldest 5,000 events to make room, then re-enqueue.
		// This matches the SAFETY.md contract: oldest events are dropped, not new ones.
		drained := 0
	trimLoop:
		for drained < 5_000 {
			select {
			case <-c.ch:
				drained++
			default:
				break trimLoop
			}
		}
		atomic.AddInt64(&c.droppedEvents, int64(drained))
		if drained > 0 {
			fmt.Fprintf(os.Stderr, "tracestax: queue full, dropped %d oldest events\n", drained)
		}
		select {
		case c.ch <- env:
		default:
			// Still full (another goroutine raced to fill it) — drop this event.
			atomic.AddInt64(&c.droppedEvents, 1)
			fmt.Fprintln(os.Stderr, "tracestax: channel full after trim, dropping event")
		}
	}
}

// Stats returns a snapshot of the client's internal health metrics.
// Safe to call from any goroutine.
func (c *Client) Stats() ClientStats {
	c.mu.Lock()
	state := c.circuit
	failures := c.consecutiveFailures
	c.mu.Unlock()

	var stateStr string
	switch state {
	case circuitClosed:
		stateStr = "closed"
	case circuitOpen:
		stateStr = "open"
	case circuitHalfOpen:
		stateStr = "half_open"
	}

	return ClientStats{
		QueueLen:            len(c.ch),
		DroppedEvents:       atomic.LoadInt64(&c.droppedEvents),
		CircuitState:        stateStr,
		ConsecutiveFailures: failures,
	}
}

// SendEvent enqueues a task_event payload for delivery to /v1/ingest.
func (c *Client) SendEvent(ev TaskEvent) {
	c.send("/v1/ingest", ev)
}

// SendHeartbeat enqueues a heartbeat payload for async delivery.
// Use HeartbeatSync if you need to act on the server's directives.
func (c *Client) SendHeartbeat(hb HeartbeatPayload) {
	c.send("/v1/heartbeat", hb)
}

// HeartbeatSync sends a heartbeat synchronously and returns the server directives.
// Returns nil directives on error (non-blocking guarantee upheld by the caller).
func (c *Client) HeartbeatSync(hb HeartbeatPayload) (*HeartbeatDirectives, error) {
	if !c.enabled || c.dryRun {
		return nil, nil
	}
	body, err := c.postForJSON("/v1/heartbeat", hb)
	if err != nil {
		return nil, err
	}
	if body == nil {
		return nil, nil
	}
	// Parse directives out of the raw response map
	raw, ok := body.(map[string]any)
	if !ok {
		return nil, nil
	}
	d := &HeartbeatDirectives{}
	if dRaw, ok := raw["directives"].(map[string]any); ok {
		if pi, ok := dRaw["pause_ingest"].(bool); ok {
			d.PauseIngest = pi
		}
		if pum, ok := dRaw["pause_until_ms"].(float64); ok {
			v := int64(pum)
			d.PauseUntilMs = &v
		}
		if cmdsRaw, ok := dRaw["commands"].([]any); ok {
			for _, cr := range cmdsRaw {
				if cm, ok := cr.(map[string]any); ok {
					id, _ := cm["id"].(string)
					typ, _ := cm["type"].(string)
					if id != "" && typ != "" {
						d.Commands = append(d.Commands, HeartbeatCommand{ID: id, Type: typ})
					}
				}
			}
		}
	}
	return d, nil
}

// SetPauseUntil pauses ingest delivery until the given epoch millisecond timestamp.
func (c *Client) SetPauseUntil(epochMs int64) {
	c.mu.Lock()
	c.pauseUntil = time.UnixMilli(epochMs)
	c.mu.Unlock()
}

// ExecuteCommand executes a server-issued command. Currently supports "thread_dump".
func (c *Client) ExecuteCommand(cmd HeartbeatCommand) {
	if cmd.Type != "thread_dump" {
		return
	}
	wk := c.workerKey
	if wk == "" {
		h, _ := os.Hostname()
		wk = fmt.Sprintf("go:%s:%d", h, os.Getpid())
	}
	dump := captureGoroutineDump()
	_, _ = c.postForJSON("/v1/dump", DumpPayload{
		CmdID:      cmd.ID,
		WorkerKey:  wk,
		DumpText:   dump,
		Language:   "go",
		SDKVersion: sdkVersion,
		CapturedAt: time.Now().UTC().Format(time.RFC3339),
	})
}

// Heartbeat builds and enqueues a heartbeat for the given worker. It is a
// convenience wrapper around SendHeartbeat that populates all required fields.
// The call returns immediately; delivery happens in the background goroutine.
func (c *Client) Heartbeat(workerKey, hostname string, pid int, queues []string, concurrency int) {
	c.send("/v1/heartbeat", HeartbeatPayload{
		Framework:  c.framework,
		Language:   "go",
		SDKVersion: sdkVersion,
		Timestamp:  time.Now().UTC().Format(time.RFC3339),
		Worker: WorkerInfo{
			Key:         workerKey,
			Hostname:    hostname,
			PID:         pid,
			Queues:      queues,
			Concurrency: concurrency,
		},
	})
}

// Snapshot enqueues a queue-depth snapshot for delivery to /v1/snapshot.
// The call returns immediately; delivery happens in the background goroutine.
func (c *Client) Snapshot(queueName string, depth, activeCount, failedCount int) {
	c.send("/v1/snapshot", SnapshotPayload{
		Framework:   c.framework,
		Language:    "go",
		SDKVersion:  sdkVersion,
		Timestamp:   time.Now().UTC().Format(time.RFC3339),
		QueueName:   queueName,
		Depth:       depth,
		ActiveCount: activeCount,
		FailedCount: failedCount,
	})
}

// drain is the background goroutine that serialises and POSTs all enqueued
// payloads. It exits when done is closed and the channel is drained.
func (c *Client) drain() {
	defer func() {
		if r := recover(); r != nil {
			fmt.Fprintln(os.Stderr, "tracestax: panic in drain goroutine, restarting:", r)
			go c.drain()
		}
	}()
	for {
		// Check backpressure pause BEFORE receiving from channel.
		// Receiving and then discarding would permanently lose the event.
		c.mu.Lock()
		paused := !c.pauseUntil.IsZero() && time.Now().Before(c.pauseUntil)
		c.mu.Unlock()
		if paused {
			select {
			case <-c.done:
				return
			case <-time.After(500 * time.Millisecond):
			}
			continue
		}

		select {
		case env := <-c.ch:
			if _, err := c.postForJSON(env.path, env.body); err != nil {
				fmt.Fprintln(os.Stderr, "tracestax:", err)
				// Sleep for the adaptive backoff delay before processing the next
				// item. Reading the delay under the lock, then sleeping without it,
				// avoids blocking other goroutines during the wait.
				c.mu.Lock()
				delay := c.currentDrainDelay
				c.mu.Unlock()
				if delay > 0 {
					select {
					case <-c.done:
						return
					case <-time.After(delay):
					}
				}
			}
		case <-c.done:
			// Flush whatever remains in the channel before exiting.
			for {
				select {
				case env := <-c.ch:
					if _, err := c.postForJSON(env.path, env.body); err != nil {
						fmt.Fprintln(os.Stderr, "tracestax:", err)
					}
				default:
					return
				}
			}
		}
	}
}

// postForJSON serialises body as JSON, POSTs it, and returns the parsed
// response body as map[string]any. All errors are returned to the caller.
func (c *Client) postForJSON(path string, body any) (any, error) {
	data, err := json.Marshal(body)
	if err != nil {
		return nil, fmt.Errorf("marshal: %w", err)
	}
	// Guard against huge payloads that could waste bandwidth or cause
	// server-side rejection. 512 KB matches every other SDK's limit.
	if len(data) > 512*1024 {
		return nil, fmt.Errorf("payload exceeds 512 KB (%d bytes), dropping", len(data))
	}

	url := c.endpoint + path
	req, err := http.NewRequestWithContext(context.Background(), http.MethodPost, url, bytes.NewReader(data))
	if err != nil {
		return nil, fmt.Errorf("build request: %w", err)
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Authorization", "Bearer "+c.apiKey)
	req.Header.Set("User-Agent", "tracestax-go/"+sdkVersion)

	resp, err := c.httpClient.Do(req)
	if err != nil {
		c.recordFailure(0)
		return nil, fmt.Errorf("http post %s: %w", path, err)
	}
	defer resp.Body.Close()

	// Honor X-Retry-After
	if ra := resp.Header.Get("X-Retry-After"); ra != "" {
		if secs, err2 := strconv.Atoi(ra); err2 == nil && secs > 0 {
			c.mu.Lock()
			c.pauseUntil = time.Now().Add(time.Duration(secs) * time.Second)
			c.mu.Unlock()
		}
	}

	if resp.StatusCode == 401 {
		fmt.Fprintln(os.Stderr, "tracestax: auth failed (401) – check your API key; events will continue to queue")
		return nil, fmt.Errorf("http post %s: status 401 (auth failure)", path)
	}
	if resp.StatusCode >= 400 {
		c.recordFailure(0)
		return nil, fmt.Errorf("http post %s: status %d", path, resp.StatusCode)
	}

	c.recordSuccess()

	respData, _ := io.ReadAll(io.LimitReader(resp.Body, 1<<20)) // cap at 1 MB
	var result any
	_ = json.Unmarshal(respData, &result)
	return result, nil
}

func (c *Client) recordSuccess() {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.consecutiveFailures = 0
	c.circuit = circuitClosed
	if c.currentDrainDelay > 0 {
		c.currentDrainDelay /= 2
	}
}

func (c *Client) recordFailure(retryAfterSecs int) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if retryAfterSecs > 0 {
		c.pauseUntil = time.Now().Add(time.Duration(retryAfterSecs) * time.Second)
	}
	c.consecutiveFailures++
	// Adaptive backoff: start at 500 ms, double on each failure, cap at maxFlushInterval.
	if c.currentDrainDelay == 0 {
		c.currentDrainDelay = 500 * time.Millisecond
	} else if c.currentDrainDelay < maxFlushInterval {
		c.currentDrainDelay *= 2
		if c.currentDrainDelay > maxFlushInterval {
			c.currentDrainDelay = maxFlushInterval
		}
	}
	if c.consecutiveFailures >= circuitOpenThreshold && c.circuit == circuitClosed {
		c.circuit = circuitOpen
		c.circuitOpenedAt = time.Now()
		fmt.Fprintln(os.Stderr, "tracestax: unreachable, circuit open, events dropped")
	} else if c.circuit == circuitHalfOpen {
		c.circuit = circuitOpen
		c.circuitOpenedAt = time.Now()
	}
}

// captureGoroutineDump captures all goroutine stack traces using runtime.Stack.
func captureGoroutineDump() string {
	buf := make([]byte, 1<<20) // 1 MB
	n := runtime.Stack(buf, true)
	return string(buf[:n])
}

// captureCallerStack captures the current goroutine's stack trace. Used by
// framework middlewares to attach a stack to error events — Go errors don't
// carry stack traces natively, so we snapshot at the point the middleware
// observes the failure. The trace shows the call chain that returned the error.
func captureCallerStack() string {
	buf := make([]byte, 16384) // 16 KB — single goroutine stacks are small
	n := runtime.Stack(buf, false)
	return string(buf[:n])
}
