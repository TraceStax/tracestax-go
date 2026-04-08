package tracestax

import (
	"net/http"
	"net/http/httptest"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func TestNewClientDefaults(t *testing.T) {
	c := New("test-key")
	if c.framework != "asynq" {
		t.Errorf("expected default framework 'asynq', got '%s'", c.framework)
	}
	if !c.enabled {
		t.Error("expected enabled=true by default")
	}
	if c.dryRun {
		t.Error("expected dryRun=false by default")
	}
}

func TestWithFrameworkOption(t *testing.T) {
	c := New("test-key", WithFramework("river"))
	if c.framework != "river" {
		t.Errorf("expected framework 'river', got '%s'", c.framework)
	}
}

func TestDisabledClientDoesNotSend(t *testing.T) {
	c := New("test-key", WithEnabled(false))
	c.Start()
	defer c.Close()
	c.SendEvent(TaskEvent{Type: "task_event"})
	// Should not panic or send
	if len(c.ch) != 0 {
		t.Error("expected no events in channel when disabled")
	}
}

func TestDryRunClientDoesNotSend(t *testing.T) {
	c := New("test-key", WithDryRun(true))
	c.Start()
	defer c.Close()
	c.SendEvent(TaskEvent{Type: "task_event"})
	if len(c.ch) != 0 {
		t.Error("expected no events in channel when dry-run")
	}
}

func TestCloseFlushesChannel(t *testing.T) {
	c := New("test-key", WithEnabled(false))
	c.Close()
	// Should not panic on double close or close without start
}

// ── Fire-and-forget guarantee ─────────────────────────────────────────────────

// TestSendEventNeverBlocks verifies that SendEvent returns immediately even
// when the channel is full (backpressure protection).
func TestSendEventNeverBlocks(t *testing.T) {
	// Use a server that hangs to keep channel items from draining
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		time.Sleep(10 * time.Second) // simulate slow server
	}))
	defer ts.Close()

	c := New("test-key", WithEndpoint(ts.URL), WithTimeout(100*time.Millisecond))
	c.Start()
	defer c.Close()

	// Fill the channel buffer (channelBuffer = 10,000 slots) plus overflow — must never block
	done := make(chan struct{})
	go func() {
		for i := 0; i < channelBuffer+100; i++ {
			c.SendEvent(TaskEvent{Type: "task_event", Status: "started"})
		}
		close(done)
	}()

	select {
	case <-done:
		// pass
	case <-time.After(2 * time.Second):
		t.Error("SendEvent blocked for more than 2 seconds — caller latency violated")
	}
}

// TestSendEventNeverPanics sends 1000 events from concurrent goroutines and
// checks for data races (run with go test -race).
func TestSendEventNeverPanics(t *testing.T) {
	var serverErr int32
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusServiceUnavailable)
	}))
	defer ts.Close()

	c := New("test-key", WithEndpoint(ts.URL), WithTimeout(200*time.Millisecond))
	c.Start()

	var wg sync.WaitGroup
	for g := 0; g < 10; g++ {
		wg.Add(1)
		go func(n int) {
			defer wg.Done()
			for i := 0; i < 100; i++ {
				// Must not panic
				func() {
					defer func() {
						if r := recover(); r != nil {
							atomic.AddInt32(&serverErr, 1)
						}
					}()
					c.SendEvent(TaskEvent{
						Type:   "task_event",
						Status: "started",
					})
				}()
			}
		}(g)
	}
	wg.Wait()
	c.Close()

	if atomic.LoadInt32(&serverErr) > 0 {
		t.Errorf("SendEvent panicked %d times during concurrent use", serverErr)
	}
}

// ── Circuit breaker ───────────────────────────────────────────────────────────

// TestCircuitBreakerOpens verifies that after 3 consecutive HTTP failures the
// circuit opens and subsequent SendEvent calls are dropped immediately without
// hitting the HTTP server.
func TestCircuitBreakerOpens(t *testing.T) {
	var reqCount int32

	// Signal channel: written to after response is sent so we know the client
	// has received the failure and had time to call recordFailure.
	done := make(chan struct{}, 20)

	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusServiceUnavailable)
		done <- struct{}{}
	}))
	defer ts.Close()

	c := New("test-key", WithEndpoint(ts.URL), WithTimeout(500*time.Millisecond))
	c.Start()

	// Send 3 events and wait for all 3 HTTP requests to complete
	for i := 0; i < 3; i++ {
		c.SendEvent(TaskEvent{Type: "task_event", Status: "started"})
	}
	for i := 0; i < 3; i++ {
		select {
		case <-done:
		case <-time.After(3 * time.Second):
			t.Fatal("timeout waiting for HTTP request to be processed")
		}
	}
	// Give recordFailure a moment to update circuit state
	time.Sleep(20 * time.Millisecond)

	atomic.StoreInt32(&reqCount, 0)

	// Circuit should be OPEN — next event must be dropped in send() itself
	c.mu.Lock()
	state := c.circuit
	c.mu.Unlock()

	if state != circuitOpen {
		t.Errorf("expected circuit OPEN after 3 failures, got state=%d", state)
	}

	// Verify: sending an event does NOT put it on the channel
	before := len(c.ch)
	c.SendEvent(TaskEvent{Type: "task_event", Status: "started"})
	after := len(c.ch)

	if after > before {
		t.Errorf("expected event to be dropped (circuit open), but channel grew from %d to %d", before, after)
	}

	c.Close()
}

// TestCircuitBreakerResetsAfterCooldown verifies the OPEN → HALF_OPEN → CLOSED
// transition using direct state manipulation to avoid a 30s sleep.
func TestCircuitBreakerResetsAfterCooldown(t *testing.T) {
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		w.Write([]byte(`{"ok":true}`))
	}))
	defer ts.Close()

	c := New("test-key", WithEndpoint(ts.URL), WithTimeout(500*time.Millisecond))

	// Manually put the circuit into OPEN state with an expired cooldown
	c.mu.Lock()
	c.circuit = circuitOpen
	c.circuitOpenedAt = time.Now().Add(-31 * time.Second) // past cooldown
	c.consecutiveFailures = 3
	c.mu.Unlock()

	// Verify send() transitions to HALF_OPEN and lets the event through
	c.Start()
	c.SendEvent(TaskEvent{Type: "task_event", Status: "started"})

	// Give drain goroutine time to process
	time.Sleep(200 * time.Millisecond)

	c.mu.Lock()
	state := c.circuit
	failures := c.consecutiveFailures
	c.mu.Unlock()

	if state != circuitClosed {
		t.Errorf("expected circuit CLOSED after successful probe, got state=%d", state)
	}
	if failures != 0 {
		t.Errorf("expected 0 consecutive failures after reset, got %d", failures)
	}

	c.Close()
}

// TestCircuitBreakerHalfOpenFailure verifies that a failed HALF_OPEN probe
// sends the circuit back to OPEN.
func TestCircuitBreakerHalfOpenFailure(t *testing.T) {
	reqProcessed := make(chan struct{}, 1)
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusServiceUnavailable)
		reqProcessed <- struct{}{}
	}))
	defer ts.Close()

	c := New("test-key", WithEndpoint(ts.URL), WithTimeout(500*time.Millisecond))

	// Start in HALF_OPEN — next send() will not be dropped, probe will fail
	c.mu.Lock()
	c.circuit = circuitHalfOpen
	c.mu.Unlock()

	c.Start()
	c.SendEvent(TaskEvent{Type: "task_event", Status: "started"})

	select {
	case <-reqProcessed:
	case <-time.After(3 * time.Second):
		t.Fatal("timeout waiting for probe request")
	}

	time.Sleep(20 * time.Millisecond) // let recordFailure run

	c.mu.Lock()
	state := c.circuit
	c.mu.Unlock()

	if state != circuitOpen {
		t.Errorf("expected circuit OPEN after failed probe, got state=%d", state)
	}

	c.Close()
}

// TestStats_InitialState verifies Stats() returns correct values before any traffic.
func TestStats_InitialState(t *testing.T) {
	c := New("test-key")
	s := c.Stats()
	if s.CircuitState != "closed" {
		t.Errorf("expected circuit closed, got %q", s.CircuitState)
	}
	if s.DroppedEvents != 0 {
		t.Errorf("expected 0 dropped events, got %d", s.DroppedEvents)
	}
	if s.ConsecutiveFailures != 0 {
		t.Errorf("expected 0 consecutive failures, got %d", s.ConsecutiveFailures)
	}
}

// TestStats_DropCounterIncrements verifies droppedEvents increments when channel is full.
func TestStats_DropCounterIncrements(t *testing.T) {
	c := New("test-key") // no Start() — drain goroutine never runs

	// Fill the channel to capacity
	for i := 0; i < channelBuffer; i++ {
		c.ch <- envelope{path: "/v1/ingest", body: nil}
	}

	// Send one more — must be dropped and counted
	c.SendEvent(TaskEvent{Type: "task_event"})

	s := c.Stats()
	if s.DroppedEvents <= 0 {
		t.Errorf("expected DroppedEvents>0, got %d", s.DroppedEvents)
	}

	// Drain so Close() can finish cleanly
	for len(c.ch) > 0 {
		<-c.ch
	}
	c.Close()
}

// ── Idempotency & double-call safety (C1, C2 from audit) ─────────────────────

// TestStartIdempotent verifies that calling Start() more than once launches
// only a single drain goroutine, not one per call. Two goroutines racing on
// the same channel would cause double-sends detectable under -race.
func TestStartIdempotent(t *testing.T) {
	var requestCount int32
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		atomic.AddInt32(&requestCount, 1)
		w.WriteHeader(http.StatusOK)
		w.Write([]byte(`{}`))
	}))
	defer ts.Close()

	c := New("test-key", WithEndpoint(ts.URL), WithTimeout(500*time.Millisecond))

	// Call Start() three times — should still spawn exactly one drain goroutine
	c.Start()
	c.Start()
	c.Start()
	defer c.Close()

	c.SendEvent(TaskEvent{Type: "task_event", Status: "started"})

	// Allow the event to be delivered
	time.Sleep(200 * time.Millisecond)

	// Exactly 1 HTTP request expected — if multiple goroutines drained the
	// channel the event would still only be sent once (channel semantics), but
	// the -race flag would catch the data race.
	count := atomic.LoadInt32(&requestCount)
	if count != 1 {
		t.Errorf("expected exactly 1 HTTP request, got %d", count)
	}
}

// TestCloseIdempotent verifies that calling Close() more than once does not
// panic (no double-close on the done channel).
func TestCloseIdempotent(t *testing.T) {
	c := New("test-key", WithEnabled(false))
	c.Start()

	// Must not panic — before the fix this was: close(c.done) → panic
	defer func() {
		if r := recover(); r != nil {
			t.Errorf("Close() panicked on double-call: %v", r)
		}
	}()

	c.Close()
	c.Close() // second call must be a no-op
	c.Close() // third call for good measure
}

// TestBackpressureEventsNotLost verifies that events sent while the client is
// paused (X-Retry-After) are NOT dropped — they stay in the channel until the
// pause window expires (C3 from audit).
func TestBackpressureEventsNotLost(t *testing.T) {
	var requestBodies []string
	var mu sync.Mutex
	requestCount := 0

	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		requestCount++
		mu.Unlock()
		w.WriteHeader(http.StatusOK)
		w.Write([]byte(`{}`))
	}))
	defer ts.Close()

	c := New("test-key", WithEndpoint(ts.URL), WithTimeout(500*time.Millisecond))
	_ = requestBodies

	// Set a 200ms backpressure pause
	c.mu.Lock()
	c.pauseUntil = time.Now().Add(200 * time.Millisecond)
	c.mu.Unlock()

	c.Start()
	defer c.Close()

	// Send an event while paused — it must land in the channel, not be dropped
	c.SendEvent(TaskEvent{Type: "task_event", Status: "started"})

	// Channel should hold the event (not discarded)
	if len(c.ch) == 0 {
		t.Error("event was dropped during backpressure pause; expected it to remain in channel")
	}

	// After the pause expires the event should be delivered.
	// The drain loop polls every 500ms, so allow 1s for the pause to expire
	// (200ms) and the delivery to complete.
	time.Sleep(1000 * time.Millisecond)

	mu.Lock()
	count := requestCount
	mu.Unlock()

	if count == 0 {
		t.Error("expected event to be delivered after backpressure window expired, got 0 requests")
	}
}

// TestChannelFullDropsWithoutBlocking verifies the "channel full → drop" path
// does not deadlock or panic.
func TestChannelFullDropsWithoutBlocking(t *testing.T) {
	// enabled=true but no Start() — drain goroutine never runs
	c := New("test-key")

	// Fill the channel to capacity
	for i := 0; i < channelBuffer; i++ {
		c.ch <- envelope{path: "/v1/ingest", body: nil}
	}

	// SendEvent on a full channel must drop silently and return immediately
	done := make(chan struct{})
	go func() {
		c.SendEvent(TaskEvent{Type: "task_event"})
		close(done)
	}()

	select {
	case <-done:
		// pass — did not block
	case <-time.After(1 * time.Second):
		t.Error("SendEvent blocked when channel was full")
	}

	// Drain so Close() can finish
	for len(c.ch) > 0 {
		<-c.ch
	}
	c.Close()
}

// ── Panic recovery ────────────────────────────────────────────────────────────

// panicRoundTripper is an http.RoundTripper that panics on the first call, then
// delegates to a real server on subsequent calls. This lets us verify that the
// drain goroutine recovers from a panic and resumes delivery.
type panicRoundTripper struct {
	delegate http.RoundTripper
	panicked int32 // atomic: 0 = not yet, 1 = already panicked
}

func (p *panicRoundTripper) RoundTrip(req *http.Request) (*http.Response, error) {
	if atomic.CompareAndSwapInt32(&p.panicked, 0, 1) {
		panic("simulated panic in RoundTripper")
	}
	return p.delegate.RoundTrip(req)
}

// TestDrainGoroutinePanicRecovery verifies that a panic inside the drain
// goroutine is recovered, the goroutine restarts, and subsequent events are
// still delivered successfully.
func TestDrainGoroutinePanicRecovery(t *testing.T) {
	delivered := make(chan struct{}, 10)

	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{}`))
		delivered <- struct{}{}
	}))
	defer ts.Close()

	prt := &panicRoundTripper{delegate: http.DefaultTransport}
	c := &Client{
		apiKey:   "test-key",
		endpoint: ts.URL,
		httpClient: &http.Client{
			Timeout:   2 * time.Second,
			Transport: prt,
		},
		ch:      make(chan envelope, channelBuffer),
		done:    make(chan struct{}),
		enabled: true,
	}
	c.Start()

	// First event will trigger the panic; drain goroutine must restart
	c.SendEvent(TaskEvent{Type: "task_event", Status: "started"})

	// Wait for the panic + restart to settle, then send a second event
	time.Sleep(200 * time.Millisecond)
	c.SendEvent(TaskEvent{Type: "task_event", Status: "completed"})

	// At least one event must be delivered after the panic recovery
	select {
	case <-delivered:
		// pass — delivery resumed after panic
	case <-time.After(5 * time.Second):
		t.Error("drain goroutine did not recover from panic; no events delivered")
	}

	c.Close()
}

// TestLargePayloadDropped verifies that a JSON-serialized payload exceeding
// 512 KB is silently dropped by postForJSON without panicking or crashing.
func TestLargePayloadDropped(t *testing.T) {
	// Build a server that tracks whether it received any request.
	received := make(chan struct{}, 1)
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		received <- struct{}{}
		w.WriteHeader(http.StatusOK)
	}))
	defer ts.Close()

	c := New("test-key", WithEndpoint(ts.URL))
	c.Start()

	// Build an event whose JSON representation exceeds 512 KB.
	bigStr := make([]byte, 600*1024)
	for i := range bigStr {
		bigStr[i] = 'a'
	}
	ev := TaskEvent{
		Framework:  "asynq",
		Language:   "go",
		SDKVersion: "0.1.0",
		Type:       "task_event",
		Worker:     WorkerInfo{Key: "w1", Hostname: "h", PID: 1, Concurrency: 1, Queues: []string{"default"}},
		Task:       TaskInfo{Name: string(bigStr), ID: "id1", Queue: "default", Attempt: 1},
		Status:     "started",
		Metrics:    MetricsInfo{DurationMS: 0},
	}

	// SendEvent must not panic.
	c.SendEvent(ev)

	// Allow the drain goroutine a moment to process.
	time.Sleep(200 * time.Millisecond)
	c.Close()

	// The server must NOT have received the oversized payload.
	select {
	case <-received:
		t.Error("server received an oversized payload — 512 KB guard not working")
	default:
		// correct: event was dropped before HTTP call
	}
}

// TestAuth401DoesNotOpenCircuitBreaker verifies that a 401 Unauthorized response is
// NOT counted as a circuit-breaker failure. A 401 is a permanent misconfiguration
// (wrong API key), not a transient network error. If the circuit opened on 401 it
// would silently drop all events and hide the real problem.
func TestAuth401DoesNotOpenCircuitBreaker(t *testing.T) {
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusUnauthorized)
	}))
	defer ts.Close()

	c := New("bad-key", WithEndpoint(ts.URL))
	c.Start()

	ids := []string{"run-1", "run-2", "run-3"}
	for _, id := range ids {
		c.SendEvent(TaskEvent{
			Type: "task_event", Status: "started",
			Worker:  WorkerInfo{Key: "w", Queues: []string{"q"}},
			Task:    TaskInfo{Name: "j", ID: id, Queue: "q", Attempt: 1},
			Metrics: MetricsInfo{},
		})
	}
	c.Close()

	stats := c.Stats()
	if stats.CircuitState != "closed" {
		t.Errorf("circuit state = %q after 401 responses; want \"closed\"", stats.CircuitState)
	}
	if stats.ConsecutiveFailures != 0 {
		t.Errorf("consecutive_failures = %d after 401 responses; want 0", stats.ConsecutiveFailures)
	}
}

// TestConcurrentClose verifies that calling Close() concurrently from multiple
// goroutines does not panic or deadlock.
func TestConcurrentClose(t *testing.T) {
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))
	defer ts.Close()

	c := New("test-key", WithEndpoint(ts.URL))
	c.Start()
	c.SendEvent(TaskEvent{Type: "task_event", Status: "started",
		Worker: WorkerInfo{Key: "w", Queues: []string{"q"}},
		Task:   TaskInfo{Name: "j", ID: "1", Queue: "q", Attempt: 1},
		Metrics: MetricsInfo{},
	})

	var wg sync.WaitGroup
	for i := 0; i < 3; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			c.Close() // must not panic
		}()
	}
	wg.Wait()
}
