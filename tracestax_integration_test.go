package tracestax

import (
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"strings"
	"testing"
	"time"
)

// ingestURL returns the mock-ingest base URL or skips the test if not configured.
func ingestURL(t *testing.T) string {
	t.Helper()
	u := os.Getenv("TRACESTAX_INGEST_URL")
	if u == "" {
		t.Skip("TRACESTAX_INGEST_URL not set — skipping integration test")
	}
	return strings.TrimRight(u, "/")
}

func resetIngest(t *testing.T, base string) {
	t.Helper()
	resp, err := http.Post(base+"/test/reset", "application/json", strings.NewReader("{}"))
	if err != nil {
		t.Fatalf("reset ingest: %v", err)
	}
	resp.Body.Close()
}

func fetchIngestEvents(t *testing.T, base string) []map[string]any {
	t.Helper()
	resp, err := http.Get(base + "/test/events")
	if err != nil {
		t.Fatalf("fetch events: %v", err)
	}
	defer resp.Body.Close()
	body, _ := io.ReadAll(resp.Body)
	var events []map[string]any
	if err := json.Unmarshal(body, &events); err != nil {
		t.Fatalf("decode events: %v — body: %s", err, body)
	}
	return events
}

func fetchIngestHeartbeats(t *testing.T, base string) []map[string]any {
	t.Helper()
	resp, err := http.Get(base + "/test/heartbeats")
	if err != nil {
		t.Fatalf("fetch heartbeats: %v", err)
	}
	defer resp.Body.Close()
	body, _ := io.ReadAll(resp.Body)
	var hbs []map[string]any
	if err := json.Unmarshal(body, &hbs); err != nil {
		t.Fatalf("decode heartbeats: %v — body: %s", err, body)
	}
	return hbs
}

// TestIntegration_SendEvent verifies that a task event reaches mock-ingest.
func TestIntegration_SendEvent(t *testing.T) {
	base := ingestURL(t)
	resetIngest(t, base)

	c := New("ts_test_abc", WithEndpoint(base), WithFramework("asynq"))
	c.Start()

	c.SendEvent(TaskEvent{
		Framework:  "asynq",
		Language:   "go",
		SDKVersion: sdkVersion,
		Type:       "task_event",
		Status:     "succeeded",
		Worker:     WorkerInfo{Key: "worker-1", Hostname: "test-host", PID: 1, Queues: []string{"default"}, Concurrency: 4},
		Task:       TaskInfo{Name: "ProcessOrderJob", ID: "job-int-001", Queue: "default", Attempt: 1},
		Metrics:    MetricsInfo{DurationMS: 123.0},
	})

	c.Close()

	events := fetchIngestEvents(t, base)
	if len(events) == 0 {
		t.Fatal("expected at least one event in mock-ingest, got none")
	}

	found := false
	for _, ev := range events {
		if task, ok := ev["task"].(map[string]any); ok {
			if task["name"] == "ProcessOrderJob" {
				found = true
				break
			}
		}
	}
	if !found {
		t.Errorf("ProcessOrderJob not found in events: %v", events)
	}
}

// TestIntegration_SendHeartbeat verifies that a heartbeat reaches mock-ingest.
func TestIntegration_SendHeartbeat(t *testing.T) {
	base := ingestURL(t)
	resetIngest(t, base)

	c := New("ts_test_abc", WithEndpoint(base), WithFramework("asynq"))
	c.Start()

	c.Heartbeat("worker-hb-1", "integration-host", 42, []string{"default", "critical"}, 8)

	c.Close()

	hbs := fetchIngestHeartbeats(t, base)
	if len(hbs) == 0 {
		t.Fatal("expected at least one heartbeat in mock-ingest, got none")
	}
}

// TestIntegration_Snapshot verifies that a queue snapshot reaches mock-ingest.
func TestIntegration_Snapshot(t *testing.T) {
	base := ingestURL(t)
	resetIngest(t, base)

	c := New("ts_test_abc", WithEndpoint(base), WithFramework("asynq"))
	c.Start()

	c.Snapshot("default", 42, 3, 1)

	c.Close()

	// Snapshots go to /v1/snapshot; assert via /test/requests
	resp, err := http.Get(base + "/test/requests")
	if err != nil {
		t.Fatalf("fetch requests: %v", err)
	}
	defer resp.Body.Close()
	body, _ := io.ReadAll(resp.Body)
	if !strings.Contains(string(body), "/v1/snapshot") {
		t.Errorf("expected /v1/snapshot request in log, got: %s", body)
	}
}

// TestIntegration_MultipleEvents verifies that a batch of events all land in mock-ingest.
func TestIntegration_MultipleEvents(t *testing.T) {
	base := ingestURL(t)
	resetIngest(t, base)

	c := New("ts_test_abc", WithEndpoint(base), WithFramework("river"))
	c.Start()

	for i := 0; i < 5; i++ {
		c.SendEvent(TaskEvent{
			Framework:  "river",
			Language:   "go",
			SDKVersion: sdkVersion,
			Type:       "task_event",
			Status:     "succeeded",
			Worker:     WorkerInfo{Key: "worker-batch", Hostname: "host", PID: 1, Queues: []string{"default"}, Concurrency: 1},
			Task:       TaskInfo{Name: "BatchJob", ID: fmt.Sprintf("job-batch-%d", i), Queue: "default", Attempt: 1},
			Metrics:    MetricsInfo{DurationMS: float64(i+1) * 10},
		})
	}

	c.Close()

	// Allow a brief moment for the last flush to complete
	time.Sleep(200 * time.Millisecond)

	events := fetchIngestEvents(t, base)
	if len(events) < 5 {
		t.Errorf("expected at least 5 events, got %d: %v", len(events), events)
	}
}

// TestIntegration_FailedEvent verifies error payloads are forwarded correctly.
func TestIntegration_FailedEvent(t *testing.T) {
	base := ingestURL(t)
	resetIngest(t, base)

	c := New("ts_test_abc", WithEndpoint(base), WithFramework("asynq"))
	c.Start()

	c.SendEvent(TaskEvent{
		Framework:  "asynq",
		Language:   "go",
		SDKVersion: sdkVersion,
		Type:       "task_event",
		Status:     "failed",
		Worker:     WorkerInfo{Key: "worker-err", Hostname: "host", PID: 1, Queues: []string{"default"}, Concurrency: 2},
		Task:       TaskInfo{Name: "FlakyJob", ID: "job-fail-001", Queue: "default", Attempt: 3},
		Metrics:    MetricsInfo{DurationMS: 500.0},
		Error:      &ErrorInfo{Type: "TimeoutError", Message: "context deadline exceeded"},
	})

	c.Close()

	events := fetchIngestEvents(t, base)
	found := false
	for _, ev := range events {
		if ev["status"] == "failed" {
			found = true
			break
		}
	}
	if !found {
		t.Errorf("expected a failed event, got: %v", events)
	}
}
