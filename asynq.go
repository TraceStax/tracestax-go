package tracestax

import (
	"context"
	"fmt"
	"os"
	"reflect"
	"time"

	"github.com/hibiken/asynq"
)

// NewAsynqMiddleware returns an asynq middleware function that wraps each task
// handler and sends a task_event to TraceStax upon completion or failure.
//
// Usage:
//
//	mux := asynq.NewServeMux()
//	mux.Use(tracestax.NewAsynqMiddleware(client))
func NewAsynqMiddleware(client *Client) func(asynq.Handler) asynq.Handler {
	return func(next asynq.Handler) asynq.Handler {
		return asynq.HandlerFunc(func(ctx context.Context, task *asynq.Task) error {
			startTime := time.Now()

			workerInfo := buildWorkerInfo([]string{resolveAsynqQueue(ctx, task)})

			processErr := next.ProcessTask(ctx, task)

			durationMS := float64(time.Since(startTime).Microseconds()) / 1000.0

			ev := TaskEvent{
				Framework:  "asynq",
				Language:   "go",
				SDKVersion: sdkVersion,
				Type:       "task_event",
				Worker:     workerInfo,
				Task: TaskInfo{
					Name:    task.Type(),
					ID:      resolveAsynqTaskID(ctx),
					Queue:   resolveAsynqQueue(ctx, task),
					Attempt: resolveAsynqAttempt(ctx),
				},
				Metrics: MetricsInfo{
					DurationMS: roundMS(durationMS),
				},
			}

			if processErr != nil {
				ev.Status = "failed"
				ev.Error = &ErrorInfo{
					Type:       errorTypeName(processErr),
					Message:    processErr.Error(),
					StackTrace: captureCallerStack(),
				}
			} else {
				ev.Status = "succeeded"
			}

			client.SendEvent(ev)
			return processErr
		})
	}
}

// resolveAsynqTaskID extracts the task ID from the asynq context. Asynq stores
// it under the unexported key asynq.taskIDKey; we fall back to "unknown" when
// the value is absent.
func resolveAsynqTaskID(ctx context.Context) string {
	if id, ok := asynq.GetTaskID(ctx); ok {
		return id
	}
	return "unknown"
}

// resolveAsynqQueue extracts the queue name from the asynq context.
func resolveAsynqQueue(ctx context.Context, _ *asynq.Task) string {
	if q, ok := asynq.GetQueueName(ctx); ok {
		return q
	}
	return "default"
}

// resolveAsynqAttempt extracts the current attempt count from the asynq
// context. Asynq counts from 0 on first execution, so we add 1.
func resolveAsynqAttempt(ctx context.Context) int {
	if retried, ok := asynq.GetRetryCount(ctx); ok {
		return retried + 1
	}
	return 1
}

// buildWorkerInfo constructs a WorkerInfo from the current process.
func buildWorkerInfo(queues []string) WorkerInfo {
	hostname, err := os.Hostname()
	if err != nil {
		hostname = "unknown"
	}
	pid := os.Getpid()
	return WorkerInfo{
		Key:         fmt.Sprintf("%s:%d", hostname, pid),
		Hostname:    hostname,
		PID:         pid,
		Concurrency: 1,
		Queues:      queues,
	}
}

// errorTypeName returns the concrete type name of an error value (e.g.
// "*url.Error") without importing reflect-heavy packages.
func errorTypeName(err error) string {
	if err == nil {
		return ""
	}
	t := reflect.TypeOf(err)
	if t.Kind() == reflect.Ptr {
		return "*" + t.Elem().Name()
	}
	return t.Name()
}

// roundMS rounds a duration in milliseconds to two decimal places.
func roundMS(ms float64) float64 {
	return float64(int64(ms*100+0.5)) / 100
}

// Compile-time assertion: asynq.HandlerFunc satisfies asynq.Handler.
var _ asynq.Handler = asynq.HandlerFunc(nil)
