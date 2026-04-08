package tracestax

import (
	"context"
	"strconv"
	"time"

	"github.com/riverqueue/river/rivertype"
)

// TraceStaxMiddleware implements the river.WorkerMiddleware interface and sends
// a task_event to TraceStax for every job execution attempted by the River worker.
//
// Usage:
//
//	workers := river.NewWorkers()
//	river.AddWorkerSafely(workers, &MyWorker{})
//	riverClient, _ := river.NewClient(riverpgxv5.New(pool), &river.Config{
//	    Workers: workers,
//	    WorkerMiddleware: []rivertype.WorkerMiddleware{
//	        &tracestax.TraceStaxMiddleware{Client: client},
//	    },
//	})
type TraceStaxMiddleware struct {
	Client *Client
}

// Work satisfies the rivertype.WorkerMiddleware interface. It records the start
// time, delegates to the inner work function, then sends a task_event carrying
// the job metadata and outcome.
func (m *TraceStaxMiddleware) Work(ctx context.Context, job *rivertype.JobRow, doInner func(ctx context.Context) error) error {
	startTime := time.Now()

	queue := job.Queue
	if queue == "" {
		queue = "default"
	}

	workerInfo := buildWorkerInfo([]string{queue})

	innerErr := doInner(ctx)

	durationMS := float64(time.Since(startTime).Microseconds()) / 1000.0

	ev := TaskEvent{
		Framework:  "river",
		Language:   "go",
		SDKVersion: sdkVersion,
		Type:       "task_event",
		Worker:     workerInfo,
		Task: TaskInfo{
			Name:    job.Kind,
			ID:      strconv.FormatInt(job.ID, 10),
			Queue:   queue,
			Attempt: job.Attempt,
		},
		Metrics: MetricsInfo{
			DurationMS: roundMS(durationMS),
		},
	}

	if innerErr != nil {
		ev.Status = "failed"
		ev.Error = &ErrorInfo{
			Type:       errorTypeName(innerErr),
			Message:    innerErr.Error(),
			StackTrace: captureCallerStack(),
		}
	} else {
		ev.Status = "succeeded"
	}

	m.Client.SendEvent(ev)
	return innerErr
}
