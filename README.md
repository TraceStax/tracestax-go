# tracestax-go

Go SDK for [TraceStax](https://tracestax.com) - automatic job-queue monitoring for
**Asynq** and **River**.

## Installation

```sh
go get github.com/tracestax/tracestax-go
```

## Quick start

```go
import "github.com/tracestax/tracestax-go"

client := tracestax.New("ts_live_YOUR_API_KEY")
client.Start()
defer client.Close()
```

`Start` launches a background goroutine that drains an internal buffered
channel and POSTs events to the TraceStax ingest API. `Close` flushes the
channel and blocks for up to 5 s before returning.

### Options

| Option | Default | Description |
|--------|---------|-------------|
| `tracestax.WithEndpoint(url)` | `https://ingest.tracestax.com` | Override the API base URL |
| `tracestax.WithTimeout(d)` | `10s` | Per-request HTTP timeout |

---

## Asynq

```go
package main

import (
    "github.com/hibiken/asynq"
    "github.com/tracestax/tracestax-go"
)

func main() {
    rw := tracestax.New("ts_live_YOUR_API_KEY")
    rw.Start()
    defer rw.Close()

    srv := asynq.NewServer(
        asynq.RedisClientOpt{Addr: "localhost:6379"},
        asynq.Config{Concurrency: 10},
    )

    mux := asynq.NewServeMux()

    // Install TraceStax middleware - wraps every handler registered on this mux.
    mux.Use(tracestax.NewAsynqMiddleware(rw))

    // Register your task handlers as normal.
    mux.HandleFunc("email:welcome", handleWelcomeEmail)

    if err := srv.Run(mux); err != nil {
        panic(err)
    }
}

func handleWelcomeEmail(ctx context.Context, t *asynq.Task) error {
    // Your task logic here.
    return nil
}
```

The middleware captures:

| Field | Source |
|-------|--------|
| `task.name` | `task.Type()` |
| `task.id` | `asynq.GetTaskID(ctx)` |
| `task.queue` | `asynq.GetQueueName(ctx)` |
| `task.attempt` | `asynq.GetRetryCount(ctx) + 1` |
| `status` | `"succeeded"` / `"failed"` |
| `metrics.duration_ms` | wall-clock time around `ProcessTask` |
| `error.type` / `error.message` | populated on failure |

---

## River

```go
package main

import (
    "context"

    "github.com/jackc/pgx/v5/pgxpool"
    "github.com/riverqueue/river"
    "github.com/riverqueue/river/riverdriver/riverpgxv5"
    "github.com/tracestax/tracestax-go"
)

type SortArgs struct {
    Strings []string `json:"strings"`
}

func (SortArgs) Kind() string { return "sort" }

type SortWorker struct {
    river.WorkerDefaults[SortArgs]
}

func (w *SortWorker) Work(ctx context.Context, job *river.Job[SortArgs]) error {
    // Your task logic here.
    return nil
}

func main() {
    rw := tracestax.New("ts_live_YOUR_API_KEY")
    rw.Start()
    defer rw.Close()

    ctx := context.Background()
    pool, _ := pgxpool.New(ctx, "postgres://localhost/mydb")

    workers := river.NewWorkers()
    river.AddWorkerSafely(workers, &SortWorker{})

    riverClient, _ := river.NewClient(riverpgxv5.New(pool), &river.Config{
        Workers: workers,
        // Install TraceStax as a WorkerMiddleware.
        WorkerMiddleware: []rivertype.WorkerMiddleware{
            &tracestax.TraceStaxMiddleware{Client: rw},
        },
        Queues: map[string]river.QueueConfig{
            river.QueueDefault: {MaxWorkers: 10},
        },
    })

    if err := riverClient.Start(ctx); err != nil {
        panic(err)
    }
    // ...
}
```

The middleware captures:

| Field | Source |
|-------|--------|
| `task.name` | `job.Kind` |
| `task.id` | `strconv.FormatInt(job.ID, 10)` |
| `task.queue` | `job.Queue` |
| `task.attempt` | `job.Attempt` |
| `status` | `"succeeded"` / `"failed"` |
| `metrics.duration_ms` | wall-clock time around `doInner` |
| `error.type` / `error.message` | populated on failure |

---

## Payload reference

Every task event sent to `/v1/ingest` has the following shape:

```json
{
  "framework": "asynq",
  "language": "go",
  "sdk_version": "0.1.0",
  "type": "task_event",
  "worker": {
    "key": "hostname:12345",
    "hostname": "my-host",
    "pid": 12345,
    "concurrency": 1,
    "queues": ["default"]
  },
  "task": {
    "name": "email:welcome",
    "id": "abc123",
    "queue": "default",
    "attempt": 1
  },
  "status": "succeeded",
  "metrics": {
    "duration_ms": 42.7
  }
}
```

On failure an `error` object is added:

```json
"error": {
  "type": "*mypackage.MyError",
  "message": "something went wrong"
}
```

---

## License

Apache 2.0 - see [LICENSE](../../LICENSE).
