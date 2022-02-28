package pool

import (
	"context"
	"fmt"
	"time"

	"github.com/betas-in/logger"
)

type workerGroup struct {
	log       *logger.Logger
	name      string
	heartbeat time.Duration
}

// NewWorkerGroup ...
func NewWorkerGroup(name string, heartbeat time.Duration, log *logger.Logger) WorkerGroup {
	w := workerGroup{
		name:      name,
		heartbeat: heartbeat,
		log:       log,
	}
	return &w
}

func (w *workerGroup) Process(ctx context.Context, workerCtx *WorkerContext, id string) {
	workerCtx.Heartbeat <- Heartbeat{ID: id, Ping: true}
	workerId := fmt.Sprintf("%s-%s", w.name, id)

	w.log.Info(workerId).Msgf("Started worker")

	ticker := time.NewTicker(w.heartbeat)
	defer ticker.Stop()

	for {
		select {
		case j := <-workerCtx.Jobs:
			w.log.Info(workerId).Msgf("Job %+v", j)
			workerCtx.Heartbeat <- Heartbeat{ID: id, Processed: 1}
			workerCtx.Processed <- j
		case <-ctx.Done():
			w.log.Info(workerId).Msgf("Done")
			workerCtx.Heartbeat <- Heartbeat{ID: id, Closed: true}
			return
		case <-workerCtx.Close:
			w.log.Info(workerId).Msgf("Closing")
			workerCtx.Heartbeat <- Heartbeat{ID: id, Closed: true}
			return
		case <-ticker.C:
			w.log.Info(workerId).Msgf("Heartbeat")
			workerCtx.Heartbeat <- Heartbeat{ID: id, Ping: true}
		}
	}
}
