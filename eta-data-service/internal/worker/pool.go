package worker

import (
	"context"
	"log/slog"
	"sync"
)

// TaskProcessor описывает обработчик одной задачи из очереди.
type TaskProcessor interface {
	Run(ctx context.Context) error
}

// Pool запускает несколько воркеров, каждый из которых крутит свой Run(ctx).
type Pool struct {
	logger *slog.Logger
	count  int
}

// NewPool создаёт пул.
func NewPool(logger *slog.Logger, count int) *Pool {
	if count <= 0 {
		count = 1
	}
	return &Pool{
		logger: logger,
		count:  count,
	}
}

// Start запускает count горутин, каждая вызывает processor.Run(ctx) до завершения контекста.
func (p *Pool) Start(ctx context.Context, name string, processor TaskProcessor) {
	var wg sync.WaitGroup
	wg.Add(p.count)

	for i := 0; i < p.count; i++ {
		go func(id int) {
			defer wg.Done()
			if err := processor.Run(ctx); err != nil {
				p.logger.Error("worker stopped with error", "worker", name, "id", id, "error", err)
			}
		}(i)
	}

	go func() {
		<-ctx.Done()
		wg.Wait()
	}()
}

