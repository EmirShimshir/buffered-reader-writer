package main

import (
	"errors"
	"fmt"
	"sync"
)

var (
	ErrEofCommitCookie = errors.New("no more data")
	ErrNextFailed      = errors.New("next failed")
	ErrProcessFailed   = errors.New("process failed")
	ErrCommitFailed    = errors.New("commit failed")
)

type Producer interface {
	Next() (items []any, cookie int, err error)
	Commit(cookie int) error
}

type Consumer interface {
	Process(items []any) error
}

type batch struct {
	buf     []any
	cookies []int
}

// StageError — ошибка стадии с индексом и самой ошибкой
type StageError struct {
	Index int
	Err   error
}

// StageFunc — функция стадии, возвращает ошибку
type StageFunc func(cancelCh <-chan struct{}) error

// Pipeline структура
type Pipeline struct {
	stages      []StageFunc
	cancelChans []chan struct{}
}

// NewPipeline создаёт пустой pipeline
func NewPipeline() *Pipeline {
	return &Pipeline{
		stages:      []StageFunc{},
		cancelChans: []chan struct{}{},
	}
}

// AddStage добавляет стадию
func (pl *Pipeline) AddStage(stage StageFunc) {
	pl.stages = append(pl.stages, stage)
	pl.cancelChans = append(pl.cancelChans, make(chan struct{}))
}

// Run запускает pipeline и ждёт завершения
func (pl *Pipeline) Run() error {
	if len(pl.stages) == 0 {
		return nil
	}

	var wg sync.WaitGroup
	errCh := make(chan StageError, len(pl.stages))
	doneErrCh := make(chan StageError, len(pl.stages))
	onceList := make([]sync.Once, len(pl.stages))

	// Запуск стадий
	for i, stage := range pl.stages {
		wg.Add(1)
		cancelCh := pl.cancelChans[i]
		index := i
		go func(st StageFunc, ch chan struct{}, idx int) {
			defer wg.Done()
			if err := st(ch); err != nil {
				errCh <- StageError{Index: idx, Err: err}
			}
		}(stage, cancelCh, index)
	}

	// Координатор ошибок с каскадным shutdown
	go func() {
		defer close(doneErrCh)
		for se := range errCh {
			// каскадное закрытие всех предыдущих стадий
			for i := se.Index; i >= 0; i-- {
				onceList[i].Do(func() { close(pl.cancelChans[i]) })
			}
			doneErrCh <- se
		}
	}()

	wg.Wait()
	close(errCh) // закрыть канал ошибок, чтобы координатор завершил работу

	// Собираем все ошибки
	var allErrs []error
	for se := range doneErrCh {
		allErrs = append(allErrs, se.Err)
	}

	if len(allErrs) > 0 {
		return errors.Join(allErrs...)
	}
	return nil
}

func Pipe(p Producer, c Consumer, maxItems int) error {
	pipeline := NewPipeline()

	batchCh := make(chan batch, 1)
	cookiesCh := make(chan int, 256)

	pipeline.AddStage(func(cancelCh <-chan struct{}) error {
		return runNext(cancelCh, p, maxItems, batchCh)
	})

	pipeline.AddStage(func(cancelCh <-chan struct{}) error {
		return runProcess(cancelCh, c, batchCh, cookiesCh)
	})

	pipeline.AddStage(func(cancelCh <-chan struct{}) error {
		return runCommit(cancelCh, p, cookiesCh)
	})

	return pipeline.Run()
}

func runNext(cancelCh <-chan struct{}, p Producer, maxItems int, batchCh chan<- batch) error {
	defer close(batchCh)

	buf := make([]any, 0, maxItems)
	var cookies []int
	for {
		select {
		case <-cancelCh:
			return nil
		default:
			items, cookie, err := p.Next()
			if errors.Is(err, ErrEofCommitCookie) {
				if len(buf) > 0 {
					if ok := writeChanWithCancel(cancelCh, batchCh, batch{buf: buf, cookies: cookies}); !ok {
						return nil
					}
				}
				return nil
			}
			if err != nil {
				return fmt.Errorf("%w: %v", ErrNextFailed, err)
			}

			if len(buf)+len(items) > maxItems {
				if ok := writeChanWithCancel(cancelCh, batchCh, batch{buf: buf, cookies: cookies}); !ok {
					return nil
				}
				buf = make([]any, 0, maxItems)
				cookies = []int{}

			}
			buf = append(buf, items...)
			cookies = append(cookies, cookie)
		}
	}
}

func runProcess(cancelCh <-chan struct{}, c Consumer, batchCh <-chan batch, cookiesCh chan<- int) error {
	defer close(cookiesCh)
	for {
		batch, ok := readChanWithCancel(cancelCh, batchCh)
		if !ok {
			return nil
		}
		if err := c.Process(batch.buf); err != nil {
			return fmt.Errorf("%w: %v", ErrProcessFailed, err)
		}
		for _, cookie := range batch.cookies {
			if ok := writeChanWithCancel(cancelCh, cookiesCh, cookie); !ok {
				return nil
			}
		}
	}

}

func runCommit(cancelCh <-chan struct{}, p Producer, cookiesCh <-chan int) error {
	for {
		cookie, ok := readChanWithCancel(cancelCh, cookiesCh)
		if !ok {
			return nil
		}
		if err := p.Commit(cookie); err != nil {
			return fmt.Errorf("%w: %v", ErrCommitFailed, err)
		}
	}

}

func readChanWithCancel[T any](cancelCh <-chan struct{}, dataCh <-chan T) (T, bool) {
	var zero T
	select {
	case <-cancelCh:
		return zero, false
	case value, ok := <-dataCh:
		if !ok {
			return zero, false
		}
		return value, true
	}
}

func writeChanWithCancel[T any](cancelCh <-chan struct{}, dataCh chan<- T, value T) bool {
	select {
	case <-cancelCh:
		return false
	case dataCh <- value:
		return true
	}
}
