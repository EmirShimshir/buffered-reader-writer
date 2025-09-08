package main

import (
	"errors"
	"fmt"
	"sync"
)

var (
	ErrNextFailed    = errors.New("next failed")
	ErrProcessFailed = errors.New("process failed")
	ErrCommitFailed  = errors.New("commit failed")
)

type Producer interface {
	// Next возвращает:
	// - пакет элементов для обработки
	// - cookie для подтверждения после обработки
	// - ошибку
	Next() (items []any, cookie int, err error)

	// Commit подтверждает обработку пакета данных
	Commit(cookie int) error
}

type Consumer interface {
	// Process обрабатывает переданные элементы
	Process(items []any) error
}

type batch struct {
	buf     []any
	cookies []int
}

func Pipe(p Producer, c Consumer, maxItems int) error {
	batchCh := make(chan batch, 1)
	cookiesCh := make(chan int, 256)
	errCh := make(chan error, 3) // по количеству стадий
	var wg sync.WaitGroup

	// сигнальные каналы для каскадного shutdown
	cancelNextCh := make(chan struct{})
	cancelProcessCh := make(chan struct{})
	cancelCommitCh := make(chan struct{})

	wg.Add(1)
	go func() {
		defer wg.Done()
		if err := runNext(cancelNextCh, p, maxItems, batchCh); err != nil {
			errCh <- fmt.Errorf("%w: %v", ErrNextFailed, err)
		}
	}()

	wg.Add(1)
	go func() {
		defer wg.Done()
		if err := runProcess(cancelProcessCh, c, batchCh, cookiesCh); err != nil {
			errCh <- fmt.Errorf("%w: %v", ErrProcessFailed, err)
		}
	}()

	wg.Add(1)
	go func() {
		defer wg.Done()
		if err := runCommit(cancelCommitCh, p, cookiesCh); err != nil {
			errCh <- fmt.Errorf("%w: %v", ErrCommitFailed, err)
		}
	}()

	// Координатор ошибок
	doneErrCh := make(chan error, 3) // по количеству стадий
	var onceNext, onceProcess, onceCommit sync.Once

	go func() {
		defer close(doneErrCh)
		for e := range errCh {
			switch {
			case errors.Is(e, ErrNextFailed):
				onceNext.Do(func() { close(cancelNextCh) })
				// Process & Commit доработают сами
			case errors.Is(e, ErrProcessFailed):
				onceProcess.Do(func() { close(cancelProcessCh) })
				onceNext.Do(func() { close(cancelNextCh) })
				// Commit доработает сам
			case errors.Is(e, ErrCommitFailed):
				onceCommit.Do(func() { close(cancelCommitCh) })
				onceProcess.Do(func() { close(cancelProcessCh) })
				onceNext.Do(func() { close(cancelNextCh) })
			}
			doneErrCh <- e
		}
	}()

	wg.Wait()
	close(errCh) // закрыть канал ошибок, чтобы завершить координатор

	// Собираем все ошибки
	var allErrs []error
	for e := range doneErrCh {
		allErrs = append(allErrs, e)
	}

	if len(allErrs) > 0 {
		return errors.Join(allErrs...)
	}
	return nil
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
			if err != nil {
				return err
			}
			if cookie == -1 {
				if len(buf) > 0 {
					if ok := writeChanWithCancel(cancelCh, batchCh, batch{buf: buf, cookies: cookies}); !ok {
						return nil
					}
				}
				return nil
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
			return err
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
			return err
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
