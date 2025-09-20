package main

import (
	"context"
	"errors"
	"fmt"
	"golang.org/x/sync/errgroup"
)

var (
	ErrEofCommitCookie = errors.New("no more data")
	ErrNextFailed      = errors.New("next failed")
	ErrProcessFailed   = errors.New("process failed")
	ErrCommitFailed    = errors.New("commit failed")
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
	g, ctx := errgroup.WithContext(context.Background())

	batchCh := make(chan batch, 1)
	cookiesCh := make(chan int, 256)

	g.Go(func() error {
		return runNext(ctx, p, maxItems, batchCh)
	})

	g.Go(func() error {
		return runProcess(ctx, c, batchCh, cookiesCh)
	})

	g.Go(func() error {
		return runCommit(ctx, p, cookiesCh)
	})

	return g.Wait()
}

func runNext(ctx context.Context, p Producer, maxItems int, batchCh chan<- batch) error {
	defer close(batchCh)

	buf := make([]any, 0, maxItems)
	var cookies []int
	for {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		items, cookie, err := p.Next()
		if errors.Is(err, ErrEofCommitCookie) {
			if len(buf) > 0 {
				if err := writeChanWithContext(ctx, batchCh, batch{buf: buf, cookies: cookies}); err != nil {
					return err
				}
			}
			return nil
		}
		if err != nil {
			return fmt.Errorf("%w: %v", ErrNextFailed, err)
		}

		if len(buf)+len(items) > maxItems {
			if err := writeChanWithContext(ctx, batchCh, batch{buf: buf, cookies: cookies}); err != nil {
				return err
			}
			buf = make([]any, 0, maxItems)
			cookies = []int{}
		}
		buf = append(buf, items...)
		cookies = append(cookies, cookie)
	}

}

func runProcess(ctx context.Context, c Consumer, batchCh <-chan batch, cookiesCh chan<- int) error {
	defer close(cookiesCh)

	for {
		batch, ok, err := readChanWithContext(ctx, batchCh)
		if err != nil {
			return err
		}
		if !ok {
			return nil
		}
		if err := c.Process(batch.buf); err != nil {
			return fmt.Errorf("%w: %v", ErrProcessFailed, err)
		}
		for _, cookie := range batch.cookies {
			if err := writeChanWithContext(ctx, cookiesCh, cookie); err != nil {
				return err
			}
		}
	}

}

func runCommit(ctx context.Context, p Producer, cookiesCh <-chan int) error {
	for {
		cookie, ok, err := readChanWithContext(ctx, cookiesCh)
		if err != nil {
			return err
		}
		if !ok {
			return nil
		}
		if err := p.Commit(cookie); err != nil {
			return fmt.Errorf("%w: %v", ErrCommitFailed, err)
		}
	}

}

func readChanWithContext[T any](ctx context.Context, ch <-chan T) (T, bool, error) {
	var zero T
	select {
	case <-ctx.Done():
		return zero, false, ctx.Err()
	case value, ok := <-ch:
		if !ok {
			return zero, false, nil
		}
		return value, true, nil
	}
}

func writeChanWithContext[T any](ctx context.Context, ch chan<- T, value T) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	case ch <- value:
		return nil
	}
}
