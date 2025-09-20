package main

import (
	"errors"
	"fmt"
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

func Pipe(p Producer, c Consumer, maxItems int) error {
	buf := make([]any, 0, maxItems)
	var cookies []int

	for {
		items, cookie, err := p.Next()
		if errors.Is(err, ErrEofCommitCookie) {
			// Обрабатываем оставшиеся данные в буфере
			if len(buf) > 0 {
				if err := c.Process(buf); err != nil {
					return fmt.Errorf("%w: %v", ErrProcessFailed, err)
				}
				for _, ck := range cookies {
					if err := p.Commit(ck); err != nil {
						return fmt.Errorf("%w: %v", ErrCommitFailed, err)
					}
				}
			}
			return nil
		}
		if err != nil {
			return fmt.Errorf("%w: %v", ErrNextFailed, err)
		}

		// Проверяем, помещаются ли новые данные в буфер
		if len(buf)+len(items) > maxItems {
			// Буфер переполнен, обрабатываем текущие данные
			if err := c.Process(buf); err != nil {
				return fmt.Errorf("%w: %v", ErrProcessFailed, err)
			}
			for _, cookie := range cookies {
				if err := p.Commit(cookie); err != nil {
					return fmt.Errorf("%w: %v", ErrCommitFailed, err)
				}
			}
			// Сбрасываем буферы
			buf = make([]any, 0, maxItems)
			cookies = []int{}
		}

		// Добавляем новые данные в буфер
		buf = append(buf, items...)
		cookies = append(cookies, cookie)
	}
}
