package main

import "fmt"

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
		if err != nil {
			return fmt.Errorf("next error: %w", err)
		}

		// Если cookie == -1, это конец данных
		if cookie == -1 {
			// Обрабатываем оставшиеся данные в буфере
			if len(buf) > 0 {
				if err := c.Process(buf); err != nil {
					return fmt.Errorf("process error: %w", err)
				}
				for _, ck := range cookies {
					if err := p.Commit(ck); err != nil {
						return fmt.Errorf("commit error: %w", err)
					}
				}
			}
			return nil
		}

		// Проверяем, помещаются ли новые данные в буфер
		if len(buf)+len(items) > maxItems {
			// Буфер переполнен, обрабатываем текущие данные
			if err := c.Process(buf); err != nil {
				return fmt.Errorf("process error: %w", err)
			}
			for _, cookie := range cookies {
				if err := p.Commit(cookie); err != nil {
					return fmt.Errorf("commit error: %w", err)
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
