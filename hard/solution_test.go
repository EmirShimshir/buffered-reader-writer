package main

import (
	"errors"
	"testing"

	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
)

// --- Моковые объекты ---

type MockProducer struct {
	mock.Mock
}

func (m *MockProducer) Next() ([]any, int, error) {
	args := m.Called()
	return args.Get(0).([]any), args.Int(1), args.Error(2)
}

func (m *MockProducer) Commit(cookie int) error {
	args := m.Called(cookie)
	return args.Error(0)
}

type MockConsumer struct {
	mock.Mock
}

func (m *MockConsumer) Process(items []any) error {
	args := m.Called(items)
	return args.Error(0)
}

// --- Тесты ---

func TestPipe_HappyPathSingleBatch(t *testing.T) {
	producer := &MockProducer{}
	consumer := &MockConsumer{}
	maxItems := 10

	// Producer возвращает данные и cookie = 1
	data := []any{"item1", "item2", "item3"}
	producer.On("Next").Return(data, 1, nil).Once()

	// Consumer должен получить данные
	consumer.On("Process", data).Return(nil).Once()

	// Producer должен закоммитить cookie
	producer.On("Commit", 1).Return(nil).Once()

	// Producer возвращает конец данных
	producer.On("Next").Return([]any{}, -1, nil).Once()

	err := Pipe(producer, consumer, maxItems)
	require.NoError(t, err)

	producer.AssertExpectations(t)
	consumer.AssertExpectations(t)
}

func TestPipe_HappyPathMultipleBatches(t *testing.T) {
	producer := &MockProducer{}
	consumer := &MockConsumer{}
	maxItems := 5

	// Первый батч - 3 элемента
	batch1 := []any{"item1", "item2", "item3"}
	producer.On("Next").Return(batch1, 1, nil).Once()

	// Второй батч - 2 элемента (все еще помещается в буфер)
	batch2 := []any{"item4", "item5"}
	producer.On("Next").Return(batch2, 2, nil).Once()

	// Третий батч - 3 элемента (переполнение буфера)
	batch3 := []any{"item6", "item7", "item8"}
	producer.On("Next").Return(batch3, 3, nil).Once()

	// Consumer получает объединенные batch1 + batch2
	expectedFirstProcess := []any{"item1", "item2", "item3", "item4", "item5"}
	consumer.On("Process", expectedFirstProcess).Return(nil).Once()

	// Producer коммитит cookie 1 и 2
	producer.On("Commit", 1).Return(nil).Once()
	producer.On("Commit", 2).Return(nil).Once()

	// Producer возвращает конец данных
	producer.On("Next").Return([]any{}, -1, nil).Once()

	// Consumer получает оставшиеся данные
	consumer.On("Process", batch3).Return(nil).Once()

	// Producer коммитит cookie 3
	producer.On("Commit", 3).Return(nil).Once()

	err := Pipe(producer, consumer, maxItems)
	require.NoError(t, err)

	producer.AssertExpectations(t)
	consumer.AssertExpectations(t)
}

func TestPipe_ExactBufferSize(t *testing.T) {
	producer := &MockProducer{}
	consumer := &MockConsumer{}
	maxItems := 3

	// Точное заполнение буфера
	batch1 := []any{"item1", "item2", "item3"}
	producer.On("Next").Return(batch1, 1, nil).Once()

	// Consumer получает полный буфер
	consumer.On("Process", batch1).Return(nil).Once()

	// Producer коммитит cookie
	producer.On("Commit", 1).Return(nil).Once()

	// Конец данных
	producer.On("Next").Return([]any{}, -1, nil).Once()

	err := Pipe(producer, consumer, maxItems)
	require.NoError(t, err)

	producer.AssertExpectations(t)
	consumer.AssertExpectations(t)
}

func TestPipe_EmptyData(t *testing.T) {
	producer := &MockProducer{}
	consumer := &MockConsumer{}
	maxItems := 10

	// Сразу конец данных
	producer.On("Next").Return([]any{}, -1, nil).Once()

	err := Pipe(producer, consumer, maxItems)
	require.NoError(t, err)

	// Consumer не должен быть вызван
	consumer.AssertNotCalled(t, "Process", mock.Anything)
	producer.AssertNotCalled(t, "Commit", mock.Anything)

	producer.AssertExpectations(t)
	consumer.AssertExpectations(t)
}

func TestPipe_ProducerError(t *testing.T) {
	producer := &MockProducer{}
	consumer := &MockConsumer{}
	maxItems := 10

	expectedErr := errors.New("producer error")
	producer.On("Next").Return([]any{}, 0, expectedErr).Once()

	err := Pipe(producer, consumer, maxItems)
	require.Error(t, err)
	require.Contains(t, err.Error(), expectedErr.Error())

	producer.AssertExpectations(t)
	consumer.AssertNotCalled(t, "Process", mock.Anything)
}

func TestPipe_ConsumerError(t *testing.T) {
	producer := &MockProducer{}
	consumer := &MockConsumer{}
	maxItems := 2

	data1 := []any{"item1", "item2"}
	producer.On("Next").Return(data1, 1, nil).Once()

	data2 := []any{"item3"}
	producer.On("Next").Return(data2, 2, nil).Once()

	producer.On("Next").Return([]any{}, -1, nil).Maybe()

	processErr := errors.New("consumer error")
	consumer.On("Process", data1).Return(processErr).Once()

	err := Pipe(producer, consumer, maxItems)
	require.Error(t, err)
	require.Contains(t, err.Error(), processErr.Error())

	producer.AssertExpectations(t)
	consumer.AssertExpectations(t)
	producer.AssertNotCalled(t, "Commit", mock.Anything)
}

func TestPipe_ConsumerEndError(t *testing.T) {
	producer := &MockProducer{}
	consumer := &MockConsumer{}
	maxItems := 2

	data1 := []any{"item1", "item2"}
	producer.On("Next").Return(data1, 1, nil).Once()

	data2 := []any{}
	producer.On("Next").Return(data2, -1, nil).Once()

	processErr := errors.New("consumer error")
	consumer.On("Process", data1).Return(processErr).Once()

	err := Pipe(producer, consumer, maxItems)
	require.Error(t, err)
	require.Contains(t, err.Error(), processErr.Error())

	producer.AssertExpectations(t)
	consumer.AssertExpectations(t)
	producer.AssertNotCalled(t, "Commit", mock.Anything)
}

func TestPipe_CommitError(t *testing.T) {
	producer := &MockProducer{}
	consumer := &MockConsumer{}
	maxItems := 2

	data1 := []any{"item1", "item2"}
	producer.On("Next").Return(data1, 1, nil).Once()

	data2 := []any{"item3"}
	producer.On("Next").Return(data2, 2, nil).Once()

	producer.On("Next").Return([]any{}, -1, nil).Maybe()

	consumer.On("Process", data1).Return(nil).Once()

	consumer.On("Process", mock.Anything).Return(nil).Maybe()

	commitErr := errors.New("commit error")
	producer.On("Commit", 1).Return(commitErr).Once()

	err := Pipe(producer, consumer, maxItems)
	require.Error(t, err)
	require.Contains(t, err.Error(), commitErr.Error())

	producer.AssertExpectations(t)
	consumer.AssertExpectations(t)
}

func TestPipe_MultipleCommitsWithError(t *testing.T) {
	producer := &MockProducer{}
	consumer := &MockConsumer{}
	maxItems := 5

	// Два батча, которые будут объединены
	batch1 := []any{"item1", "item2"}
	batch2 := []any{"item3", "item4"}

	producer.On("Next").Return(batch1, 1, nil).Once()
	producer.On("Next").Return(batch2, 2, nil).Once()
	producer.On("Next").Return([]any{}, -1, nil).Once()

	expectedData := []any{"item1", "item2", "item3", "item4"}
	consumer.On("Process", expectedData).Return(nil).Once()

	// Первый коммит успешен, второй падает
	producer.On("Commit", 1).Return(nil).Once()
	commitErr := errors.New("second commit failed")
	producer.On("Commit", 2).Return(commitErr).Once()

	err := Pipe(producer, consumer, maxItems)
	require.Error(t, err)
	require.Contains(t, err.Error(), commitErr.Error())

	producer.AssertExpectations(t)
	consumer.AssertExpectations(t)
}

func TestPipe_SingleItemMultipleBatches(t *testing.T) {
	producer := &MockProducer{}
	consumer := &MockConsumer{}
	maxItems := 2

	// Несколько батчей по одному элементу
	batches := [][]any{
		{"item1"},
		{"item2"},
		{"item3"},
	}

	cookies := []int{1, 2, 3}

	for i := 0; i < len(batches); i++ {
		producer.On("Next").Return(batches[i], cookies[i], nil).Once()
	}
	producer.On("Next").Return([]any{}, -1, nil).Once()

	// Ожидаемые вызовы Process
	consumer.On("Process", []any{"item1", "item2"}).Return(nil).Once()
	consumer.On("Process", []any{"item3"}).Return(nil).Once()

	// Ожидаемые коммиты
	producer.On("Commit", 1).Return(nil).Once()
	producer.On("Commit", 2).Return(nil).Once()
	producer.On("Commit", 3).Return(nil).Once()

	err := Pipe(producer, consumer, maxItems)
	require.NoError(t, err)

	producer.AssertExpectations(t)
	consumer.AssertExpectations(t)
}

func TestPipe_NoDataButWithCookieMinusOne(t *testing.T) {
	producer := &MockProducer{}
	consumer := &MockConsumer{}
	maxItems := 10

	// Producer сразу возвращает cookie = -1 без данных
	producer.On("Next").Return([]any{}, -1, nil).Once()

	err := Pipe(producer, consumer, maxItems)
	require.NoError(t, err)

	// Никаких вызовов Process или Commit не должно быть
	consumer.AssertNotCalled(t, "Process", mock.Anything)
	producer.AssertNotCalled(t, "Commit", mock.Anything)

	producer.AssertExpectations(t)
	consumer.AssertExpectations(t)
}
