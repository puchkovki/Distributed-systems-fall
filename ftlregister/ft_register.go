package ftlregister

import (
	"context"
	"fmt"
	"math"
	"sync"

	"github.com/dati-mipt/distributed-systems/network"
	"github.com/dati-mipt/distributed-systems/util"
)

// FaultTolerantRegister хранит значение счетчиков на разных репликах
type FaultTolerantRegister struct {
	rid      int64
	current  util.TimestampedValue
	replicas map[int64]network.Link
}

// NewFaultTolerantRegister — конструктор регистра по rid и replicas
func NewFaultTolerantRegister(rid int64) *FaultTolerantRegister {
	return &FaultTolerantRegister{
		rid:      rid,
		replicas: map[int64]network.Link{},
	}
}

// "составные" READ  и WRITE
// READ = {read + write}
// WRITE = {read + write}

func (ftr *FaultTolerantRegister) Write(value int64) bool {
	currVal := DecideByQuorum(ftr, nil)
	fmt.Println("Read current value ", currVal.Val)

	currVal.Val = value
	currVal.Ts.Number = ftr.current.Ts.Number + 1
	currVal.Ts.Rid = ftr.rid
	ftr.current = currVal
	DecideByQuorum(ftr, value)
	fmt.Println("Write value ", value)

	return true
}

func (ftr *FaultTolerantRegister) Read() int64 {
	currVal := DecideByQuorum(ftr, nil)
	fmt.Println("Read current value ", currVal.Val)
	currVal.Ts.Rid = ftr.rid
	DecideByQuorum(ftr, currVal)
	fmt.Println("Write value ", currVal.Val)

	return ftr.current.Val
}

// DecideByQuorum рассылает BlockingMessage запрос на все реплики
// и получает консенсус от кворума реплик
func DecideByQuorum(ftr *FaultTolerantRegister, msg interface{}) util.TimestampedValue {
	messages := make(chan util.TimestampedValue, len(ftr.replicas))

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go ftr.BlockingMessageToQuorum(ctx, msg, &messages)

	majority := int(math.Ceil(float64(len(ftr.replicas) / 2)))
	var counter = 0
	var newTsV util.TimestampedValue
	for elem := range messages {
		if counter <= majority {
			if !elem.Ts.Less(newTsV.Ts) {
				newTsV = elem
			}
			counter++
		} else {
			cancel()
			break
		}

	}

	return newTsV
}

// BlockingMessageToQuorum рассылает BlockingMessage запрос на все реплики
func (ftr *FaultTolerantRegister) BlockingMessageToQuorum(ctx context.Context, msg interface{}, messages *chan util.TimestampedValue) {
	// Глобальный семафор
	var wg sync.WaitGroup
	for i, rep := range ftr.replicas {
		wg.Add(1)

		go func(i *int64, rep *network.Link) {
			// Send возвращает канал, передающий интерфейс
			message, ok := (<-((*rep).Send(ctx, msg))).(util.TimestampedValue)
			if ok {
				select {
				case <-ctx.Done():
					return
				case *messages <- message:
				}
			} else {
				fmt.Println(fmt.Errorf("Returned value from the %d-th replica is not TimestampValue", i).Error())
			}

			wg.Done()
		}(&i, &rep)
	}

	wg.Wait()
	close(*messages)
}

// Introduce устанавливает links между репликами
func (ftr *FaultTolerantRegister) Introduce(rid int64, link network.Link) {
	if link != nil {
		ftr.replicas[rid] = link
	}
}

// Receive принимает сообщения
func (ftr *FaultTolerantRegister) Receive(rid int64, msg interface{}) interface{} {
	if update, ok := msg.(util.TimestampedValue); ok {
		if ftr.current.Ts.Less(update.Ts) {
			ftr.current = update
		}
	}
	return ftr.current
}
