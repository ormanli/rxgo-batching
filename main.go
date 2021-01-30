package main

import (
	"context"
	"log"
	"net/http"
	"sync"
	"sync/atomic"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/reactivex/rxgo/v2"
)

func main() {
	r := gin.Default()
	sink := newRecordSink()

	r.POST("/records", func(c *gin.Context) {
		var r record
		if err := c.ShouldBindJSON(&r); err != nil {
			c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
			return
		}

		response, err := sink.add(r)
		if err != nil {
			c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
			return
		}

		c.JSON(http.StatusCreated, response)
	})

	r.GET("/records", func(c *gin.Context) {
		c.JSON(http.StatusOK, sink.getAll())
	})

	err := r.Run()
	if err != nil {
		log.Fatal(err)
	}
}

type record struct {
	ID   uint64 `json:"ID"`
	Name string `json:"Name"`
}

type recordSink struct {
	recordMap map[uint64]record
	input     chan rxgo.Item
	pipeline  rxgo.Observable
	counter   uint64
	mu        sync.RWMutex
}

func (s *recordSink) add(r record) (record, error) {
	id := atomic.AddUint64(&s.counter, 1)

	go func(id uint64, r record) {
		s.input <- rxgo.Of(record{
			ID:   id,
			Name: r.Name,
		})
	}(id, r)

	item, err := s.pipeline.
		Find(func(i interface{}) bool {
			r := i.(record)
			return r.ID == id
		}).Get()

	if err != nil {
		return record{}, err
	}

	return item.V.(record), nil
}

func (s *recordSink) getAll() []record {
	s.mu.RLock()
	defer s.mu.RUnlock()

	records := make([]record, 0, len(s.recordMap))
	for _, v := range s.recordMap {
		records = append(records, v)
	}

	return records
}

func newRecordSink() *recordSink {
	s := &recordSink{
		input:     make(chan rxgo.Item),
		recordMap: make(map[uint64]record),
	}

	s.pipeline = rxgo.FromChannel(s.input).
		BufferWithTimeOrCount(rxgo.WithDuration(time.Millisecond*10), 10).
		Map(func(_ context.Context, i interface{}) (interface{}, error) {
			records := i.([]interface{})

			s.mu.Lock()
			defer s.mu.Unlock()
			for k := range records {
				r := records[k].(record)

				s.recordMap[r.ID] = r
			}

			return records, nil
		}).
		FlatMap(func(item rxgo.Item) rxgo.Observable {
			return rxgo.Just(item.V.([]interface{}))()
		})

	return s
}
