package kafka_go

import (
	"context"
	"errors"
	"sync"
)

type Message struct {
	Offset int64
	Value  string
}

type Reader struct {
	Messages  <-chan Message
	messages  chan Message
	mu        sync.Mutex
	ctx       context.Context
	cancel    context.CancelFunc
	offset    int64
	committed int64
	closed    bool
}

func NewReader() *Reader {
	ctx, cancel := context.WithCancel(context.Background())
	ch := make(chan Message)
	return &Reader{
		Messages:  ch,
		messages:  ch,
		ctx:       ctx,
		cancel:    cancel,
		committed: -1,
	}
}

func (r *Reader) Close() error {
	r.mu.Lock()
	defer r.mu.Unlock()
	if !r.closed {
		r.closed = true
		r.cancel()
		close(r.messages)
	}
	return nil
}

func (r *Reader) Consume(msgs []Message) {
	for i := range msgs {
		if r.ctx.Err() != nil {
			return
		}

		msg := msgs[i]

		r.mu.Lock()
		r.offset = msg.Offset
		r.mu.Unlock()

		select {
		case <-r.ctx.Done():
			return
		case r.messages <- msg:
		}

		if err := r.commit(); err != nil {
			return
		}
	}
}

func (r *Reader) commit() error {
	if r.ctx.Err() != nil {
		return r.ctx.Err()
	}

	r.mu.Lock()
	defer r.mu.Unlock()

	r.committed = r.offset
	return nil
}

var ErrContextCancelled = errors.New("context cancelled")
