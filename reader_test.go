package kafka_go

import (
	"context"
	"testing"
	"time"
)

func TestCommitAfterContextCancel(t *testing.T) {
	r := NewReader()
	r.cancel()
	time.Sleep(10 * time.Millisecond)

	err := r.commit()
	if err == nil {
		t.Fatal("expected commit to return error after context cancellation")
	}
	if err != context.Canceled {
		t.Fatalf("expected context.Canceled, got %v", err)
	}
}

func TestCommitOnNormalRead(t *testing.T) {
	r := NewReader()
	msgs := []Message{{Offset: 1, Value: "test"}}

	go r.Consume(msgs)

	select {
	case msg := <-r.Messages:
		if msg.Offset != 1 {
			t.Fatalf("expected offset 1, got %d", msg.Offset)
		}
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for message")
	}

	time.Sleep(50 * time.Millisecond)

	r.mu.Lock()
	committed := r.committed
	r.mu.Unlock()

	if committed != 1 {
		t.Fatalf("expected committed offset 1, got %d", committed)
	}
}
