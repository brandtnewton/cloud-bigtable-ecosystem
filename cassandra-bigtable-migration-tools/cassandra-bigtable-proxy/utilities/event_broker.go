package utilities

import "sync"

type Subscriber[T any] interface {
	OnEvent(event T)
}

type EventPublisher[T any] struct {
	subscribers []Subscriber[T]
	mu          sync.RWMutex // Mutex to protect the subscribers slice
}

func NewPublisher[T any]() *EventPublisher[T] {
	return &EventPublisher[T]{
		subscribers: make([]Subscriber[T], 0),
	}
}

func (p *EventPublisher[T]) Register(s Subscriber[T]) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.subscribers = append(p.subscribers, s)
}

func (p *EventPublisher[T]) Deregister(s Subscriber[T]) {
	p.mu.Lock()
	defer p.mu.Unlock()
	// Find and remove the subscriber (simplified removal)
	for i, sub := range p.subscribers {
		// You would typically need a unique identifier for a robust comparison
		// For simplicity, we compare pointers here (which works for struct pointers)
		if sub == s {
			p.subscribers = append(p.subscribers[:i], p.subscribers[i+1:]...)
			return
		}
	}
}

func (p *EventPublisher[T]) SendEvent(data T) {
	p.mu.RLock()
	defer p.mu.RUnlock()

	for _, s := range p.subscribers {
		s.OnEvent(data)
	}
}
