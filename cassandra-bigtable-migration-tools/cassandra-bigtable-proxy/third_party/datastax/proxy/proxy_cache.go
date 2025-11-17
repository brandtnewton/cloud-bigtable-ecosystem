package proxy

import "sync"

type proxyCache[T any] struct {
	d map[[16]byte]T
	l sync.RWMutex
}

func newProxyCache[T any]() *proxyCache[T] {
	return &proxyCache[T]{
		d: make(map[[16]byte]T),
		l: sync.RWMutex{},
	}
}

func (c *proxyCache[T]) get(id [16]byte) (T, bool) {
	c.l.RLock()
	defer c.l.RUnlock()
	t, ok := c.d[id]
	return t, ok
}

func (c *proxyCache[T]) set(id [16]byte, t T) {
	c.l.Lock()
	defer c.l.Unlock()
	c.d[id] = t
}
