package limiter

import (
	"container/list"
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/platinummonkey/go-concurrency-limits/core"
)

type fifoElement struct {
	ctx         context.Context
	releaseChan chan core.Listener
}

func (e *fifoElement) setListener(listener core.Listener) {
	select {
	case e.releaseChan <- listener:
		// noop
	default:
		// timeout has expired
	}
}

type fifoQueue struct {
	q  *list.List
	mu sync.RWMutex
}

func (q *fifoQueue) len() int {
	q.mu.RLock()
	defer q.mu.RUnlock()
	return q.q.Len()
}

func (q *fifoQueue) push(ctx context.Context) (*list.Element, chan core.Listener) {
	q.mu.Lock()
	defer q.mu.Unlock()
	releaseChan := make(chan core.Listener, 1)
	fe := fifoElement{
		ctx:         ctx,
		releaseChan: releaseChan,
	}
	e := q.q.PushBack(fe)
	return e, releaseChan
}

func (q *fifoQueue) pop() *fifoElement {
	q.mu.Lock()
	defer q.mu.Unlock()
	e := q.q.Front()
	if e != nil {
		fe := e.Value.(fifoElement)
		q.q.Remove(e)
		return &fe
	}
	return nil
}

func (q *fifoQueue) peek() *fifoElement {
	q.mu.RLock()
	defer q.mu.RUnlock()
	e := q.q.Front()
	if e != nil {
		fe := q.q.Front().Value.(fifoElement)
		return &fe
	}
	return nil
}

func (q *fifoQueue) remove(event *list.Element) {
	if event == nil {
		return
	}
	q.mu.Lock()
	defer q.mu.Unlock()
	q.q.Remove(event)
}

// FifoBlockingListener implements a blocking listener for the FifoBlockingListener
type FifoBlockingListener struct {
	delegateListener core.Listener
	limiter          *FifoBlockingLimiter
}

func (l *FifoBlockingListener) unblock() {
	l.limiter.mu.Lock()
	defer l.limiter.mu.Unlock()
	if l.limiter.backlog.len() > 0 {
		event := l.limiter.backlog.peek()
		if event != nil {
			listener, ok := l.limiter.delegate.Acquire(event.ctx)
			if ok && listener != nil {
				nextEvent := l.limiter.backlog.pop()
				nextEvent.setListener(listener)
			}
		}
		// otherwise: still can't acquire the limit.  unblock will be called again next time the limit is released.
	}
}

// OnDropped is called to indicate the request failed and was dropped due to being rejected by an external limit or
// hitting a timeout.  Loss based Limit implementations will likely do an aggressive reducing in limit when this
// happens.
func (l *FifoBlockingListener) OnDropped() {
	l.delegateListener.OnDropped()
	l.unblock()
}

// OnIgnore is called to indicate the operation failed before any meaningful RTT measurement could be made and
// should be ignored to not introduce an artificially low RTT.
func (l *FifoBlockingListener) OnIgnore() {
	l.delegateListener.OnIgnore()
	l.unblock()
}

// OnSuccess is called as a notification that the operation succeeded and internally measured latency should be
// used as an RTT sample.
func (l *FifoBlockingListener) OnSuccess() {
	l.delegateListener.OnSuccess()
	l.unblock()
}

// FifoBlockingLimiter implements a Limiter that blocks the caller when the limit has been reached.  This strategy
// ensures the resource is properly protected but favors availability over latency by not fast failing requests when
// the limit has been reached.  To help keep success latencies low and minimize timeouts any blocked requests are
// processed in last in/first out order.
//
// Use this limiter only when the concurrency model allows the limiter to be blocked.
type FifoBlockingLimiter struct {
	delegate          core.Limiter
	maxBacklogSize    int
	maxBacklogTimeout time.Duration

	backlog fifoQueue
	c       *sync.Cond
	mu      sync.RWMutex
}

// NewFifoBlockingLimiter will create a new FifoBlockingLimiter
func NewFifoBlockingLimiter(
	delegate core.Limiter,
	maxBacklogSize int,
	maxBacklogTimeout time.Duration,
) *FifoBlockingLimiter {
	if maxBacklogSize <= 0 {
		maxBacklogSize = 100
	}
	if maxBacklogTimeout == 0 {
		maxBacklogTimeout = time.Millisecond * 1000
	}
	mu := sync.Mutex{}
	return &FifoBlockingLimiter{
		delegate:          delegate,
		maxBacklogSize:    maxBacklogSize,
		maxBacklogTimeout: maxBacklogTimeout,
		backlog: fifoQueue{
			q: list.New(),
		},
		c: sync.NewCond(&mu),
	}
}

// NewFifoBlockingLimiterWithDefaults will create a new FifoBlockingLimiter with default values.
func NewFifoBlockingLimiterWithDefaults(
	delegate core.Limiter,
) *FifoBlockingLimiter {
	return NewFifoBlockingLimiter(delegate, 100, time.Millisecond*1000)
}

func (l *FifoBlockingLimiter) tryAcquire(ctx context.Context) core.Listener {
	// Try to acquire a token and return immediately if successful
	listener, ok := l.delegate.Acquire(ctx)
	if ok && listener != nil {
		return listener
	}

	// Restrict backlog size so the queue doesn't grow unbounded during an outage
	if l.backlog.len() >= l.maxBacklogSize {
		return nil
	}

	// Create a holder for a listener and block until a listener is released by another
	// operation.  Holders will be unblocked in FIFO order
	event, eventReleaseChan := l.backlog.push(ctx)
	select {
	case listener = <-eventReleaseChan:
		return listener
	case <-time.After(l.maxBacklogTimeout):
		// Remove the holder from the backlog.  This item is likely to be at the end of the
		// list so do a remove to minimize the number of items to traverse
		l.backlog.remove(event)
		return nil
	}
}

// Acquire a token from the limiter.  Returns an Optional.empty() if the limit has been exceeded.
// If acquired the caller must call one of the Listener methods when the operation has been completed to release
// the count.
//
// context Context for the request. The context is used by advanced strategies such as LookupPartitionStrategy.
func (l *FifoBlockingLimiter) Acquire(ctx context.Context) (core.Listener, bool) {
	delegateListener := l.tryAcquire(ctx)
	if delegateListener == nil {
		return nil, false
	}
	return &FifoBlockingListener{
		delegateListener: delegateListener,
		limiter:          l,
	}, true
}

func (l *FifoBlockingLimiter) String() string {
	return fmt.Sprintf("FifoBlockingLimiter{delegate=%v, maxBacklogSize=%d, maxBacklogTimeout=%v}",
		l.delegate, l.maxBacklogSize, l.maxBacklogTimeout)
}
