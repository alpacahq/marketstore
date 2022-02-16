package shelf

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/alpacahq/marketstore/v4/utils/io"
)

// ShelfHandler gets executed by a shelf on its packages.
type ShelfHandler *func(tbk io.TimeBucketKey, data interface{}) error

// NewShelfHandler creates a new ShelfHandler from a supplied function.
func NewShelfHandler(f func(tbk io.TimeBucketKey, data interface{}) error) ShelfHandler {
	return ShelfHandler(&f)
}

// Shelf stores packages, which have shelf lives (^^) and are
// meant to have the shelf's handler executed after some deadline.
type Shelf struct {
	sync.Mutex
	m       map[string]*Package
	handler ShelfHandler
}

// NewShelf initializes a new shelf with the provided handler function.
func NewShelf(h ShelfHandler) *Shelf {
	return &Shelf{
		m:       map[string]*Package{},
		handler: h,
	}
}

// Store a new package to the shelf. This operation cancels, and replaces the existing
// package with the same TimeBucketKey on the shelf, so make sure not to prematurely
// store new packages before the previous have a chance to finish naturally.
func (s *Shelf) Store(tbk *io.TimeBucketKey, data interface{}, deadline *time.Time) {
	s.Lock()
	defer s.Unlock()

	if tbk == nil {
		return
	}

	key := tbk.String()

	// if a package already exists for this key,
	// let's cancel it, then remove it to replace
	// it with a new one.
	if p, ok := s.m[key]; ok {
		// If this is a replacement, make sure we stop the previous
		// package from executing so we don't send duplicates
		if deadline.Equal(*p.deadline) {
			p.Stop()
		}
		// If it is not a replacement, let's delete it from the map
		// but the async goroutine will still execute the previous
		// package in the background
		delete(s.m, key)
	}

	ctx, cancel := context.WithDeadline(context.Background(), *deadline)

	p := &Package{
		ctx:      ctx,
		Cancel:   cancel,
		Data:     data,
		stopped:  atomic.Value{},
		deadline: deadline,
	}

	p.Start(tbk, s.handler)

	s.m[key] = p
}

// Package is a data entry with a context to ensure async
// execution or cancellation if necessary.
type Package struct {
	deadline *time.Time
	stopped  atomic.Value
	ctx      context.Context
	Cancel   context.CancelFunc
	Data     interface{}
}

// Stop the package from running the handler on its data
// and call its context's cancel function to gracefully
// deallocate its resources.
func (p *Package) Stop() {
	p.stopped.Store(true)
	p.Cancel()
}

// Start causes the package to begin listening to it's context's
// done channel which is set by the deadline passed to the context.
// This is done in a separate goroutine.
func (p *Package) Start(tbk *io.TimeBucketKey, h ShelfHandler) {
	p.stopped.Store(false)

	go func() {
		// Recommended to call this regardless of the context
		// timing out naturally to free up resources ASAP
		defer p.Cancel()

		// closed when Cancel() is called, thus the routine
		// will either timeout, or be explicitly canceled, and won't be
		// accidentally leaked
		<-p.ctx.Done() // block until done
		if !p.stopped.Load().(bool) {
			if err := (*h)(*tbk, p.Data); err != nil {
				// nolint:forbidigo // CLI output needs fmt.Println
				fmt.Printf("failed to expire data package (%v)\n", err)
			}
		}
	}()
}
