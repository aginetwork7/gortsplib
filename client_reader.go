package gortsplib

import (
	"sync"

	"github.com/bluenviron/gortsplib/v5/pkg/base"
	"github.com/bluenviron/gortsplib/v5/pkg/liberrors"
)

type clientReader struct {
	c *Client

	mutex                                sync.Mutex
	allowInterleavedFrames               bool
	discardInterleavedFramesWhenDisabled bool

	terminate chan struct{}

	done chan struct{}
}

func (r *clientReader) start() {
	r.terminate = make(chan struct{})
	r.done = make(chan struct{})

	go r.run()
}

func (r *clientReader) setAllowInterleavedFrames(v bool) {
	r.mutex.Lock()
	defer r.mutex.Unlock()
	r.allowInterleavedFrames = v
}

func (r *clientReader) setDiscardInterleavedFramesWhenDisabled(v bool) {
	r.mutex.Lock()
	defer r.mutex.Unlock()
	r.discardInterleavedFramesWhenDisabled = v
}

func (r *clientReader) close() {
	close(r.terminate)
	<-r.done
}

func (r *clientReader) run() {
	defer close(r.done)

	err := r.runInner()

	select {
	case r.c.chReadError <- err:
	case <-r.terminate:
	}
}

func (r *clientReader) runInner() error {
	for {
		what, err := r.c.conn.Read()
		if err != nil {
			return err
		}

		switch what := what.(type) {
		case *base.Response:
			select {
			case r.c.chResponse <- what:
			case <-r.terminate:
			}

		case *base.Request:
			select {
			case r.c.chRequest <- what:
			case <-r.terminate:
			}

		case *base.InterleavedFrame:
			r.mutex.Lock()

			if !r.allowInterleavedFrames {
				if r.discardInterleavedFramesWhenDisabled {
					r.mutex.Unlock()
					continue
				}
				r.mutex.Unlock()
				return liberrors.ErrClientUnexpectedFrame{}
			}

			if cb, ok := r.c.tcpCallbackByChannel[what.Channel]; ok {
				cb(what.Payload)
			}
			r.mutex.Unlock()
		}
	}
}
