//  Copyright 2021-Present Couchbase, Inc.
//
//  Use of this software is governed by the Business Source License included
//  in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
//  in that file, in accordance with the Business Source License, use of this
//  software will be governed by the Apache License, Version 2.0, included in
//  the file licenses/APL2.txt.

package main

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"path"
	"runtime"
	"strings"
	"sync"
	"time"

	log "github.com/couchbase/clog"
)

// This is inspired from the Golang's original TimeoutHandler implementation.
// ref: https://github.com/golang/go/blob/a6ff433d6a927e8ad8eaa6828127233296d12ce5/src/net/http/server.go#L3286
// It primarily replaces the underlying buf with the http.ResponseWriter.
//
// newTimeoutHandler returns a Handler that runs h with the given time limit.
//
// The new Handler calls h.ServeHTTP to handle each request, but if a
// call runs for longer than its time limit, the handler responds with
// a 503 Service Unavailable error and the given message in its body.
// (If msg is empty, a suitable default message will be sent.)
// After such a timeout, writes by h to its ResponseWriter will return
// ErrHandlerTimeout.
func newTimeoutHandler(h http.Handler, dt time.Duration, msg string) http.Handler {
	return &timeoutHandler{
		handler: h,
		body:    msg,
		dt:      dt,
	}
}

// timeoutHandler supports the Pusher interface but does not support
// the Hijacker or Flusher interfaces.
type timeoutHandler struct {
	handler http.Handler
	body    string
	dt      time.Duration

	// When set, no context will be created and this context will
	// be used instead.
	testContext context.Context
}

func (h *timeoutHandler) errorBody() string {
	if h.body != "" {
		return h.body
	}
	return "<html><head><title>Timeout</title></head><body><h1>Timeout</h1></body></html>"
}

func (h *timeoutHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	ctx := h.testContext
	if ctx == nil {
		var cancelCtx context.CancelFunc
		ctx, cancelCtx = context.WithTimeout(r.Context(), h.dt)
		defer cancelCtx()
	}
	r = r.WithContext(ctx)
	done := make(chan struct{})
	tw := &timeoutWriter{
		w:   w,
		h:   make(http.Header),
		req: r,
	}
	panicChan := make(chan interface{}, 1)
	go func() {
		defer func() {
			if p := recover(); p != nil {
				panicChan <- p
			}
		}()
		h.handler.ServeHTTP(tw, r)
		close(done)
	}()
	select {
	case p := <-panicChan:
		panic(p)

	case <-done:
		tw.mu.Lock()
		defer tw.mu.Unlock()
		dst := w.Header()
		for k, vv := range tw.h {
			dst[k] = vv
		}
		if !tw.wroteHeader {
			tw.code = http.StatusOK
			w.WriteHeader(tw.code)
		}

	case <-ctx.Done():
		tw.mu.Lock()
		defer tw.mu.Unlock()
		w.WriteHeader(http.StatusServiceUnavailable)
		io.WriteString(w, h.errorBody())
		tw.timedOut = true
	}
}

type timeoutWriter struct {
	w   http.ResponseWriter
	h   http.Header
	req *http.Request

	mu          sync.Mutex
	timedOut    bool
	wroteHeader bool
	code        int
}

// Push implements the Pusher interface.
func (tw *timeoutWriter) Push(target string, opts *http.PushOptions) error {
	if pusher, ok := tw.w.(http.Pusher); ok {
		return pusher.Push(target, opts)
	}
	return http.ErrNotSupported
}

func (tw *timeoutWriter) Header() http.Header { return tw.h }

func (tw *timeoutWriter) Write(p []byte) (int, error) {
	tw.mu.Lock()
	defer tw.mu.Unlock()
	if tw.timedOut {
		return 0, http.ErrHandlerTimeout
	}
	if !tw.wroteHeader {
		tw.writeHeaderLocked(http.StatusOK)
	}
	return tw.w.Write(p)
}

func (tw *timeoutWriter) writeHeaderLocked(code int) {
	checkWriteHeaderCode(code)

	switch {
	case tw.timedOut:
		return
	case tw.wroteHeader:
		if tw.req != nil {
			caller := relevantCaller()
			logf(tw.req, "http: superfluous response.WriteHeader call from"+
				" %s (%s:%d)", caller.Function, path.Base(caller.File),
				caller.Line)
		}
	default:
		dst := tw.w.Header()
		for k, vv := range tw.h {
			dst[k] = vv
		}

		tw.wroteHeader = true
		tw.code = code
	}
}

func (tw *timeoutWriter) WriteHeader(code int) {
	tw.mu.Lock()
	defer tw.mu.Unlock()
	tw.writeHeaderLocked(code)
}

// relevantCaller searches the call stack for the first function outside of net/http.
// The purpose of this function is to provide more helpful error messages.
func relevantCaller() runtime.Frame {
	pc := make([]uintptr, 16)
	n := runtime.Callers(1, pc)
	frames := runtime.CallersFrames(pc[:n])
	var frame runtime.Frame
	for {
		frame, more := frames.Next()
		if !strings.HasPrefix(frame.Function, "net/http.") {
			return frame
		}
		if !more {
			break
		}
	}
	return frame
}

// logf prints to the ErrorLog of the *Server associated with request r
// via ServerContextKey. If there's no associated server, or if ErrorLog
// is nil, logging is done via the log package's standard logger.
func logf(r *http.Request, format string, args ...interface{}) {
	s, _ := r.Context().Value(http.ServerContextKey).(*http.Server)
	if s != nil && s.ErrorLog != nil {
		s.ErrorLog.Printf(format, args...)
	} else {
		log.Printf(format, args...)
	}
}

func checkWriteHeaderCode(code int) {
	// Issue 22880: require valid WriteHeader status codes.
	// For now we only enforce that it's three digits.
	// In the future we might block things over 599 (600 and above aren't defined
	// at https://httpwg.org/specs/rfc7231.html#status.codes)
	// and we might block under 200 (once we have more mature 1xx support).
	// But for now any three digits.
	//
	// We used to send "HTTP/1.1 000 0" on the wire in responses but there's
	// no equivalent bogus thing we can realistically send in HTTP/2,
	// so we'll consistently panic instead and help people find their bugs
	// early. (We can't return an error from WriteHeader even if we wanted to.)
	if code < 100 || code > 999 {
		panic(fmt.Sprintf("invalid WriteHeader code %v", code))
	}
}
