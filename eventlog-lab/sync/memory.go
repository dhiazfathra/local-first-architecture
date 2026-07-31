package sync

import (
	"context"
	"errors"
	"fmt"
	"io"
	"sync"

	"github.com/dhiazfathra/local-first-architecture/eventlog-lab/clock"
	"github.com/dhiazfathra/local-first-architecture/eventlog-lab/sync/syncpb"
)

// FrameFilter intercepts one frame in flight and returns the frames to actually
// deliver: none (a drop), the same one (pass-through), or several (a duplicate).
// The fault harness installs a filter to inject partitions, duplication, and
// reordering; the default filter passes everything through.
type FrameFilter func(from, to clock.NodeID, frame any) []any

// MemoryTransport wires clients and servers together in-process with no real
// networking, so a whole cluster runs deterministically inside one test.
type MemoryTransport struct {
	mu      sync.Mutex
	servers map[string]*Server
	filter  FrameFilter
}

// NewMemoryTransport returns an empty transport with a pass-through filter.
func NewMemoryTransport() *MemoryTransport {
	return &MemoryTransport{servers: map[string]*Server{}}
}

// Serve registers srv at addr.
func (m *MemoryTransport) Serve(addr string, srv *Server) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.servers[addr] = srv
}

// SetFilter installs a frame filter. Passing nil restores pass-through.
func (m *MemoryTransport) SetFilter(f FrameFilter) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.filter = f
}

// Dial starts a session goroutine for the server at addr and returns the client
// half of a pair of channels.
func (m *MemoryTransport) Dial(ctx context.Context, addr string) (Stream, error) {
	m.mu.Lock()
	srv, ok := m.servers[addr]
	filter := m.filter
	m.mu.Unlock()
	if !ok {
		return nil, fmt.Errorf("memory transport: no server at %q", addr)
	}

	p := &memPipe{
		toServer: make(chan *syncpb.ClientFrame, 1024),
		toClient: make(chan *syncpb.ServerFrame, 1024),
		done:     make(chan struct{}),
		filter:   filter,
		server:   srv.replica.ID(),
	}
	go func() {
		defer close(p.done)
		defer close(p.toClient)
		p.serverErr = srv.Session(ctx, (*memServerSide)(p))
	}()
	return (*memClientSide)(p), nil
}

// memPipe is a bidirectional in-memory frame pipe.
type memPipe struct {
	toServer  chan *syncpb.ClientFrame
	toClient  chan *syncpb.ServerFrame
	done      chan struct{}
	filter    FrameFilter
	client    clock.NodeID
	server    clock.NodeID
	serverErr error
}

// deliver applies the filter and returns the frames to enqueue.
func (p *memPipe) deliver(from, to clock.NodeID, frame any) []any {
	if p.filter == nil {
		return []any{frame}
	}
	return p.filter(from, to, frame)
}

type memClientSide memPipe

func (s *memClientSide) Send(f *syncpb.ClientFrame) error {
	p := (*memPipe)(s)
	if hello, ok := f.GetBody().(*syncpb.ClientFrame_Hello); ok {
		p.client = clock.NodeID(hello.Hello.GetNodeId())
	}
	for _, out := range p.deliver(p.client, p.server, f) {
		cf, ok := out.(*syncpb.ClientFrame)
		if !ok {
			return fmt.Errorf("memory transport: filter returned %T on the client side", out)
		}
		select {
		case p.toServer <- cf:
		case <-p.done:
			return errors.New("memory transport: session ended")
		}
	}
	return nil
}

func (s *memClientSide) Recv() (*syncpb.ServerFrame, error) {
	p := (*memPipe)(s)
	f, ok := <-p.toClient
	if !ok {
		if p.serverErr != nil {
			return nil, fmt.Errorf("memory transport: server session failed: %w", p.serverErr)
		}
		return nil, io.EOF
	}
	return f, nil
}

func (s *memClientSide) CloseSend() error {
	close((*memPipe)(s).toServer)
	<-(*memPipe)(s).done
	if err := (*memPipe)(s).serverErr; err != nil {
		return fmt.Errorf("memory transport: server session failed: %w", err)
	}
	return nil
}

type memServerSide memPipe

func (s *memServerSide) Recv() (*syncpb.ClientFrame, error) {
	f, ok := <-(*memPipe)(s).toServer
	if !ok {
		return nil, io.EOF
	}
	return f, nil
}

func (s *memServerSide) Send(f *syncpb.ServerFrame) error {
	p := (*memPipe)(s)
	for _, out := range p.deliver(p.server, p.client, f) {
		sf, ok := out.(*syncpb.ServerFrame)
		if !ok {
			return fmt.Errorf("memory transport: filter returned %T on the server side", out)
		}
		p.toClient <- sf
	}
	return nil
}
