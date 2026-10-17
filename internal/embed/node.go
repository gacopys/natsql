// Package embed starts and supervises a single NATS JetStream server in-process.
// Single-node only per D-55.
package embed

import (
	"fmt"
	"log/slog"
	"os"
	"time"

	natsserver "github.com/nats-io/nats-server/v2/server"
)

// NodeConfig configures an embedded NATS server.
// Zero values get sensible defaults via StartNode.
type NodeConfig struct {
	ServerName string
	Host       string
	Port       int

	// StoreDir is the JetStream store directory. When empty, the node uses a
	// new private temporary directory and deletes it on Shutdown, so state
	// never leaks between engines through a shared default path (OP-01).
	StoreDir string

	ReadyWait time.Duration

	// Logger receives the server's warnings and errors (and its notices at
	// debug level). Defaults to slog.Default().
	Logger *slog.Logger
}

// Node wraps a running NATS server instance.
type Node struct {
	srv     *natsserver.Server
	tempDir string // removed on Shutdown; empty when StoreDir was given
}

// StartNode starts a single-node embedded NATS JetStream server.
// Host defaults to "127.0.0.1", ReadyWait defaults to 10s.
// Port -1 selects a random available port; 0 means the NATS default, 4222.
func StartNode(cfg NodeConfig) (*Node, error) {
	if cfg.Host == "" {
		cfg.Host = "127.0.0.1"
	}
	if cfg.ReadyWait == 0 {
		cfg.ReadyWait = 10 * time.Second
	}
	if cfg.Logger == nil {
		cfg.Logger = slog.Default()
	}

	node := &Node{}
	if cfg.StoreDir == "" {
		dir, err := os.MkdirTemp("", "natsql-jetstream-")
		if err != nil {
			return nil, fmt.Errorf("embed: creating store dir: %w", err)
		}
		cfg.StoreDir, node.tempDir = dir, dir
	}

	opts := &natsserver.Options{
		ServerName: cfg.ServerName,
		Host:       cfg.Host,
		Port:       cfg.Port,
		JetStream:  true,
		StoreDir:   cfg.StoreDir,
		NoLog:      true, // the server's own logger is replaced by slogAdapter below
		NoSigs:     true,
	}
	srv, err := natsserver.NewServer(opts)
	if err != nil {
		node.removeTempDir()
		return nil, fmt.Errorf("embed: new server: %w", err)
	}
	srv.SetLogger(slogAdapter{cfg.Logger}, false, false)
	node.srv = srv

	go srv.Start()
	if !srv.ReadyForConnections(cfg.ReadyWait) {
		node.Shutdown()
		return nil, fmt.Errorf("embed: server not ready within %s", cfg.ReadyWait)
	}
	return node, nil
}

// ClientURL returns the NATS client URL for connecting to this server.
func (n *Node) ClientURL() string { return n.srv.ClientURL() }

// Shutdown stops the embedded NATS server, waits for it to fully stop, and
// deletes its store if it was a private temporary directory.
func (n *Node) Shutdown() {
	n.srv.Shutdown()
	n.srv.WaitForShutdown()
	n.removeTempDir()
}

// StoreDir returns the JetStream store directory in use.
func (n *Node) StoreDir() string { return n.srv.StoreDir() }

func (n *Node) removeTempDir() {
	if n.tempDir != "" {
		_ = os.RemoveAll(n.tempDir) // best effort: it is under the OS temp dir
	}
}

// Server returns the underlying NATS server instance.
func (n *Node) Server() *natsserver.Server { return n.srv }

// slogAdapter routes nats-server log lines to slog, so problems such as a
// full disk or JetStream failing to start are visible instead of discarded.
// Notices, of which the server prints many at startup, go to debug level.
type slogAdapter struct{ logger *slog.Logger }

func (a slogAdapter) Noticef(format string, v ...any) {
	a.logger.Debug(fmt.Sprintf(format, v...), "component", "nats-server")
}

func (a slogAdapter) Warnf(format string, v ...any) {
	a.logger.Warn(fmt.Sprintf(format, v...), "component", "nats-server")
}

func (a slogAdapter) Fatalf(format string, v ...any) {
	a.logger.Error(fmt.Sprintf(format, v...), "component", "nats-server")
}

func (a slogAdapter) Errorf(format string, v ...any) {
	a.logger.Error(fmt.Sprintf(format, v...), "component", "nats-server")
}

func (a slogAdapter) Debugf(format string, v ...any) {
	a.logger.Debug(fmt.Sprintf(format, v...), "component", "nats-server")
}

func (a slogAdapter) Tracef(format string, v ...any) {
	a.logger.Debug(fmt.Sprintf(format, v...), "component", "nats-server")
}
