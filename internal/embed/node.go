// Package embed starts and supervises a single NATS JetStream server in-process.
// Single-node only per D-55.
package embed

import (
	"fmt"
	"time"

	natsserver "github.com/nats-io/nats-server/v2/server"
)

// NodeConfig configures an embedded NATS server.
// Zero values get sensible defaults via StartNode.
type NodeConfig struct {
	ServerName string
	Host       string
	Port       int
	StoreDir   string
	ReadyWait  time.Duration
}

// Node wraps a running NATS server instance.
type Node struct {
	srv *natsserver.Server
	cfg NodeConfig
}

// StartNode starts a single-node embedded NATS JetStream server.
// Host defaults to "127.0.0.1", ReadyWait defaults to 10s.
// Port 0 or -1 selects a random available port.
func StartNode(cfg NodeConfig) (*Node, error) {
	if cfg.Host == "" {
		cfg.Host = "127.0.0.1"
	}
	if cfg.ReadyWait == 0 {
		cfg.ReadyWait = 10 * time.Second
	}
	opts := &natsserver.Options{
		ServerName: cfg.ServerName,
		Host:       cfg.Host,
		Port:       cfg.Port,
		JetStream:  true,
		StoreDir:   cfg.StoreDir,
		NoLog:      true,
		NoSigs:     true,
	}
	srv, err := natsserver.NewServer(opts)
	if err != nil {
		return nil, fmt.Errorf("embed: new server: %w", err)
	}
	go srv.Start()
	if !srv.ReadyForConnections(cfg.ReadyWait) {
		srv.Shutdown()
		return nil, fmt.Errorf("embed: server not ready within %s", cfg.ReadyWait)
	}
	return &Node{srv: srv, cfg: cfg}, nil
}

// ClientURL returns the NATS client URL for connecting to this server.
func (n *Node) ClientURL() string { return n.srv.ClientURL() }

// Shutdown stops the embedded NATS server and waits for it to fully stop.
func (n *Node) Shutdown() {
	n.srv.Shutdown()
	n.srv.WaitForShutdown()
}

// Server returns the underlying NATS server instance.
func (n *Node) Server() *natsserver.Server { return n.srv }
