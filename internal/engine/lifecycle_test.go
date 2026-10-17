package engine_test

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"net"
	"net/http"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"

	natsqlpkg "github.com/gacopys/natsql/v2/internal/cfg"
	"github.com/gacopys/natsql/v2/internal/engine"
	"github.com/gacopys/natsql/v2/internal/kv"
	"github.com/gacopys/natsql/v2/internal/materialize"
)

func quietLogger() *slog.Logger { return slog.New(slog.DiscardHandler) }

// reviewConfig returns a one-view config over each named stream, view
// "review" first, then "review2", ...
func reviewConfig(streams ...string) *natsqlpkg.Config {
	cfg := &natsqlpkg.Config{}
	for i, stream := range streams {
		name := "review"
		if i > 0 {
			name += strconv.Itoa(i + 1)
		}
		cfg.Views = append(cfg.Views, natsqlpkg.ViewConfig{
			Name:         name,
			SourceStream: stream,
			KeyFields:    []string{"id"},
			Columns: []natsqlpkg.ColumnConfig{
				{Name: "id", From: "id", Type: natsqlpkg.ColumnTypeString, PrimaryKey: true},
				{Name: "value", From: "value", Type: natsqlpkg.ColumnTypeNumber},
			},
		})
	}
	return cfg
}

// pollUntil polls cond every 20ms until it holds or timeout passes.
func pollUntil(t *testing.T, timeout time.Duration, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for !cond() {
		if time.Now().After(deadline) {
			t.Fatalf("timed out after %v waiting for %s", timeout, what)
		}
		time.Sleep(20 * time.Millisecond)
	}
}

// rowCount queries the number of rows in view "review".
func rowCount(eng *engine.Engine) int {
	res := eng.Query(context.Background(), "SELECT id FROM review WHERE id != ''")
	if res.Error != nil {
		return -1
	}
	return len(res.Results)
}

// publish publishes body to stream REVIEW.
func publish(t *testing.T, js jetstream.JetStream, body string) {
	t.Helper()
	if _, err := js.Publish(context.Background(), "REVIEW.e", []byte(body)); err != nil {
		t.Fatalf("Publish failed: %v", err)
	}
}

// reservePort returns a port that is held by a listener until the test ends.
func reservePort(t *testing.T) int {
	t.Helper()
	var lc net.ListenConfig
	l, err := lc.Listen(context.Background(), "tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("Listen failed: %v", err)
	}
	t.Cleanup(func() { _ = l.Close() })
	addr, ok := l.Addr().(*net.TCPAddr)
	if !ok {
		t.Fatalf("unexpected address type %T", l.Addr())
	}
	return addr.Port
}

// TestStart_FailureReleasesEverything verifies that a Start that fails after
// starting materializers and the query subscription stops all of them, and
// that Start can be retried (LC-01).
func TestStart_FailureReleasesEverything(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	nc, js := startEmbeddedNATS(t)
	createStream(t, ctx, js, "REVIEW")

	busyPort := reservePort(t)
	eng, err := engine.New(nc, js, reviewConfig("REVIEW"), engine.WithQueryPort(busyPort), engine.WithLogger(quietLogger()))
	if err != nil {
		t.Fatalf("New failed: %v", err)
	}
	if err := eng.Start(ctx); err == nil || !strings.Contains(err.Error(), "HTTP listen failed") {
		t.Fatalf("Start with a busy port = %v, want a bind error", err)
	}
	if err := eng.Close(); !errors.Is(err, engine.ErrNotStarted) {
		t.Errorf("Close after failed Start = %v, want ErrNotStarted", err)
	}
	if s := eng.Stats(); s.Started || s.HTTPServing {
		t.Errorf("Stats after failed Start = %+v", s)
	}

	// Nothing may still be consuming or answering queries.
	publish(t, js, `{"id":"leak","value":1}`)
	if _, err := nc.Request(natsqlpkg.DefaultQuerySubject, []byte("SELECT id FROM review WHERE id = 'leak'"), 300*time.Millisecond); !errors.Is(err, nats.ErrNoResponders) {
		t.Errorf("query subscription still answering after failed Start: %v", err)
	}
	cons, err := js.Consumer(ctx, "REVIEW", materialize.ConsumerName("review"))
	if err != nil {
		t.Fatalf("Consumer failed: %v", err)
	}
	time.Sleep(300 * time.Millisecond) // give a leaked materializer time to pull
	if info, err := cons.Info(ctx); err != nil || info.NumPending != 1 || info.NumAckPending != 0 {
		t.Errorf("event consumed after failed Start: info=%+v err=%v", info, err)
	}

	// Retrying on a free port works and picks the event up.
	eng2, err := engine.New(nc, js, reviewConfig("REVIEW"), engine.WithQueryPort(0), engine.WithLogger(quietLogger()))
	if err != nil {
		t.Fatalf("New failed: %v", err)
	}
	if err := eng2.Start(ctx); err != nil {
		t.Fatalf("Start retry failed: %v", err)
	}
	defer func() { _ = eng2.Close() }()
	pollUntil(t, 10*time.Second, "the event to materialize", func() bool { return rowCount(eng2) == 1 })
}

// TestStart_MaterializerFailureStopsOtherViews verifies that when one view
// can't start, the views that did start are stopped again (LC-01).
func TestStart_MaterializerFailureStopsOtherViews(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	nc, js := startEmbeddedNATS(t)
	createStream(t, ctx, js, "REVIEW")

	eng, err := engine.New(nc, js, reviewConfig("REVIEW", "MISSING"), engine.WithQueryPort(0), engine.WithLogger(quietLogger()))
	if err != nil {
		t.Fatalf("New failed: %v", err)
	}
	err = eng.Start(ctx)
	if err == nil || !strings.Contains(err.Error(), `view "review2" failed to start`) {
		t.Fatalf("Start = %v, want review2's startup failure", err)
	}

	publish(t, js, `{"id":"a","value":1}`)
	time.Sleep(300 * time.Millisecond)
	cons, err := js.Consumer(ctx, "REVIEW", materialize.ConsumerName("review"))
	if err != nil {
		t.Fatalf("Consumer failed: %v", err)
	}
	if info, err := cons.Info(ctx); err != nil || info.NumPending != 1 {
		t.Errorf("view review kept consuming after Start failed: info=%+v err=%v", info, err)
	}
}

// TestStart_ContextOnlyBoundsStartup verifies that canceling the context
// passed to Start doesn't stop the materializers (LC-02).
func TestStart_ContextOnlyBoundsStartup(t *testing.T) {
	nc, js := startEmbeddedNATS(t)
	createStream(t, context.Background(), js, "REVIEW")

	eng, err := engine.New(nc, js, reviewConfig("REVIEW"), engine.WithQueryPort(0), engine.WithLogger(quietLogger()))
	if err != nil {
		t.Fatalf("New failed: %v", err)
	}
	startCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	if err := eng.Start(startCtx); err != nil {
		t.Fatalf("Start failed: %v", err)
	}
	cancel()
	defer func() { _ = eng.Close() }()

	publish(t, js, `{"id":"after-cancel","value":1}`)
	pollUntil(t, 10*time.Second, "an event published after Start's ctx ended to materialize", func() bool {
		return rowCount(eng) == 1
	})
	if s := eng.Stats(); !s.Ready || s.ViewStatus[0].State != "running" {
		t.Errorf("Stats = %+v, want ready and running", s)
	}
}

// slowSetupJS delays and then fails the stream lookup consumer setup does.
type slowSetupJS struct {
	jetstream.JetStream
	delay time.Duration
}

func (s slowSetupJS) Stream(ctx context.Context, name string) (jetstream.Stream, error) {
	if name == "SLOW" {
		select {
		case <-time.After(s.delay):
			return nil, errors.New("injected slow setup failure")
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}
	return s.JetStream.Stream(ctx, name)
}

// TestStart_WaitsForReadiness verifies that Start waits for consumer setup
// instead of declaring success after a fixed delay (LC-03), and that queries
// and Stats don't wait for Start meanwhile (LC-04).
func TestStart_WaitsForReadiness(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	nc, js := startEmbeddedNATS(t)
	cfg := reviewConfig("SLOW")

	eng, err := engine.New(nc, slowSetupJS{JetStream: js, delay: 1500 * time.Millisecond}, cfg, engine.WithQueryPort(0), engine.WithLogger(quietLogger()))
	if err != nil {
		t.Fatalf("New failed: %v", err)
	}

	startErr := make(chan error, 1)
	go func() { startErr <- eng.Start(ctx) }()

	time.Sleep(700 * time.Millisecond) // well inside Start, past the old 500ms guess
	began := time.Now()
	res := eng.Query(ctx, "SELECT * FROM review WHERE id = 'a'")
	stats := eng.Stats()
	if waited := time.Since(began); waited > 500*time.Millisecond {
		t.Errorf("Query and Stats waited %v for Start", waited)
	}
	if res.Error != nil || len(res.Results) != 0 {
		t.Errorf("query during Start = %+v, want the published, still empty view", res)
	}
	if stats.Started || stats.Ready {
		t.Errorf("Stats during Start = %+v, want not started", stats)
	}

	select {
	case err := <-startErr:
		if err == nil {
			_ = eng.Close()
			t.Fatal("Start succeeded although consumer setup failed after 1.5s")
		}
	case <-time.After(20 * time.Second):
		t.Fatal("Start did not return")
	}
}

// TestEngine_RestartsFailedMaterializer verifies that a materializer that
// stops after startup is restarted, with the restart visible in Stats and
// readiness (LC-03, OP-03).
func TestEngine_RestartsFailedMaterializer(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	nc, js := startEmbeddedNATS(t)
	createStream(t, ctx, js, "REVIEW")

	eng, err := engine.New(nc, js, reviewConfig("REVIEW"), engine.WithQueryPort(0), engine.WithLogger(quietLogger()))
	if err != nil {
		t.Fatalf("New failed: %v", err)
	}
	if err := eng.Start(ctx); err != nil {
		t.Fatalf("Start failed: %v", err)
	}
	defer func() { _ = eng.Close() }()

	// Deleting the durable consumer ends the running materializer.
	if err := js.DeleteConsumer(ctx, "REVIEW", materialize.ConsumerName("review")); err != nil {
		t.Fatalf("DeleteConsumer failed: %v", err)
	}
	pollUntil(t, 30*time.Second, "the materializer to be restarted", func() bool {
		s := eng.Stats()
		return s.Ready && s.ViewStatus[0].Restarts >= 1
	})
	if s := eng.Stats(); s.ViewStatus[0].LastError == "" || s.ViewStatus[0].LastErrorAt == nil {
		t.Errorf("view status after a failure = %+v, want the error recorded", s.ViewStatus[0])
	}

	publish(t, js, `{"id":"after-restart","value":1}`)
	pollUntil(t, 10*time.Second, "an event to materialize after the restart", func() bool {
		return rowCount(eng) == 1
	})
}

// TestEngine_ReadinessEndpoint verifies /healthz and /readyz over HTTP
// (OP-03).
func TestEngine_ReadinessEndpoint(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	nc, js := startEmbeddedNATS(t)
	createStream(t, ctx, js, "REVIEW")

	eng, err := engine.New(nc, js, reviewConfig("REVIEW"), engine.WithQueryPort(0), engine.WithLogger(quietLogger()))
	if err != nil {
		t.Fatalf("New failed: %v", err)
	}
	if err := eng.Start(ctx); err != nil {
		t.Fatalf("Start failed: %v", err)
	}
	defer func() { _ = eng.Close() }()
	publish(t, js, `{"id":"a","value":1}`)
	pollUntil(t, 10*time.Second, "the event to materialize", func() bool { return rowCount(eng) == 1 })

	base := "http://" + eng.Stats().HTTPAddr
	for _, path := range []string{"/healthz", "/readyz"} {
		req, err := http.NewRequestWithContext(ctx, http.MethodGet, base+path, nil)
		if err != nil {
			t.Fatalf("NewRequest failed: %v", err)
		}
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Fatalf("GET %s failed: %v", path, err)
		}
		body, err := io.ReadAll(resp.Body)
		_ = resp.Body.Close()
		if err != nil {
			t.Fatalf("reading %s failed: %v", path, err)
		}
		if resp.StatusCode != http.StatusOK {
			t.Errorf("GET %s = %d, want 200: %s", path, resp.StatusCode, body)
		}
		var stats engine.Stats
		if err := json.Unmarshal(body, &stats); err != nil {
			t.Fatalf("GET %s body is not Stats: %v", path, err)
		}
		if !stats.Ready || len(stats.ViewStatus) != 1 || stats.ViewStatus[0].EventsProcessed != 1 || stats.Queries == 0 {
			t.Errorf("GET %s = %+v", path, stats)
		}
	}
}

// TestEngine_QuerySubject verifies that the NATS endpoint listens on the
// configured subject (TR-01).
func TestEngine_QuerySubject(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	nc, js := startEmbeddedNATS(t)

	cfg := &natsqlpkg.Config{NATS: natsqlpkg.NATSConfig{QuerySubject: "tenant_a.query"}}
	eng, err := engine.New(nc, js, cfg, engine.WithQueryPort(0), engine.WithLogger(quietLogger()))
	if err != nil {
		t.Fatalf("New failed: %v", err)
	}
	if err := eng.Start(ctx); err != nil {
		t.Fatalf("Start failed: %v", err)
	}
	defer func() { _ = eng.Close() }()

	if _, err := nc.Request("tenant_a.query", []byte("SELECT * FROM v WHERE id = 'a'"), 2*time.Second); err != nil {
		t.Errorf("request on the configured subject failed: %v", err)
	}
	if _, err := nc.Request(natsqlpkg.DefaultQuerySubject, []byte("SELECT * FROM v WHERE id = 'a'"), 300*time.Millisecond); !errors.Is(err, nats.ErrNoResponders) {
		t.Errorf("default subject still answered: %v", err)
	}
}

// TestStart_RefusesChangedDefinition verifies that a view whose definition
// changed since it was materialized is refused until it is dropped, and is
// rebuilt from its source stream afterwards (WR-09).
func TestStart_RefusesChangedDefinition(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	nc, js := startEmbeddedNATS(t)
	createStream(t, ctx, js, "REVIEW")
	publish(t, js, `{"id":"a","value":1,"label":"x"}`)

	first, err := engine.New(nc, js, reviewConfig("REVIEW"), engine.WithQueryPort(0), engine.WithLogger(quietLogger()))
	if err != nil {
		t.Fatalf("New failed: %v", err)
	}
	if err := first.Start(ctx); err != nil {
		t.Fatalf("Start failed: %v", err)
	}
	pollUntil(t, 10*time.Second, "the event to materialize", func() bool { return rowCount(first) == 1 })
	if err := first.Close(); err != nil {
		t.Fatalf("Close failed: %v", err)
	}

	// Restarting with the same definition resumes.
	same, err := engine.New(nc, js, reviewConfig("REVIEW"), engine.WithQueryPort(0), engine.WithLogger(quietLogger()))
	if err != nil {
		t.Fatalf("New failed: %v", err)
	}
	if err := same.Start(ctx); err != nil {
		t.Fatalf("Start with an unchanged definition failed: %v", err)
	}
	if err := same.Close(); err != nil {
		t.Fatalf("Close failed: %v", err)
	}

	changed := reviewConfig("REVIEW")
	changed.Views[0].Columns = append(changed.Views[0].Columns, natsqlpkg.ColumnConfig{Name: "label", From: "label", Type: natsqlpkg.ColumnTypeString})
	eng, err := engine.New(nc, js, changed, engine.WithQueryPort(0), engine.WithLogger(quietLogger()))
	if err != nil {
		t.Fatalf("New failed: %v", err)
	}
	if err := eng.Start(ctx); !errors.Is(err, engine.ErrViewRebuildRequired) {
		t.Fatalf("Start with a changed definition = %v, want ErrViewRebuildRequired", err)
	}

	if err := eng.DropView(ctx, "review"); err != nil {
		t.Fatalf("DropView failed: %v", err)
	}
	if err := eng.Start(ctx); err != nil {
		t.Fatalf("Start after DropView failed: %v", err)
	}
	defer func() { _ = eng.Close() }()
	if err := eng.DropView(ctx, "review"); !errors.Is(err, engine.ErrAlreadyStarted) {
		t.Errorf("DropView on a running engine = %v, want ErrAlreadyStarted", err)
	}

	pollUntil(t, 10*time.Second, "the view to be rebuilt with the new column", func() bool {
		res := eng.Query(ctx, "SELECT label FROM review WHERE id = 'a'")
		return res.Error == nil && len(res.Results) == 1 && res.Results[0]["label"] == "x"
	})
}

// TestStart_DefinitionCanChangeBeforeMaterializing verifies that a view
// whose Start failed before materializing anything (here: a missing source
// stream) can be reconfigured without a rebuild (WR-09).
func TestStart_DefinitionCanChangeBeforeMaterializing(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	nc, js := startEmbeddedNATS(t)
	createStream(t, ctx, js, "REVIEW")

	typo, err := engine.New(nc, js, reviewConfig("MISSPELLED_STREAM"), engine.WithQueryPort(0), engine.WithLogger(quietLogger()))
	if err != nil {
		t.Fatalf("New failed: %v", err)
	}
	if err := typo.Start(ctx); err == nil {
		t.Fatal("Start on a missing stream succeeded")
	}

	fixed, err := engine.New(nc, js, reviewConfig("REVIEW"), engine.WithQueryPort(0), engine.WithLogger(quietLogger()))
	if err != nil {
		t.Fatalf("New failed: %v", err)
	}
	if err := fixed.Start(ctx); err != nil {
		t.Fatalf("Start after fixing the source stream = %v, want success without a rebuild", err)
	}
	defer func() { _ = fixed.Close() }()
	publish(t, js, `{"id":"a","value":1}`)
	pollUntil(t, 10*time.Second, "the event to materialize", func() bool { return rowCount(fixed) == 1 })
}

// TestStart_RefusesEarlierKeyLayout verifies that a view materialized by an
// earlier version (a durable consumer, rows in the "{view}/pk/" layout) must
// be dropped before it is resumed, and that DropView removes the old rows
// (WR-09, PF-01).
func TestStart_RefusesEarlierKeyLayout(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	nc, js := startEmbeddedNATS(t)
	createStream(t, ctx, js, "REVIEW")
	publish(t, js, `{"id":"a","value":1}`)

	// The state an earlier version leaves behind.
	stream, err := js.Stream(ctx, "REVIEW")
	if err != nil {
		t.Fatalf("Stream failed: %v", err)
	}
	if _, err := stream.CreateOrUpdateConsumer(ctx, jetstream.ConsumerConfig{Durable: materialize.ConsumerName("review"), AckPolicy: jetstream.AckExplicitPolicy}); err != nil {
		t.Fatalf("creating the old consumer failed: %v", err)
	}
	kvb, err := kv.InitBucket(ctx, js, 1)
	if err != nil {
		t.Fatalf("InitBucket failed: %v", err)
	}
	for _, key := range []string{"review/pk/a", "review/meta/schema"} {
		if _, err := kvb.Put(ctx, key, []byte(`{"id":"a","value":1}`)); err != nil {
			t.Fatalf("Put failed: %v", err)
		}
	}

	eng, err := engine.New(nc, js, reviewConfig("REVIEW"), engine.WithQueryPort(0), engine.WithLogger(quietLogger()))
	if err != nil {
		t.Fatalf("New failed: %v", err)
	}
	err = eng.Start(ctx)
	if !errors.Is(err, engine.ErrViewRebuildRequired) || !strings.Contains(err.Error(), "drop-view") {
		t.Fatalf("Start over an earlier layout = %v, want ErrViewRebuildRequired with instructions", err)
	}

	if err := eng.DropView(ctx, "review"); err != nil {
		t.Fatalf("DropView failed: %v", err)
	}
	if _, err := kvb.Get(ctx, "review/pk/a"); !errors.Is(err, jetstream.ErrKeyNotFound) {
		t.Errorf("old-layout row survived DropView: %v", err)
	}
	if err := eng.Start(ctx); err != nil {
		t.Fatalf("Start after DropView failed: %v", err)
	}
	defer func() { _ = eng.Close() }()
	pollUntil(t, 10*time.Second, "the view to be replayed", func() bool { return rowCount(eng) == 1 })
}

// TestQuery_DuringCloseDoesNotDeadlock verifies that queries arriving over
// HTTP and NATS while Close runs are answered or refused promptly, and
// that Close finishes (LC-04).
func TestQuery_DuringCloseDoesNotDeadlock(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	nc, js := startEmbeddedNATS(t)
	createStream(t, ctx, js, "REVIEW")

	eng, err := engine.New(nc, js, reviewConfig("REVIEW"), engine.WithQueryPort(0), engine.WithLogger(quietLogger()))
	if err != nil {
		t.Fatalf("New failed: %v", err)
	}
	if err := eng.Start(ctx); err != nil {
		t.Fatalf("Start failed: %v", err)
	}

	var stop atomic.Bool
	done := make(chan struct{})
	go func() {
		defer close(done)
		for !stop.Load() {
			_, _ = nc.Request(natsqlpkg.DefaultQuerySubject, []byte("SELECT * FROM review WHERE value = 1"), time.Second) // load only; failures during Close are expected
			_ = eng.Query(ctx, "SELECT * FROM review WHERE id = 'a'")
		}
	}()
	time.Sleep(100 * time.Millisecond)

	began := time.Now()
	if err := eng.Close(); err != nil {
		t.Fatalf("Close failed: %v", err)
	}
	if took := time.Since(began); took > 5*time.Second {
		t.Errorf("Close took %v under query load", took)
	}
	stop.Store(true)
	<-done

	if res := eng.Query(ctx, "SELECT * FROM review WHERE id = 'a'"); res.Error != nil {
		t.Errorf("Query after Close = %s, want it to keep working while the connection is open", *res.Error)
	}
}
