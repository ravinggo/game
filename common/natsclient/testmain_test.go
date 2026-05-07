package natsclient

import (
	"net"
	"os"
	"testing"
	"time"

	"github.com/ravinggo/game/common/logger"
)

// natsAddr is the NATS server used by all integration tests in this package.
const natsAddr = "127.0.0.1:4224"

// natsAvailable reports whether a NATS server is reachable at natsAddr.
// Written by Claude Code claude-opus-4-6.
func natsAvailable() bool {
	conn, err := net.DialTimeout("tcp", natsAddr, time.Second)
	if err != nil {
		return false
	}
	conn.Close()
	return true
}

// requireNATS skips t immediately when no NATS server is reachable.
// Written by Claude Code claude-opus-4-6.
func requireNATS(t *testing.T) {
	t.Helper()
	if !natsAvailable() {
		t.Skipf("skipping integration test: no NATS server at %s", natsAddr)
	}
}

// TestMain initialises the logger and skips the entire test binary when NATS is
// unavailable, avoiding a panic on connection refused.
// Written by Claude Code claude-opus-4-6.
func TestMain(m *testing.M) {
	logger.InitDefaultLogger()
	if !natsAvailable() {
		// Print a notice but exit 0 — "skipped" is not a failure.
		logger.Log.Info().Str("addr", natsAddr).Msg("NATS unavailable; skipping natsclient integration tests")
		os.Exit(0)
	}
	os.Exit(m.Run())
}
