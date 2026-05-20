package lifecycle

// boot_log.go — append-only JSON log of boot failures (e.g. ~/.rune/logs/boot.log).
// Complementary to lifecycle.Manager.LastBootError() (in-memory, latest only)
// and slog (stderr / handler chain). The on-disk log is the fallback for
// cases the structured classifier didn't fully describe — humans / admins
// can grep across attempts.
//
// Design:
//   - One JSON object per line ({ "time": ..., "kind": ..., ... })
//   - Size-based rotation: when a write would exceed maxBytes, the current
//     file is rotated to "<path>.1" (previous .1 overwritten) and a fresh
//     file is opened. Two-file scheme, no external deps.
//   - Best-effort: open/write errors do NOT propagate (logging must not
//     take down the boot loop).
//   - Sensitive substrings in `detail` are redacted via obs.Redact before
//     writing — same patterns the slog handler uses.
//   - The destination path is injected (see NewBootLogger) so callers control
//     the root via bootstrap.Paths and tests can write to a temp dir.

import (
	"encoding/json"
	"log/slog"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/envector/rune-go/internal/domain"
	"github.com/envector/rune-go/internal/obs"
)

// DefaultBootLogMaxBytes — rotation threshold when none is supplied.
const DefaultBootLogMaxBytes int64 = 1 << 20 // 1 MiB

// BootLogger owns the on-disk boot-failure log. Construct with NewBootLogger
// and inject into Manager via Manager.SetBootLog. A nil *BootLogger is a safe
// no-op so callers (and tests) can leave it unset.
type BootLogger struct {
	mu       sync.Mutex
	path     string
	maxBytes int64
	file     *os.File
	// resolved tracks whether we've tried to open at least once — failures
	// after that switch to silent (avoid spamming slog with the same error).
	resolved bool
}

// NewBootLogger returns a logger that appends to path, rotating at maxBytes
// (DefaultBootLogMaxBytes when maxBytes <= 0). The file is opened lazily on
// the first Persist call.
func NewBootLogger(path string, maxBytes int64) *BootLogger {
	if maxBytes <= 0 {
		maxBytes = DefaultBootLogMaxBytes
	}
	return &BootLogger{path: path, maxBytes: maxBytes}
}

// bootLogEntry is the JSON shape written per line. Pinned schema so admins
// can write scripts against it without worrying about field renames.
type bootLogEntry struct {
	Time     time.Time            `json:"time"`
	Kind     domain.BootErrorKind `json:"kind"`
	Phase    domain.BootPhase     `json:"phase,omitempty"`
	Detail   string               `json:"detail"`
	Hint     string               `json:"hint,omitempty"`
	Attempts int                  `json:"attempts,omitempty"`
}

// Persist appends a JSON line describing the boot error. No-op when the
// receiver or be is nil. Failures are swallowed (logged once via slog, then
// silent) so the log writer can't break the boot loop.
func (bl *BootLogger) Persist(be *domain.BootError) {
	if bl == nil || be == nil {
		return
	}
	bl.mu.Lock()
	defer bl.mu.Unlock()

	if bl.file == nil {
		if err := bl.openLocked(); err != nil {
			if !bl.resolved {
				slog.Warn("boot_log: file open failed — boot errors will not be persisted",
					"err", err, "path", bl.path)
			}
			bl.resolved = true
			return
		}
	}

	entry := bootLogEntry{
		Time:     be.At,
		Kind:     be.Kind,
		Phase:    be.Phase,
		Detail:   obs.Redact(be.Detail),
		Hint:     be.Hint, // hint never contains secrets by construction
		Attempts: be.Attempts,
	}
	line, err := json.Marshal(entry)
	if err != nil {
		// json.Marshal failing on these stdlib types should be impossible,
		// but be defensive.
		slog.Warn("boot_log: marshal failed", "err", err)
		return
	}
	line = append(line, '\n')

	bl.rotateIfNeededLocked(int64(len(line)))
	if bl.file == nil {
		return // rotation failed to reopen; openLocked already logged
	}
	if _, err := bl.file.Write(line); err != nil {
		slog.Warn("boot_log: write failed", "err", err)
	}
}

// Close closes the underlying file. Called from graceful shutdown so the OS
// flushes the buffer. Idempotent and nil-safe; satisfies the Closer interface.
func (bl *BootLogger) Close() error {
	if bl == nil {
		return nil
	}
	bl.mu.Lock()
	defer bl.mu.Unlock()
	if bl.file != nil {
		err := bl.file.Close()
		bl.file = nil
		return err
	}
	return nil
}

// Path returns the destination path. Nil-safe (returns "").
func (bl *BootLogger) Path() string {
	if bl == nil {
		return ""
	}
	return bl.path
}

// openLocked mkdir -p the logs directory + opens the log file in O_APPEND mode
// with 0600 perms (file is owner-only since detail may contain internal
// endpoints / error strings). Caller must hold bl.mu.
func (bl *BootLogger) openLocked() error {
	dir := filepath.Dir(bl.path)
	if err := os.MkdirAll(dir, 0o700); err != nil {
		return err
	}
	f, err := os.OpenFile(bl.path, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0o600)
	if err != nil {
		return err
	}
	bl.file = f
	return nil
}

// rotateIfNeededLocked rotates the current file to "<path>.1" when appending
// `incoming` bytes would push it past maxBytes, then reopens a fresh file. The
// previous ".1" is overwritten (two-file scheme). Best-effort: on any error it
// leaves bl.file as-is or nil and lets the caller skip the write. Caller must
// hold bl.mu.
func (bl *BootLogger) rotateIfNeededLocked(incoming int64) {
	info, err := bl.file.Stat()
	if err != nil {
		return
	}
	if info.Size()+incoming <= bl.maxBytes {
		return
	}
	_ = bl.file.Close()
	bl.file = nil
	// Overwrite any prior rotation; we keep exactly one generation of history.
	if err := os.Rename(bl.path, bl.path+".1"); err != nil {
		slog.Warn("boot_log: rotate rename failed", "err", err, "path", bl.path)
		// Fall through and try to reopen the original so we don't lose new writes.
	}
	if err := bl.openLocked(); err != nil {
		if !bl.resolved {
			slog.Warn("boot_log: reopen after rotate failed", "err", err, "path", bl.path)
		}
		bl.resolved = true
	}
}
