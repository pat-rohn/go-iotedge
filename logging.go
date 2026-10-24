package iotedge

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"strings"
	"sync/atomic"

	"github.com/sirupsen/logrus"
)

// LevelTrace is below slog.LevelDebug, for per-message detail.
const LevelTrace = slog.Level(-8)

// consoleLevel is the lowest level written to stderr (Verbosity, -v flag).
var consoleLevel = new(slog.LevelVar)

// persistLevel is the lowest level stored in the logs table
// (IoTConfig.LogPersistLevel); every stored line is a DB write.
var persistLevel atomic.Int64

// logTarget is the logs table server log lines go to; New sets it. slog's
// default logger is global, so this is too.
var logTarget atomic.Pointer[LoggingDB]

func init() {
	persistLevel.Store(int64(slog.LevelWarn))
	slog.SetDefault(slog.New(&persistHandler{
		next: slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: consoleLevel}),
	}))
	bridgeLogrus()
}

// SetLogLevel sets the lowest level written to stderr.
func SetLogLevel(level slog.Level) {
	consoleLevel.Set(level)
	syncLogrusLevel()
}

func setLogPersistLevel(level slog.Level) {
	persistLevel.Store(int64(level))
	syncLogrusLevel()
}

// syncLogrusLevel lets logrus drop what neither stderr nor the logs table
// wants before formatting it (timeseries traces every query).
func syncLogrusLevel() {
	lowest := min(consoleLevel.Level(), slog.Level(persistLevel.Load()))
	switch {
	case lowest <= LevelTrace:
		logrus.SetLevel(logrus.TraceLevel)
	case lowest <= slog.LevelDebug:
		logrus.SetLevel(logrus.DebugLevel)
	case lowest <= slog.LevelInfo:
		logrus.SetLevel(logrus.InfoLevel)
	case lowest <= slog.LevelWarn:
		logrus.SetLevel(logrus.WarnLevel)
	default:
		logrus.SetLevel(logrus.ErrorLevel)
	}
}

// ParseLogLevel reads "trace", "debug", "info", "warn"/"warning" or "error".
func ParseLogLevel(name string) (slog.Level, error) {
	switch strings.ToLower(strings.TrimSpace(name)) {
	case "trace":
		return LevelTrace, nil
	case "debug":
		return slog.LevelDebug, nil
	case "info":
		return slog.LevelInfo, nil
	case "warn", "warning":
		return slog.LevelWarn, nil
	case "error", "fatal", "panic":
		return slog.LevelError, nil
	}
	return 0, fmt.Errorf("unknown log level %q", name)
}

// persistServerLogs stores server log lines at persistLevel and above in db,
// including what the timeseries library logs through logrus.
func persistServerLogs(db *LoggingDB) {
	logTarget.Store(db)
}

// fatal logs at Error and exits, for unrecoverable startup errors.
func fatal(msg string, args ...any) {
	slog.Error(msg, args...)
	os.Exit(1)
}

// persistHandler writes records to next and, at persistLevel and above, to
// the logs table. Records with persisted_log=true (device logs, already
// stored by LogMessage) and the attribute device=<name> are honoured.
type persistHandler struct {
	next  slog.Handler
	attrs []slog.Attr
}

func (h *persistHandler) Enabled(ctx context.Context, level slog.Level) bool {
	return h.next.Enabled(ctx, level) || level >= slog.Level(persistLevel.Load())
}

func (h *persistHandler) Handle(ctx context.Context, r slog.Record) error {
	var err error
	if r.Level >= slog.Level(persistLevel.Load()) {
		err = h.persist(r)
	}
	if h.next.Enabled(ctx, r.Level) {
		if nextErr := h.next.Handle(ctx, r); nextErr != nil {
			return nextErr
		}
	}
	return err
}

func (h *persistHandler) WithAttrs(attrs []slog.Attr) slog.Handler {
	return &persistHandler{next: h.next.WithAttrs(attrs), attrs: append(append([]slog.Attr{}, h.attrs...), attrs...)}
}

func (h *persistHandler) WithGroup(name string) slog.Handler {
	return &persistHandler{next: h.next.WithGroup(name), attrs: h.attrs}
}

func (h *persistHandler) persist(r slog.Record) error {
	db := logTarget.Load()
	if db == nil {
		return nil
	}
	device, persisted := "server", false
	visit := func(a slog.Attr) bool {
		switch a.Key {
		case "device":
			if v := a.Value.String(); v != "" {
				device = v
			}
		case "persisted_log":
			persisted = a.Value.Kind() == slog.KindBool && a.Value.Bool()
		}
		return true
	}
	for _, a := range h.attrs {
		visit(a)
	}
	r.Attrs(visit)
	if persisted {
		return nil
	}
	level := Info
	switch {
	case r.Level >= slog.LevelError:
		level = Error
	case r.Level >= slog.LevelWarn:
		level = Warning
	case r.Level < slog.LevelInfo:
		level = Debug
	}
	query := `INSERT INTO logs (device, text, level) VALUES (?, ?, ?)`
	if db.Postgres() {
		query = `INSERT INTO logs (device, text, level) VALUES ($1, $2, $3)`
	}
	if _, err := db.DB.Exec(query, device, r.Message, int(level)); err != nil {
		return fmt.Errorf("persist server log: %w", err)
	}
	return nil
}

// bridgeLogrus routes logrus output (the timeseries library still uses it)
// into slog, so it follows the same levels and reaches the logs table.
func bridgeLogrus() {
	logrus.SetOutput(discard{})
	syncLogrusLevel()
	logrus.AddHook(logrusBridge{})
}

type discard struct{}

func (discard) Write(p []byte) (int, error) { return len(p), nil }

type logrusBridge struct{}

func (logrusBridge) Levels() []logrus.Level { return logrus.AllLevels }

func (logrusBridge) Fire(e *logrus.Entry) error {
	level := slog.LevelInfo
	switch e.Level {
	case logrus.PanicLevel, logrus.FatalLevel, logrus.ErrorLevel:
		level = slog.LevelError
	case logrus.WarnLevel:
		level = slog.LevelWarn
	case logrus.DebugLevel:
		level = slog.LevelDebug
	case logrus.TraceLevel:
		level = LevelTrace
	}
	if !slog.Default().Enabled(context.Background(), level) {
		return nil
	}
	args := make([]any, 0, 2*len(e.Data))
	for k, v := range e.Data {
		args = append(args, k, v)
	}
	slog.Log(context.Background(), level, e.Message, args...)
	return nil
}

func (s *IoTEdge) LogMessage(msg LogMessage) error {
	// persisted_log: InsertLogMessage below stores the message; the handler must not.
	logger := slog.With("fnct", "LogMessage", "device", msg.Device, "level", int(msg.Level), "persisted_log", true)
	switch msg.Level {
	case Debug:
		logger.Debug(msg.Text)
	case Warning:
		logger.Warn(msg.Text)
	case Error:
		logger.Error(msg.Text)
	default:
		logger.Info(msg.Text)
	}
	if err := s.LoggingDB.InsertLogMessage(msg); err != nil {
		logger.Error(fmt.Sprintf("failed to log message to DB: %v", err))
		return err
	}
	return nil
}
