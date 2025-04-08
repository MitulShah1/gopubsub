package logger

import (
	"os"

	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

// Level represents the logging level
type Level int

const (
	// DebugLevel is for detailed debugging information
	DebugLevel Level = iota
	// InfoLevel is for general information
	InfoLevel
	// WarnLevel is for warning messages
	WarnLevel
	// ErrorLevel is for error messages
	ErrorLevel
)

// Fields is a map of key-value pairs for structured logging
type Fields map[string]interface{}

// Logger is the main logger interface
type Logger interface {
	Debug(msg string, fields Fields)
	Info(msg string, fields Fields)
	Warn(msg string, fields Fields)
	Error(msg string, fields Fields)
	WithFields(fields Fields) Logger
}

// DefaultLogger is the default implementation of Logger using Zap
type DefaultLogger struct {
	zap *zap.Logger
}

// New creates a new logger with the specified level
func New(level Level) Logger {
	// Create encoder config
	encoderConfig := zapcore.EncoderConfig{
		TimeKey:        "timestamp",
		LevelKey:       "level",
		NameKey:        "logger",
		CallerKey:      "caller",
		MessageKey:     "msg",
		StacktraceKey:  "stacktrace",
		LineEnding:     zapcore.DefaultLineEnding,
		EncodeLevel:    zapcore.LowercaseLevelEncoder,
		EncodeTime:     zapcore.ISO8601TimeEncoder,
		EncodeDuration: zapcore.SecondsDurationEncoder,
		EncodeCaller:   zapcore.ShortCallerEncoder,
	}

	// Create core
	core := zapcore.NewCore(
		zapcore.NewJSONEncoder(encoderConfig),
		zapcore.AddSync(os.Stdout),
		zapcore.Level(level),
	)

	// Create logger
	logger := zap.New(core, zap.AddCaller())

	return &DefaultLogger{
		zap: logger,
	}
}

// WithFields creates a new logger with additional fields
func (l *DefaultLogger) WithFields(fields Fields) Logger {
	// Convert Fields to zap.Field slice
	zapFields := make([]zap.Field, 0, len(fields))
	for k, v := range fields {
		zapFields = append(zapFields, zap.Any(k, v))
	}

	// Create new logger with fields
	newLogger := l.zap.With(zapFields...)

	return &DefaultLogger{
		zap: newLogger,
	}
}

// Debug logs a debug message
func (l *DefaultLogger) Debug(msg string, fields Fields) {
	zapFields := make([]zap.Field, 0, len(fields))
	for k, v := range fields {
		zapFields = append(zapFields, zap.Any(k, v))
	}
	l.zap.Debug(msg, zapFields...)
}

// Info logs an info message
func (l *DefaultLogger) Info(msg string, fields Fields) {
	zapFields := make([]zap.Field, 0, len(fields))
	for k, v := range fields {
		zapFields = append(zapFields, zap.Any(k, v))
	}
	l.zap.Info(msg, zapFields...)
}

// Warn logs a warning message
func (l *DefaultLogger) Warn(msg string, fields Fields) {
	zapFields := make([]zap.Field, 0, len(fields))
	for k, v := range fields {
		zapFields = append(zapFields, zap.Any(k, v))
	}
	l.zap.Warn(msg, zapFields...)
}

// Error logs an error message
func (l *DefaultLogger) Error(msg string, fields Fields) {
	zapFields := make([]zap.Field, 0, len(fields))
	for k, v := range fields {
		zapFields = append(zapFields, zap.Any(k, v))
	}
	l.zap.Error(msg, zapFields...)
}
