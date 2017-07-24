// Package logrusadapter provides a logger that writes to a github.com/Sirupsen/logrus.Logger
// log.
package logrusadapter

import (
	"github.com/Sirupsen/logrus"
	"github.com/jackc/pgx"
)

type Logger struct {
	l *logrus.Logger
}

func NewLogger(l *logrus.Logger) *Logger {
	return &Logger{l: l}
}

func (l *Logger) Log(level pgx.LogLevel, msg string, data map[string]interface{}) {
	var logger logrus.FieldLogger
	if data != nil {
		logger = l.l.WithFields(data)
	} else {
		logger = l.l
	}

	switch level {
	case pgx.LogLevelTrace:
		logger.WithField("PGX_LOG_LEVEL", level).Debug(msg)
	case pgx.LogLevelDebug:
		logger.Debug(msg)
	case pgx.LogLevelInfo:
		logger.Info(msg)
	case pgx.LogLevelWarn:
		logger.Warn(msg)
	case pgx.LogLevelError:
		logger.Error(msg)
	default:
		logger.WithField("INVALID_PGX_LOG_LEVEL", level).Error(msg)
	}
}
