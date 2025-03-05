package logger

type NBLogger struct {
}

func NewBNLogger() NBLogger {
	return NBLogger{}
}

func (this_ *NBLogger) Debug(format string, v ...interface{}) {
	Log.Debug().Msgf(format, v...)
}
func (this_ *NBLogger) Info(format string, v ...interface{}) {
	Log.Info().Msgf(format, v...)
}
func (this_ *NBLogger) Warn(format string, v ...interface{}) {
	Log.Warn().Msgf(format, v...)
}
func (this_ *NBLogger) Error(format string, v ...interface{}) {
	Log.Error().Msgf(format, v...)
}
