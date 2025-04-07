// Package logger contains routines for logging messages to the console with support for asynchronous logging and
// colour-coding.
package logger

import (
	"fmt"
	"lab5/constants"
	"os"
	"time"
)

var timeStart = time.Now()

func toColourString(sID int) string {
	colourRange := 7
	idx := (sID % colourRange) + 8
	return fmt.Sprintf("\033[38;5;%dm", idx)
}

/*
Logger allows pretty-printed asynchronous logging and minimizes string formatting overheads.
*/
type Logger struct {
	colourString string
	serverPrefix string
	shouldLog    bool
	debugStart   time.Time
	topicMap     map[int]string
}

/*
Log a message with the given topic and format string.

@param topic: The topic of the log message.
@param format: The format string for the log message (see fmt.Sprintf: https://pkg.go.dev/fmt#Sprintf)
@param a: The arguments to the format string (see fmt.Sprintf: https://pkg.go.dev/fmt#Sprintf)
*/
func (l *Logger) Log(topic int, format string, a ...any) {
	if l.shouldLog {
		logMsg := fmt.Sprintf("%s[%v]@%6.3fs|[%v] %v",
			l.colourString,
			l.serverPrefix,
			float64(time.Since(l.debugStart).Microseconds())/1000.0,
			l.topicMap[topic],
			fmt.Sprintf(format, a...))
		if constants.SynchronousLogger {
			fmt.Println(logMsg)
		} else {
			go fmt.Println(logMsg)
		}
	}
}

/*
NewLogger creates a new logger.

@param shouldLog: Whether the logger should log messages.
@param serverPrefix: The prefix for the server.
@param topicMap: A map of enums to strings that can be printed as "sub-topics" in the log messages.
*/
func NewLogger(loggerId int, shouldLog bool, serverPrefix string, topicMap map[int]string) *Logger {
	loggingOverride, ok := os.LookupEnv("CPSC_416_LOGGER_OVERRIDE")
	if ok {
		shouldLog = loggingOverride == "true"
	}

	logger := &Logger{
		colourString: toColourString(loggerId),
		shouldLog:    shouldLog,
		serverPrefix: serverPrefix,
		debugStart:   timeStart,
		topicMap:     topicMap,
	}
	return logger
}
