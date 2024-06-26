package raft

import (
	"fmt"
	"log"
	"os"
	"strconv"
	"sync"
	"time"
)

type loglevel int

const (
	ERROR loglevel = iota
	WARNING
	DEBUG
	INFO
)

// 日志级别映射表
var logLevelPrefix = map[loglevel]string{
	DEBUG:   "DEBUG: ",
	INFO:    "INFO: ",
	WARNING: "WARNING: ",
	ERROR:   "ERROR: ",
}

type logModule string

const (
	dClient  logModule = "CLNT"
	dCommit  logModule = "CMIT"
	dDrop    logModule = "DROP"
	dLeader  logModule = "LEAD"
	dLog     logModule = "LOG1"
	dLog2    logModule = "LOG2"
	dPersist logModule = "PERS"
	dSnap    logModule = "SNAP"
	dTerm    logModule = "TERM"
	dTest    logModule = "TEST"
	dTimer   logModule = "TIMR"
	dTrace   logModule = "TRCE"
	dVote    logModule = "VOTE"
)

// Debugging
// const Debug = false
var (
	logStart   time.Time
	logVerbose loglevel
	logmu      sync.RWMutex
)

func getVerbose() int {
	v := os.Getenv("VERBOSE")
	level := 0
	if v != "" {
		var err error
		level, err = strconv.Atoi(v)
		if err != nil {
			log.Fatalf("Invalid verbosity %v", v)
		}
	}
	return level
}

func init() {
	logmu.Lock()
	defer logmu.Unlock()
	logStart = time.Now()
	logVerbose = loglevel(getVerbose())

	log.SetFlags(log.Flags() &^ (log.Ldate | log.Ltime))
}

func LOGPRINT(level loglevel, submodule logModule, format string, a ...interface{}) {
	logmu.RLock()
	defer logmu.RUnlock()
	if logVerbose >= level {
		time := time.Since(logStart).Microseconds()
		time /= 100
		logPrefix := fmt.Sprintf("%06d %v %-8s ", time, submodule, logLevelPrefix[level])
		format = logPrefix + format
		log.Printf(format, a...)
	}
}

/*func DPrintf(format string, a ...interface{}) (n int, err error) {
	if logVerbose >= DEBUG {
		log.Printf(format, a...)
	}
	return
}*/
