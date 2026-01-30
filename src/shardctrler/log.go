package shardctrler

import (
	"fmt"
	"log"
	"sync/atomic"
	"time"
)

type LogTopic int32

const (
	LogNone LogTopic = iota
	LogRPC
	LogApply
	LogSnapshot
	LogDebug
	LogError
)

var LogMask int32 // 日志模块的掩码信息

func initLogger(topics ...LogTopic) {
	var mask int32
	for _, t := range topics {
		mask |= 1 << t
	}
	atomic.StoreInt32(&LogMask, mask)
}

func enabled(t LogTopic) bool {
	return atomic.LoadInt32(&LogMask)&(1<<t) != 0
}

func LogPrintf(t LogTopic, meta string, format string, a ...interface{}) {
	if !enabled(t) {
		return
	}

	now := time.Now().Format("15:04:05.000")
	msg := fmt.Sprintf(format, a...)
	log.Printf("[%s][%s] %s %s",
		now, TopicToString(t), meta, msg)
}

func TopicToString(t LogTopic) string {
	switch t {
	case LogRPC:
		return " RPC"
	case LogApply:
		return "APLY"
	case LogSnapshot:
		return "SNAP"
	case LogError:
		return " ERR"
	default:
		return "UNKN"
	}
}

func StringToTopic(strTopic []string) []LogTopic {
	if len(strTopic) == 0 {
		return []LogTopic{LogError}
	}

	topics := make([]LogTopic, 0)
	topics = append(topics, LogError)
topic_loop:
	for _, strTpc := range strTopic {
		switch strTpc {
		case "RPC":
			topics = append(topics, LogRPC)
		case "APLY":
			topics = append(topics, LogApply)
		case "SNAP":
			topics = append(topics, LogSnapshot)
		case "ERR":
			// 默认开启
		case "DBG":
			topics = append(topics, LogDebug)
		case "ALL":
			topics = make([]LogTopic, 0)
			topics = append(topics, LogRPC, LogApply, LogSnapshot, LogDebug, LogNone)
			break topic_loop
		default:
			log.Printf("init unknown Topic %v", strTpc)
		}
	}

	return topics
}
