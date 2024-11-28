package newrelicreceiver

import (
    "strings"
    "time"

    "go.opentelemetry.io/collector/pdata/pcommon"
    "go.opentelemetry.io/collector/pdata/plog"
)

type newRelicLog struct {
    Message   string `json:"message"`
    Timestamp int64  `json:"timestamp"`
    Severity  string `json:"severity"`
    Service   string `json:"service"`
}

func convertNewRelicLogsToOTEL(nrLogs []newRelicLog) plog.Logs {
    logs := plog.NewLogs()
    rl := logs.ResourceLogs().AppendEmpty()
    scopeLogs := rl.ScopeLogs().AppendEmpty()

    for _, nrLog := range nrLogs {
        logRecord := scopeLogs.LogRecords().AppendEmpty()
        logRecord.SetTimestamp(pcommon.NewTimestampFromTime(time.Unix(0, nrLog.Timestamp*int64(time.Millisecond))))
        logRecord.Body().SetStr(nrLog.Message)
        logRecord.SetSeverityText(nrLog.Severity)
        logRecord.Attributes().PutStr("service.name", nrLog.Service)
    }

    return logs
}

func splitTags(tags string) map[string]string {
    tagMap := make(map[string]string)
    if tags == "" {
        return tagMap
    }
    for _, tag := range strings.Split(tags, ",") {
        kv := strings.Split(tag, ":")
        if len(kv) == 2 && kv[1] != "" && kv[0] != "" {
            tagMap[strings.TrimSpace(kv[0])] = strings.TrimSpace(kv[1])
        }
    }
    return tagMap
}
