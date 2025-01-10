// Copyright 2024-2025 CardinalHQ, Inc
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package githubeventsreceiver

import (
	"time"

	"github.com/google/go-github/v63/github"
	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/plog"
)

const (
	eventTypeWorkflowRun = "github.event.workflow.run"
	eventTypeWorkflowJob = "github.event.workflow.job"
)

func createLogs(eventType interface{}, payload []byte) plog.Logs {
	switch event := eventType.(type) {
	case *github.WorkflowJobEvent:
		return createLogsWorkflowJobEvent(event, payload)

	case *github.WorkflowRunEvent:
		return createLogsWorkflowRunEvent(event, payload)

	default:
		return plog.Logs{}
	}
}

func createLogsWorkflowJobEvent(event *github.WorkflowJobEvent, payload []byte) plog.Logs {
	log := plog.NewLogs()
	resourceLog := log.ResourceLogs().AppendEmpty()
	scopeLog := resourceLog.ScopeLogs().AppendEmpty()
	logRecord := scopeLog.LogRecords().AppendEmpty()

	logRecord.Body().SetStr(string(payload))
	logRecord.Attributes().PutStr("event.name", eventTypeWorkflowJob)
	logRecord.Attributes().PutStr("github.workflow.name", *event.GetWorkflowJob().WorkflowName)
	logRecord.Attributes().PutStr("github.repository.owner.login", *event.Repo.Owner.Login)
	logRecord.Attributes().PutInt("github.repository.owner.id", *event.Repo.Owner.ID)

	logRecord.Attributes().PutStr("github.workflow.job.created_at", event.GetWorkflowJob().GetCreatedAt().Format(time.RFC3339))
	logRecord.Attributes().PutStr("github.workflow.job.completed_at", event.GetWorkflowJob().GetCompletedAt().Format(time.RFC3339))
	logRecord.Attributes().PutStr("github.workflow.job.conclusion", event.GetWorkflowJob().GetConclusion())
	logRecord.Attributes().PutStr("github.workflow.job.head_branch", event.GetWorkflowJob().GetHeadBranch())
	logRecord.Attributes().PutStr("github.workflow.job.head_sha", event.GetWorkflowJob().GetHeadSHA())
	logRecord.Attributes().PutStr("github.workflow.job.html_url", event.GetWorkflowJob().GetHTMLURL())
	logRecord.Attributes().PutInt("github.workflow.job.id", event.GetWorkflowJob().GetID())

	logRecord.Attributes().PutStr("github.workflow.job.name", event.GetWorkflowJob().GetName())
	logRecord.Attributes().PutInt("github.workflow.job.run_attempt", event.GetWorkflowJob().GetRunAttempt())
	logRecord.Attributes().PutInt("github.workflow.job.run_id", event.GetWorkflowJob().GetRunID())
	logRecord.Attributes().PutStr("github.workflow.job.runner.group_name", event.GetWorkflowJob().GetRunnerGroupName())
	logRecord.Attributes().PutStr("github.workflow.job.runner.name", event.GetWorkflowJob().GetRunnerName())
	logRecord.Attributes().PutStr("github.workflow.job.sender.login", event.GetSender().GetLogin())
	logRecord.Attributes().PutStr("github.workflow.job.started_at", event.GetWorkflowJob().GetStartedAt().Format(time.RFC3339))
	logRecord.Attributes().PutStr("github.workflow.job.status", event.GetWorkflowJob().GetStatus())

	//TODO: This may not be correct?
	logRecord.SetTimestamp(pcommon.NewTimestampFromTime(time.Now()))

	return log
}

func createLogsWorkflowRunEvent(event *github.WorkflowRunEvent, payload []byte) plog.Logs {
	log := plog.NewLogs()
	resourceLog := log.ResourceLogs().AppendEmpty()
	scopeLog := resourceLog.ScopeLogs().AppendEmpty()
	logRecord := scopeLog.LogRecords().AppendEmpty()

	logRecord.Body().SetStr(string(payload))
	logRecord.Attributes().PutStr("event.name", eventTypeWorkflowRun)
	logRecord.Attributes().PutStr("github.repository.owner.login", *event.Repo.Owner.Login)
	logRecord.Attributes().PutInt("github.repository.owner.id", *event.Repo.Owner.ID)
	logRecord.Attributes().PutStr("github.workflow.run.conclusion", event.GetWorkflowRun().GetConclusion())
	logRecord.Attributes().PutStr("github.workflow.run.created_at", event.GetWorkflowRun().GetCreatedAt().Format(time.RFC3339))
	logRecord.Attributes().PutStr("github.workflow.run.display_title", event.GetWorkflowRun().GetDisplayTitle())
	logRecord.Attributes().PutStr("github.workflow.run.event", event.GetWorkflowRun().GetEvent())
	logRecord.Attributes().PutStr("github.workflow.run.head_branch", event.GetWorkflowRun().GetHeadBranch())
	logRecord.Attributes().PutStr("github.workflow.run.head_sha", event.GetWorkflowRun().GetHeadSHA())
	logRecord.Attributes().PutStr("github.workflow.run.html_url", event.GetWorkflowRun().GetHTMLURL())
	logRecord.Attributes().PutInt("github.workflow.run.id", event.GetWorkflowRun().GetID())
	logRecord.Attributes().PutStr("github.workflow.run.name", event.GetWorkflowRun().GetName())
	logRecord.Attributes().PutStr("github.workflow.run.path", event.GetWorkflow().GetPath())

	logRecord.Attributes().PutInt("github.workflow.run.run_attempt", int64(event.GetWorkflowRun().GetRunAttempt()))
	logRecord.Attributes().PutStr("github.workflow.run.run_started_at", event.GetWorkflowRun().RunStartedAt.Format(time.RFC3339))
	logRecord.Attributes().PutStr("github.workflow.run.status", event.GetWorkflowRun().GetStatus())
	logRecord.Attributes().PutStr("github.workflow.run.sender.login", event.GetSender().GetLogin())
	logRecord.Attributes().PutStr("github.workflow.run.triggering_actor.login", event.GetWorkflowRun().GetTriggeringActor().GetLogin())
	logRecord.Attributes().PutStr("github.workflow.run.updated_at", event.GetWorkflowRun().GetUpdatedAt().Format(time.RFC3339))

	//TODO: This may not be correct?
	logRecord.SetTimestamp(pcommon.NewTimestampFromTime(time.Now()))

	return log
}
