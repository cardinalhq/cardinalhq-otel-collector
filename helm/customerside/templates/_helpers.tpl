{{/*
Expand the name of the chart.
*/}}
{{- define "customerside.name" -}}
{{- default .Chart.Name .Values.nameOverride | trunc 63 | trimSuffix "-" }}
{{- end }}

{{/*
Add Namesapce if set
*/}}
{{- define "customerside.namespace" -}}
{{- if ne .Release.Namespace "default" }}
namespace: {{ .Release.Namespace }}
{{- end }}
{{- end }}

{{/*
Create a default fully qualified app name.
We truncate at 63 chars because some Kubernetes name fields are limited to this (by the DNS naming spec).
If release name contains chart name it will be used as a full name.
*/}}
{{- define "customerside.fullname" -}}
{{- if .Values.fullnameOverride }}
{{- .Values.fullnameOverride | trunc 63 | trimSuffix "-" }}
{{- else }}
{{- $name := default .Chart.Name .Values.nameOverride }}
{{- if contains $name .Release.Name }}
{{- .Release.Name | trunc 63 | trimSuffix "-" }}
{{- else }}
{{- printf "%s-%s" .Release.Name $name | trunc 63 | trimSuffix "-" }}
{{- end }}
{{- end }}
{{- end }}

{{/*
Create chart name and version as used by the chart label.
*/}}
{{- define "customerside.chart" -}}
{{- printf "%s-%s" .Chart.Name .Chart.Version | replace "+" "_" | trunc 63 | trimSuffix "-" }}
{{- end }}

{{/*
Common labels
*/}}
{{- define "customerside.labels" -}}
helm.sh/chart: {{ include "customerside.chart" . }}
{{ include "customerside.selectorLabels" . }}
{{- if .Chart.AppVersion }}
app.kubernetes.io/version: {{ .Chart.AppVersion | quote }}
{{- end }}
app.kubernetes.io/managed-by: {{ .Release.Service }}
{{- end }}

{{/*
Selector labels
*/}}
{{- define "customerside.selectorLabels" -}}
app.kubernetes.io/name: {{ include "customerside.name" . }}
app.kubernetes.io/instance: {{ .Release.Name }}
{{- end }}

{{/*
Create annotations for serviceAccount.
*/}}
{{- define "customerside.serviceAccount.annotations" }}
{{- if .Values.annotations }}
{{- if or (.Values.annotations.serviceAccount) (.Values.annotations.common) }}
annotations:
{{- if .Values.annotations.serviceAccount }}
{{- toYaml .Values.annotations.serviceAccount | nindent 2 }}
{{- end }}
{{- if .Values.annotations.common }}
{{- toYaml .Values.annotations.common | nindent 2 }}
{{- end }}
{{- end }}
{{- end }}
{{- end }}

{{/*
Create annotations for deployments.
*/}}
{{- define "customerside.deployment.annotations" }}
{{- if .Values.annotations }}
{{- if or (.Values.annotations.deployment) (.Values.annotations.common) }}
annotations:
{{- if .Values.annotations.deployment }}
{{- toYaml .Values.annotations.deployment | nindent 2 }}
{{- end }}
{{- if .Values.annotations.common }}
{{- toYaml .Values.annotations.common | nindent 2 }}
{{- end }}
{{- end }}
{{- end }}
{{- end }}

{{/*
Create annotations for pods.
*/}}
{{- define "customerside.pod.annotations" }}
{{- if .Values.annotations }}
{{- if or (.Values.annotations.pod) (.Values.annotations.common) }}
annotations:
{{- if .Values.annotations.pod }}
{{- toYaml .Values.annotations.pod | nindent 2 }}
{{- end }}
{{- if .Values.annotations.common }}
{{- toYaml .Values.annotations.common | nindent 2 }}
{{- end }}
{{- end }}
{{- end }}
{{- end }}

{{/*
Create annotations for services.
*/}}
{{- define "customerside.service.annotations" }}
{{- if .Values.annotations }}
{{- if or (.Values.annotations.service) (.Values.annotations.common) }}
annotations:
{{- if .Values.annotations.service }}
{{- toYaml .Values.annotations.service | nindent 2 }}
{{- end }}
{{- if .Values.annotations.common }}
{{- toYaml .Values.annotations.common | nindent 2 }}
{{- end }}
{{- end }}
{{- end }}
{{- end }}

{{/*
Create annotations for configMap.
*/}}
{{- define "customerside.configMap.annotations" }}
{{- if .Values.annotations }}
{{- if or (.Values.annotations.configMap) (.Values.annotations.common) }}
annotations:
{{- if .Values.annotations.configMap }}
{{- toYaml .Values.annotations.configMap | nindent 2 }}
{{- end }}
{{- if .Values.annotations.common }}
{{- toYaml .Values.annotations.common | nindent 2 }}
{{- end }}
{{- end }}
{{- end }}
{{- end }}

{{/*
Create annotations for secret.
*/}}
{{- define "customerside.secret.annotations" }}
{{- if .Values.annotations }}
{{- if or (.Values.annotations.secret) (.Values.annotations.common) }}
annotations:
{{- if .Values.annotations.secret }}
{{- toYaml .Values.annotations.secret | nindent 2 }}
{{- end }}
{{- if .Values.annotations.common }}
{{- toYaml .Values.annotations.common | nindent 2 }}
{{- end }}
{{- end }}
{{- end }}
{{- end }}

{{/*
Create annotations for role.
*/}}
{{- define "customerside.role.annotations" }}
{{- if .Values.annotations }}
{{- if or (.Values.annotations.role) (.Values.annotations.common) }}
annotations:
{{- if .Values.annotations.role }}
{{- toYaml .Values.annotations.role | nindent 2 }}
{{- end }}
{{- if .Values.annotations.common }}
{{- toYaml .Values.annotations.common | nindent 2 }}
{{- end }}
{{- end }}
{{- end }}
{{- end }}

{{/*
Create annotations for roleBinding.
*/}}
{{- define "customerside.roleBinding.annotations" }}
{{- if .Values.annotations }}
{{- if or (.Values.annotations.roleBinding) (.Values.annotations.common) }}
annotations:
{{- if .Values.annotations.roleBinding }}
{{- toYaml .Values.annotations.roleBinding | nindent 2 }}
{{- end }}
{{- if .Values.annotations.common }}
{{- toYaml .Values.annotations.common | nindent 2 }}
{{- end }}
{{- end }}
{{- end }}
{{- end }}
