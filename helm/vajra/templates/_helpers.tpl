{{/*
Expand the name of the chart.
*/}}
{{- define "vajra.name" -}}
{{- default .Chart.Name .Values.nameOverride | trunc 63 | trimSuffix "-" }}
{{- end }}

{{/*
Create a default fully qualified app name.
*/}}
{{- define "vajra.fullname" -}}
{{- if .Values.fullnameOverride }}
{{- .Values.fullnameOverride | trunc 63 | trimSuffix "-" }}
{{- else }}
{{- .Release.Name | trunc 63 | trimSuffix "-" }}
{{- end }}
{{- end }}

{{/*
Common labels
*/}}
{{- define "vajra.labels" -}}
helm.sh/chart: {{ printf "%s-%s" .Chart.Name .Chart.Version | replace "+" "_" | trunc 63 | trimSuffix "-" }}
{{ include "vajra.selectorLabels" . }}
app.kubernetes.io/managed-by: {{ .Release.Service }}
{{- with .Values.commonLabels }}
{{ toYaml . }}
{{- end }}
{{- end }}

{{/*
Selector labels
*/}}
{{- define "vajra.selectorLabels" -}}
app.kubernetes.io/name: {{ include "vajra.name" . }}
app.kubernetes.io/instance: {{ .Release.Name }}
{{- end }}

{{/*
Server selector labels
*/}}
{{- define "vajra.serverSelectorLabels" -}}
{{ include "vajra.selectorLabels" . }}
app.kubernetes.io/component: spark-server
{{- end }}

{{/*
Worker selector labels
*/}}
{{- define "vajra.workerSelectorLabels" -}}
{{ include "vajra.selectorLabels" . }}
app.kubernetes.io/component: worker
{{- end }}

{{/*
Image reference
*/}}
{{- define "vajra.image" -}}
{{ .Values.image.repository }}:{{ .Values.image.tag | default .Chart.AppVersion }}
{{- end }}

{{/*
Scratch volume definition
*/}}
{{- define "vajra.scratchVolume" -}}
- name: scratch
  {{- if eq .Values.scratch.type "hostPath" }}
  hostPath:
    path: {{ .Values.scratch.hostPath }}
    type: DirectoryOrCreate
  {{- else }}
  emptyDir:
    sizeLimit: {{ .Values.scratch.sizeLimit }}
  {{- end }}
{{- end }}
