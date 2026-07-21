{{/*
Expand the name of the chart.
*/}}
{{- define "zelox.name" -}}
{{- default .Chart.Name .Values.nameOverride | trunc 63 | trimSuffix "-" }}
{{- end }}

{{/*
Create a default fully qualified app name.
*/}}
{{- define "zelox.fullname" -}}
{{- if .Values.fullnameOverride }}
{{- .Values.fullnameOverride | trunc 63 | trimSuffix "-" }}
{{- else }}
{{- .Release.Name | trunc 63 | trimSuffix "-" }}
{{- end }}
{{- end }}

{{/*
Common labels
*/}}
{{- define "zelox.labels" -}}
helm.sh/chart: {{ printf "%s-%s" .Chart.Name .Chart.Version | replace "+" "_" | trunc 63 | trimSuffix "-" }}
{{ include "zelox.selectorLabels" . }}
app.kubernetes.io/managed-by: {{ .Release.Service }}
{{- with .Values.commonLabels }}
{{ toYaml . }}
{{- end }}
{{- end }}

{{/*
Selector labels
*/}}
{{- define "zelox.selectorLabels" -}}
app.kubernetes.io/name: {{ include "zelox.name" . }}
app.kubernetes.io/instance: {{ .Release.Name }}
{{- end }}

{{/*
Server selector labels
*/}}
{{- define "zelox.serverSelectorLabels" -}}
{{ include "zelox.selectorLabels" . }}
app.kubernetes.io/component: spark-server
{{- end }}

{{/*
Worker selector labels
*/}}
{{- define "zelox.workerSelectorLabels" -}}
{{ include "zelox.selectorLabels" . }}
app.kubernetes.io/component: worker
{{- end }}

{{/*
Image reference
*/}}
{{- define "zelox.image" -}}
{{ .Values.image.repository }}:{{ .Values.image.tag | default .Chart.AppVersion }}
{{- end }}

{{/*
Scratch volume definition
*/}}
{{- define "zelox.scratchVolume" -}}
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
