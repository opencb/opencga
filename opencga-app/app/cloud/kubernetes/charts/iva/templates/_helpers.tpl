{{/* vim: set filetype=mustache: */}}
{{/*
Expand the name of the chart.
*/}}
{{- define "iva.name" -}}
{{- default .Chart.Name .Values.nameOverride | trunc 63 | trimSuffix "-" -}}
{{- end -}}

{{/*
Create a default fully qualified app name.
We truncate at 63 chars because some Kubernetes name fields are limited to this (by the DNS naming spec).
If release name contains chart name it will be used as a full name.
*/}}
{{- define "iva.fullname" -}}
{{- if .Values.fullnameOverride -}}
{{- .Values.fullnameOverride | trunc 63 | trimSuffix "-" -}}
{{- else -}}
{{- $name := default .Chart.Name .Values.nameOverride -}}
{{- if contains $name .Release.Name -}}
{{- .Release.Name | trunc 63 | trimSuffix "-" -}}
{{- else -}}
{{- printf "%s-%s" .Release.Name $name | trunc 63 | trimSuffix "-" -}}
{{- end -}}
{{- end -}}
{{- end -}}


{{/*
Create chart name and version as used by the chart label.
*/}}
{{- define "iva.chart" -}}
{{- printf "%s-%s" .Chart.Name .Chart.Version | replace "+" "_" | trunc 63 | trimSuffix "-" -}}
{{- end -}}

{{/*
Common labels
See https://helm.sh/docs/chart_best_practices/labels/#standard-labels
*/}}
{{- define "iva.labels" -}}
helm.sh/chart: {{ include "iva.chart" . }}
{{ include "iva.selectorLabels" . }}
{{- if .Chart.AppVersion }}
app.kubernetes.io/version: {{ .Chart.AppVersion | quote }}
{{- end }}
app.kubernetes.io/managed-by: {{ .Release.Service }}
{{- end -}}

{{/*
Selector labels
*/}}
{{- define "iva.selectorLabels" -}}
app.kubernetes.io/name: {{ include "iva.name" . }}
app.kubernetes.io/instance: {{ .Release.Name }}
{{- end -}}


{{- define "pvIvaconf" -}}
pv-{{ include "iva.fullname" . }}-ivaconf
{{- end -}}


{{- define "pvcStorageClassName" -}}
{{- if .Values.azureStorageAccount.enabled -}}
azurefile
{{- end -}}
{{- end -}}

{{- define "pvcIvaconf" -}}
pvc-{{ include "iva.fullname" . }}-ivaconf
{{- end -}}

