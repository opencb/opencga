{{/* vim: set filetype=mustache: */}}
{{/*
Expand the name of the chart.
*/}}
{{- define "opencga.name" -}}
{{- default .Chart.Name .Values.nameOverride | trunc 63 | trimSuffix "-" -}}
{{- end -}}

{{/*
Create a default fully qualified app name.
We truncate at 63 chars because some Kubernetes name fields are limited to this (by the DNS naming spec).
If release name contains chart name it will be used as a full name.
*/}}
{{- define "opencga.fullname" -}}
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
{{- define "opencga.chart" -}}
{{- printf "%s-%s" .Chart.Name .Chart.Version | replace "+" "_" | trunc 63 | trimSuffix "-" -}}
{{- end -}}

{{/*
Common labels
*/}}
{{- define "opencga.labels" -}}
helm.sh/chart: {{ include "opencga.chart" . }}
{{ include "opencga.selectorLabels" . }}
{{- if .Chart.AppVersion }}
app.kubernetes.io/version: {{ .Chart.AppVersion | quote }}
{{- end }}
app.kubernetes.io/managed-by: {{ .Release.Service }}
{{- end -}}

{{/*
Selector labels
*/}}
{{- define "opencga.selectorLabels" -}}
app.kubernetes.io/name: {{ include "opencga.name" . }}
app.kubernetes.io/instance: {{ .Release.Name }}
{{- end -}}

{{/*
Create the name of the service account to use
*/}}
{{- define "opencga.serviceAccountName" -}}
{{- if .Values.serviceAccount.create -}}
    {{ default (include "opencga.fullname" .) .Values.serviceAccount.name }}
{{- else -}}
    {{ default "default" .Values.serviceAccount.name }}
{{- end -}}
{{- end -}}


{{- define "opencga.defaultConf.name" -}}
{{ include "opencga.fullname" . }}-default-conf
{{- end -}}

{{- define "opencga.secretName" -}}
{{- if .Values.opencga.secretName -}}
{{ .Values.opencga.secretName }}
{{- else -}}
{{ include "opencga.fullname" . }}-secret
{{- end -}}
{{- end -}}

{{- define "clusterRoleBinding.name" -}}
{{ include "opencga.fullname" . }}-fabric8-rbac
{{- end -}}


{{- define "pvConf" -}}
pv-{{ include "opencga.fullname" . }}-conf
{{- end -}}
{{- define "pvSessions" -}}
pv-{{ include "opencga.fullname" . }}-sessions
{{- end -}}
{{- define "pvVariants" -}}
pv-{{ include "opencga.fullname" . }}-variants
{{- end -}}
{{- define "pvAnalysisconf" -}}
pv-{{ include "opencga.fullname" . }}-analysisconf
{{- end -}}


{{- define "pvcStorageClassName" -}}
{{- if .Values.azureStorageAccount.enabled -}}
azurefile
{{- end -}}
{{- end -}}

{{- define "pvcConf" -}}
pvc-{{ include "opencga.fullname" . }}-conf
{{- end -}}
{{- define "pvcSessions" -}}
pvc-{{ include "opencga.fullname" . }}-sessions
{{- end -}}
{{- define "pvcVariants" -}}
pvc-{{ include "opencga.fullname" . }}-variants
{{- end -}}
{{- define "pvcAnalysisconf" -}}
pvc-{{ include "opencga.fullname" . }}-analysisconf
{{- end -}}


{{- define "additionalVolumes" -}}
{{- if .Values.volumes -}}
{{- range .Values.volumes }}
- name: {{ .name }}
  {{- .source | toYaml | nindent 2 -}}
{{- end }}
{{- end -}}
{{- end -}}


{{- define "additionalVolumeMounts" -}}
{{- if .Values.volumes -}}
{{- range .Values.volumes }}
- name: {{ .name }}
  mountPath: {{ .mountPath }}
{{- end }}
{{- end -}}
{{- end -}}



{{- define "mongodb.replicaSet" -}}
{{- if .Values.mongodb.deploy.enabled -}}
    {{- .Values.mongodb.deploy.name }}
{{- else -}}
    {{- .Values.mongodb.replicaSet }}
{{- end -}}
{{- end -}}

{{- define "mongodb.hosts" -}}
{{- if .Values.mongodb.deploy.enabled -}}
    {{- $name := .Values.mongodb.deploy.name -}}
    {{- $namespace := .Release.Namespace -}}
    {{- range $i := until ( int .Values.mongodb.deploy.replicas ) }}
        {{- if ne $i 0 -}}
          ,
        {{- end -}}
        {{- cat $name "-" $i "." $name "-svc." $namespace ".svc.cluster.local:27017" | replace " " "" -}}
    {{- end -}}
{{- else -}}
    {{- .Values.mongodb.external.hosts }}
{{- end -}}
{{- end -}}


{{- define "solrHosts" -}}
{{- if .Values.solr.deploy.enabled -}}
    {{- $name := .Values.solr.deploy.name -}}
    {{- $namespace := .Release.Namespace -}}
    {{- range $i := until ( int .Values.solr.deploy.zookeeper.replicas ) }}
        {{- if ne $i 0 -}}
          ,
        {{- end -}}
        {{- cat $name "-solrcloud-zookeeper-" $i "." $name "-solrcloud-zookeeper-headless." $namespace ":2181" | replace " " "" -}}
    {{- end -}}
{{- else -}}
    {{- .Values.solr.external.hosts }}
{{- end -}}
{{- end -}}
