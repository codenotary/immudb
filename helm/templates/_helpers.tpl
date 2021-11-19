{{/*
Expand the name of the chart.
*/}}
{{- define "immudb.name" -}}
{{- default .Chart.Name .Values.nameOverride | trunc 63 | trimSuffix "-" }}
{{- end }}

{{/*
Create a default fully qualified app name.
We truncate at 63 chars because some Kubernetes name fields are limited to this (by the DNS naming spec).
If release name contains chart name it will be used as a full name.
*/}}
{{- define "immudb.fullname" -}}
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
{{- define "immudb.chart" -}}
{{- printf "%s-%s" .Chart.Name .Chart.Version | replace "+" "_" | trunc 63 | trimSuffix "-" }}
{{- end }}

{{/*
Common labels
*/}}
{{- define "immudb.labels" -}}
helm.sh/chart: {{ include "immudb.chart" . }}
{{ include "immudb.selectorLabels" . }}
{{- if .Chart.AppVersion }}
app.kubernetes.io/version: {{ .Chart.AppVersion | quote }}
{{- end }}
app.kubernetes.io/managed-by: {{ .Release.Service }}
{{- end }}

{{/*
Selector labels
*/}}
{{- define "immudb.selectorLabels" -}}
app.kubernetes.io/name: {{ include "immudb.name" . }}
app.kubernetes.io/instance: {{ .Release.Name }}
{{- end }}

{{/*
Create the name of the service account to use
*/}}
{{- define "immudb.serviceAccountName" -}}
{{- if .Values.serviceAccount.create }}
{{- default (include "immudb.fullname" .) .Values.serviceAccount.name }}
{{- else }}
{{- default "default" .Values.serviceAccount.name }}
{{- end }}
{{- end }}

{{- define "immudb.chart.ingressapiversion" -}}
{{- if semverCompare ">=1.19-0" $.Capabilities.KubeVersion.GitVersion }}
{{- printf "networking.k8s.io/v1" }}
{{- else if semverCompare ">=1.14-0" $.Capabilities.KubeVersion.GitVersion }}
{{- printf "networking.k8s.io/v1beta1" }}
{{- else }}
{{- printf "extensions/v1beta1" }}
{{- end }}
{{- end }}
