{{/*
Standard name/label helpers.
*/}}
{{- define "inference.name" -}}
{{- default .Chart.Name .Values.nameOverride | trunc 63 | trimSuffix "-" -}}
{{- end -}}

{{- define "inference.labels" -}}
helm.sh/chart: {{ printf "%s-%s" .Chart.Name .Chart.Version | replace "+" "_" | trunc 63 | trimSuffix "-" }}
app.kubernetes.io/name: {{ include "inference.name" . }}
app.kubernetes.io/instance: {{ .Release.Name }}
app.kubernetes.io/version: {{ .Chart.AppVersion | quote }}
app.kubernetes.io/managed-by: {{ .Release.Service }}
app.kubernetes.io/part-of: inference-pipeline
{{- end -}}

{{/*
The MinIO subchart's own fullname logic, reproduced so we can build the S3
endpoint URL without hardcoding a release name. Kept byte-for-byte compatible
with charts/minio/templates/_helper.tpl -- if the subchart is upgraded and its
naming changes, this must change with it.
*/}}
{{- define "inference.minioFullname" -}}
{{- if .Values.minio.fullnameOverride -}}
{{- .Values.minio.fullnameOverride | trunc 63 | trimSuffix "-" -}}
{{- else -}}
{{- $name := default "minio" .Values.minio.nameOverride -}}
{{- if contains $name .Release.Name -}}
{{- .Release.Name | trunc 63 | trimSuffix "-" -}}
{{- else -}}
{{- printf "%s-%s" .Release.Name $name | trunc 63 | trimSuffix "-" -}}
{{- end -}}
{{- end -}}
{{- end -}}

{{/*
In-cluster S3 endpoint for MinIO. Every consumer -- the worker containers, the
Airflow connection -- resolves the endpoint through this, so
there is exactly one definition of where MinIO lives.
*/}}
{{- define "inference.minioEndpoint" -}}
http://{{ include "inference.minioFullname" . }}.{{ .Release.Namespace }}.svc.cluster.local:{{ .Values.minio.service.port }}
{{- end -}}
