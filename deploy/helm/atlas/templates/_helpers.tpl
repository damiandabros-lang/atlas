{{/*
Expand the name of the chart.
*/}}
{{- define "atlas.name" -}}
{{- default .Chart.Name .Values.nameOverride | trunc 63 | trimSuffix "-" }}
{{- end }}

{{/*
Fully qualified app name (release-name-chart-name, deduplicated).
*/}}
{{- define "atlas.fullname" -}}
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

{{- define "atlas.chart" -}}
{{- printf "%s-%s" .Chart.Name .Chart.Version | replace "+" "_" | trunc 63 | trimSuffix "-" }}
{{- end }}

{{/*
Common labels.
*/}}
{{- define "atlas.labels" -}}
helm.sh/chart: {{ include "atlas.chart" . }}
{{ include "atlas.selectorLabels" . }}
{{- if .Chart.AppVersion }}
app.kubernetes.io/version: {{ .Chart.AppVersion | quote }}
{{- end }}
app.kubernetes.io/managed-by: {{ .Release.Service }}
app.kubernetes.io/part-of: atlas
{{- end }}

{{/*
Selector labels (stable across upgrades — never add version here).
*/}}
{{- define "atlas.selectorLabels" -}}
app.kubernetes.io/name: {{ include "atlas.name" . }}
app.kubernetes.io/instance: {{ .Release.Name }}
{{- end }}

{{/*
Component-specific selector labels: usage {{ include "atlas.componentSelectorLabels" (dict "root" . "component" "api") }}
*/}}
{{- define "atlas.componentSelectorLabels" -}}
{{ include "atlas.selectorLabels" .root }}
app.kubernetes.io/component: {{ .component }}
{{- end }}

{{- define "atlas.serviceAccountName" -}}
{{- if .Values.serviceAccount.create }}
{{- default (include "atlas.fullname" .) .Values.serviceAccount.name }}
{{- else }}
{{- default "default" .Values.serviceAccount.name }}
{{- end }}
{{- end }}

{{- define "atlas.image" -}}
{{- $tag := default .Chart.AppVersion .Values.image.tag -}}
{{- printf "%s:%s" .Values.image.repository $tag }}
{{- end }}

{{/*
Name of the Secret that holds database-url / api-keys / device credentials.
*/}}
{{- define "atlas.secretName" -}}
{{- if .Values.secrets.existingSecret }}
{{- .Values.secrets.existingSecret }}
{{- else }}
{{- printf "%s-secrets" (include "atlas.fullname" .) }}
{{- end }}
{{- end }}

{{- define "atlas.configMapName" -}}
{{- printf "%s-config" (include "atlas.fullname" .) }}
{{- end }}

{{/*
Shared env wiring for every ATLAS container (ConfigMap + Secret).
*/}}
{{- define "atlas.envFrom" -}}
- configMapRef:
    name: {{ include "atlas.configMapName" . }}
- secretRef:
    name: {{ include "atlas.secretName" . }}
{{- end }}

{{/*
Writable scratch volumes required because the root filesystem is read-only.
*/}}
{{- define "atlas.scratchVolumes" -}}
- name: tmp
  emptyDir:
    medium: Memory
    sizeLimit: 64Mi
{{- end }}

{{- define "atlas.scratchVolumeMounts" -}}
- name: tmp
  mountPath: /tmp
{{- end }}
