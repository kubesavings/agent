{{/*
Expand the name of the chart.
*/}}
{{- define "kubesavings-agent.name" -}}
kubesavings-agent
{{- end }}

{{/*
Create a fully qualified name: <release>-kubesavings-agent, max 63 chars.
*/}}
{{- define "kubesavings-agent.fullname" -}}
{{- printf "%s-kubesavings-agent" .Release.Name | trunc 63 | trimSuffix "-" }}
{{- end }}

{{/*
Common labels.
*/}}
{{- define "kubesavings-agent.labels" -}}
helm.sh/chart: {{ printf "%s-%s" .Chart.Name .Chart.Version | quote }}
app.kubernetes.io/name: {{ include "kubesavings-agent.name" . }}
app.kubernetes.io/instance: {{ .Release.Name }}
app.kubernetes.io/version: {{ .Chart.AppVersion | quote }}
app.kubernetes.io/managed-by: {{ .Release.Service }}
{{- end }}

{{/*
Selector labels (stable subset used by podSelector / matchLabels).
*/}}
{{- define "kubesavings-agent.selectorLabels" -}}
app.kubernetes.io/name: {{ include "kubesavings-agent.name" . }}
app.kubernetes.io/instance: {{ .Release.Name }}
{{- end }}

{{/*
Name of the Secret that holds api-key and cluster-id.
Uses existingSecret when provided, otherwise the chart-managed secret.
*/}}
{{- define "kubesavings-agent.secretName" -}}
{{- if .Values.agent.existingSecret -}}
{{ .Values.agent.existingSecret }}
{{- else -}}
{{ include "kubesavings-agent.fullname" . }}
{{- end -}}
{{- end }}

{{/*
ServiceAccount name.
*/}}
{{- define "kubesavings-agent.serviceAccountName" -}}
{{- if .Values.serviceAccount.create -}}
{{ .Values.serviceAccount.name | default (include "kubesavings-agent.fullname" .) }}
{{- else -}}
{{ .Values.serviceAccount.name }}
{{- end -}}
{{- end }}
