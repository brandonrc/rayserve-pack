{{/*
Namespace for a group: cm-<group>.
*/}}
{{- define "group-ray.namespace" -}}
{{- printf "cm-%s" .name }}
{{- end }}

{{/*
RayCluster (and NebariApp) name for a group: <group>-ray.
*/}}
{{- define "group-ray.clusterName" -}}
{{- printf "%s-ray" .name }}
{{- end }}

{{/*
Hostname for a group's dashboard: explicit per group, else
<group>-ray.<hostnameSuffix>.
*/}}
{{- define "group-ray.hostname" -}}
{{- if .group.hostname }}
{{- .group.hostname }}
{{- else if .root.Values.hostnameSuffix }}
{{- printf "%s-ray.%s" .group.name .root.Values.hostnameSuffix }}
{{- else }}
{{- fail (printf "group %q needs a hostname (or set hostnameSuffix)" .group.name) }}
{{- end }}
{{- end }}

{{/*
Common labels, including the group so everything is greppable per tenant.
*/}}
{{- define "group-ray.labels" -}}
helm.sh/chart: {{ printf "%s-%s" .root.Chart.Name .root.Chart.Version }}
app.kubernetes.io/managed-by: {{ .root.Release.Service }}
app.kubernetes.io/part-of: nebari-group-ray
checkmaite.io/group: {{ .group.name }}
{{- end }}

{{/*
Merge a per-group override map over a chart-level default map.
Usage: include "group-ray.merged" (dict "default" .Values.worker "override" $group.worker)
*/}}
{{- define "group-ray.merged" -}}
{{- $out := deepCopy (.default | default dict) -}}
{{- range $k, $v := (.override | default dict) -}}
{{- $_ := set $out $k $v -}}
{{- end -}}
{{- toYaml $out -}}
{{- end }}
