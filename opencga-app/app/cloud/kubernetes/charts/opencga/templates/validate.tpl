

{{ if not .Values.skipChartAppVersionCheck }}
    {{ if eq .Chart.AppVersion "REPLACEME_OPENCGA_VERSION" }}
        {{ fail "Wrong Chart.AppVersion. Attempting to execute HELM from opencga/opencga-app/app/cloud/kubernetes"  }}
    {{ end }}
{{ end }}


{{ if .Values.azureStorageAccount.enabled }}
    {{ if eq .Values.azureStorageAccount.name "FILL_ME" }}
        {{ fail "Empty Values.azureStorageAccount.name"  }}
    {{ end }}
    {{ if eq .Values.azureStorageAccount.key "FILL_ME" }}
        {{ fail "Empty Values.azureStorageAccount.key" }}
    {{ end }}
{{ end }}

{{ if eq .Values.mongodb.user "FILL_ME" }}
    {{ fail "Empty Values.mongodb.user"  }}
{{ end }}
{{ if eq .Values.mongodb.password "FILL_ME" }}
    {{ fail "Empty Values.mongodb.password"  }}
{{ end }}

{{ if not .Values.mongodb.deploy.enabled }}
    {{ if eq .Values.mongodb.external.hosts "FILL_ME" }}
        {{ fail "Empty Values.mongodb.external.hosts" }}
    {{ end }}
{{ end }}


{{ if not .Values.solr.deploy.enabled }}
    {{ if eq .Values.solr.external.hosts "FILL_ME" }}
        {{ fail "Empty Values.solr.external.hosts" }}
    {{ end }}
{{ end }}


{{ if eq .Values.analysis.execution.id "k8s" }}
    {{ if eq .Values.analysis.execution.options.k8s.masterNode "FILL_ME" }}
        {{ fail "Empty Values.analysis.execution.options.k8s.masterNode" }}
    {{ end }}
{{ end }}


{{ if eq .Values.hadoop.sshDns "FILL_ME" }}
    {{ fail "Empty Values.hadoop.sshDns" }}
{{ end }}
{{ if eq .Values.hadoop.sshUsername "FILL_ME" }}
    {{ fail "Empty Values.hadoop.sshUsername" }}
{{ end }}
{{ if eq .Values.hadoop.sshPassword "FILL_ME" }}
    {{ fail "Empty Values.hadoop.sshPassword" }}
{{ end }}


{{ if eq .Values.opencga.admin.password "FILL_ME" }}
    {{ fail "Empty Values.opencga.admin.password" }}
{{ end }}
