
{{ if eq .Chart.AppVersion "REPLACEME_IVA_VERSION" }}
    {{ fail "Wrong Chart.AppVersion. Attempting to execute HELM from opencga/opencga-app/app/cloud/kubernetes"  }}
{{ end }}


