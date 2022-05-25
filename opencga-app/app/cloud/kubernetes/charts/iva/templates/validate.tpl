
{{ if not .Values.skipChartAppVersionCheck }}
    {{ if eq .Chart.AppVersion "REPLACEME_IVA_VERSION" }}
        {{ fail "Wrong Chart.AppVersion. Attempting to execute HELM from opencga/opencga-app/app/cloud/kubernetes"  }}
    {{ end }}
{{ end }}

{{ if not (empty .Values.iva.ingress.paths) }}
    {{ fail "Wrong Values.iva.ingress. Using an old iva.ingress.paths . Migrate paths to iva.ingress.sites "  }}
{{ end }}


