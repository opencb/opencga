package org.opencb.opencga.core.analysis.result;

import com.fasterxml.jackson.annotation.JsonProperty;
import org.opencb.commons.datastore.core.ObjectMap;
import org.opencb.opencga.core.analysis.OpenCgaAnalysisExecutor;
import org.opencb.opencga.core.annotations.AnalysisExecutor;

public class ExecutorInfo {

    private String id;
    @JsonProperty("class")
    private Class<? extends OpenCgaAnalysisExecutor> clazz;
    private ObjectMap params;
    private AnalysisExecutor.Source source;
    private AnalysisExecutor.Framework framework;

    public ExecutorInfo() {
    }

    public ExecutorInfo(String id,
                        Class<? extends OpenCgaAnalysisExecutor> clazz,
                        ObjectMap params,
                        AnalysisExecutor.Source source,
                        AnalysisExecutor.Framework framework) {
        this.id = id;
        this.clazz = clazz;
        this.params = params;
        this.source = source;
        this.framework = framework;
    }

    public String getId() {
        return id;
    }

    public ExecutorInfo setId(String id) {
        this.id = id;
        return this;
    }

    public Class<? extends OpenCgaAnalysisExecutor> getClazz() {
        return clazz;
    }

    public ExecutorInfo setClazz(Class<? extends OpenCgaAnalysisExecutor> clazz) {
        this.clazz = clazz;
        return this;
    }

    public ObjectMap getParams() {
        return params;
    }

    public ExecutorInfo setParams(ObjectMap params) {
        this.params = params;
        return this;
    }

    public AnalysisExecutor.Source getSource() {
        return source;
    }

    public ExecutorInfo setSource(AnalysisExecutor.Source source) {
        this.source = source;
        return this;
    }

    public AnalysisExecutor.Framework getFramework() {
        return framework;
    }

    public ExecutorInfo setFramework(AnalysisExecutor.Framework framework) {
        this.framework = framework;
        return this;
    }
}
