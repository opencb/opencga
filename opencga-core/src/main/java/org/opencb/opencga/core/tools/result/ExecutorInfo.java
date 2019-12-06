package org.opencb.opencga.core.tools.result;

import com.fasterxml.jackson.annotation.JsonProperty;
import org.opencb.commons.datastore.core.ObjectMap;
import org.opencb.opencga.core.tools.OpenCgaToolExecutor;
import org.opencb.opencga.core.annotations.ToolExecutor;

public class ExecutorInfo {

    private String id;
    @JsonProperty("class")
    private Class<? extends OpenCgaToolExecutor> clazz;
    private ObjectMap params;
    private ToolExecutor.Source source;
    private ToolExecutor.Framework framework;

    public ExecutorInfo() {
    }

    public ExecutorInfo(String id,
                        Class<? extends OpenCgaToolExecutor> clazz,
                        ObjectMap params,
                        ToolExecutor.Source source,
                        ToolExecutor.Framework framework) {
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

    public Class<? extends OpenCgaToolExecutor> getClazz() {
        return clazz;
    }

    public ExecutorInfo setClazz(Class<? extends OpenCgaToolExecutor> clazz) {
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

    public ToolExecutor.Source getSource() {
        return source;
    }

    public ExecutorInfo setSource(ToolExecutor.Source source) {
        this.source = source;
        return this;
    }

    public ToolExecutor.Framework getFramework() {
        return framework;
    }

    public ExecutorInfo setFramework(ToolExecutor.Framework framework) {
        this.framework = framework;
        return this;
    }
}
