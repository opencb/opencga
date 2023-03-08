package org.opencb.opencga.core.models.family;

import org.opencb.commons.datastore.core.ObjectMap;

public class PedigreeGraph {
    String base64;
    ObjectMap json;

    public PedigreeGraph() {
    }

    public PedigreeGraph(String base64, ObjectMap json) {
        this.base64 = base64;
        this.json = json;
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("PedigreeGraph{");
        sb.append("base64='").append(base64).append('\'');
        sb.append(", json=").append(json);
        sb.append('}');
        return sb.toString();
    }

    public String getBase64() {
        return base64;
    }

    public PedigreeGraph setBase64(String base64) {
        this.base64 = base64;
        return this;
    }

    public ObjectMap getJson() {
        return json;
    }

    public PedigreeGraph setJson(ObjectMap json) {
        this.json = json;
        return this;
    }
}
