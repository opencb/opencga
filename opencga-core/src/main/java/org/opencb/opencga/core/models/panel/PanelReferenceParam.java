package org.opencb.opencga.core.models.panel;

public class PanelReferenceParam {

    private String id;

    public PanelReferenceParam() {
    }

    public PanelReferenceParam(String id) {
        this.id = id;
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("PanelReferenceParam{");
        sb.append("id='").append(id).append('\'');
        sb.append('}');
        return sb.toString();
    }

    public String getId() {
        return id;
    }

    public PanelReferenceParam setId(String id) {
        this.id = id;
        return this;
    }
}
