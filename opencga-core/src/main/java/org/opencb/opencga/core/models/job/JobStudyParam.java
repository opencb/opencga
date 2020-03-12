package org.opencb.opencga.core.models.job;

import java.util.LinkedList;
import java.util.List;

public class JobStudyParam {

    private String id;
    private List<String> others;

    public JobStudyParam() {
    }

    public JobStudyParam(String id) {
        this(id, new LinkedList<>());
    }

    public JobStudyParam(String id, List<String> others) {
        this.id = id;
        this.others = others;
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("JobRelatedStudies{");
        sb.append("id='").append(id).append('\'');
        sb.append(", others=").append(others);
        sb.append('}');
        return sb.toString();
    }

    public String getId() {
        return id;
    }

    public JobStudyParam setId(String id) {
        this.id = id;
        return this;
    }

    public List<String> getOthers() {
        return others;
    }

    public JobStudyParam setOthers(List<String> others) {
        this.others = others;
        return this;
    }
}
