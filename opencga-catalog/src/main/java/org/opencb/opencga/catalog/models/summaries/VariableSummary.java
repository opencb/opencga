package org.opencb.opencga.catalog.models.summaries;

import java.util.Collections;
import java.util.List;

/**
 * Created by pfurio on 12/08/16.
 */
public class VariableSummary {

    private String name;
    private List<FeatureCount> annotations;

    public VariableSummary() {
        this("", Collections.emptyList());
    }

    public VariableSummary(String name, List<FeatureCount> annotations) {
        this.name = name;
        this.annotations = annotations;
    }

    public String getName() {
        return name;
    }

    public VariableSummary setName(String name) {
        this.name = name;
        return this;
    }

    public List<FeatureCount> getAnnotations() {
        return annotations;
    }

    public VariableSummary setAnnotations(List<FeatureCount> annotations) {
        this.annotations = annotations;
        return this;
    }
}
