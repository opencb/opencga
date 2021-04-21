package org.opencb.opencga.client.template.config;

import org.opencb.opencga.core.models.project.ProjectCreateParams;

import java.util.List;

public class TemplateProject extends ProjectCreateParams {

    private List<TemplateStudy> studies;

    public List<TemplateStudy> getStudies() {
        return studies;
    }

    public TemplateProject setStudies(List<TemplateStudy> studies) {
        this.studies = studies;
        return this;
    }
}
