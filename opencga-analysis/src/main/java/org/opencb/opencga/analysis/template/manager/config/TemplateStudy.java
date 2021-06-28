package org.opencb.opencga.analysis.template.manager.config;

import org.opencb.opencga.core.models.study.GroupCreateParams;
import org.opencb.opencga.core.models.study.StudyAclEntry;
import org.opencb.opencga.core.models.study.StudyUpdateParams;
import org.opencb.opencga.core.models.study.VariableSetCreateParams;

import java.util.List;

public class TemplateStudy extends StudyUpdateParams {

    private String id;
    private List<GroupCreateParams> groups;
    private List<VariableSetCreateParams> variableSets;

    private List<StudyAclEntry> acl;

    public String getId() {
        return id;
    }

    public TemplateStudy setId(String id) {
        this.id = id;
        return this;
    }

    public List<GroupCreateParams> getGroups() {
        return groups;
    }

    public TemplateStudy setGroups(List<GroupCreateParams> groups) {
        this.groups = groups;
        return this;
    }

    public List<VariableSetCreateParams> getVariableSets() {
        return variableSets;
    }

    public TemplateStudy setVariableSets(List<VariableSetCreateParams> variableSets) {
        this.variableSets = variableSets;
        return this;
    }

    public List<StudyAclEntry> getAcl() {
        return acl;
    }

    public TemplateStudy setAcl(List<StudyAclEntry> acl) {
        this.acl = acl;
        return this;
    }
}
