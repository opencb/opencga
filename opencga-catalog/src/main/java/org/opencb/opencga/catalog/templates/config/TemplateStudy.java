package org.opencb.opencga.catalog.templates.config;

import org.opencb.opencga.core.models.AclEntryList;
import org.opencb.opencga.core.models.study.*;

import java.util.List;

public class TemplateStudy extends StudyUpdateParams {

    private String id;
    private List<GroupCreateParams> groups;
    private List<VariableSetCreateParams> variableSets;

    private AclEntryList<StudyPermissions.Permissions> acl;

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

    public AclEntryList<StudyPermissions.Permissions> getAcl() {
        return acl;
    }

    public TemplateStudy setAcl(AclEntryList<StudyPermissions.Permissions> acl) {
        this.acl = acl;
        return this;
    }
}
