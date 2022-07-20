package org.opencb.opencga.core.models.clinical;

import org.opencb.opencga.core.models.family.Family;

import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

import org.opencb.commons.annotations.DataField;
import org.opencb.opencga.core.api.ParamConstants;

public class FamilyParam {
    @DataField(description = ParamConstants.FAMILY_PARAM_ID_DESCRIPTION)
    private String id;
    @DataField(description = ParamConstants.FAMILY_PARAM_MEMBERS_DESCRIPTION)
    private List<ProbandParam> members;

    public FamilyParam() {
    }

    public FamilyParam(String id, List<ProbandParam> members) {
        this.id = id;
        this.members = members;
    }

    public static FamilyParam of(Family family) {
        return new FamilyParam(family.getId(),
                family.getMembers() != null
                        ? family.getMembers().stream().map(ProbandParam::of).collect(Collectors.toList())
                        : Collections.emptyList());
    }

    public Family toFamily() {
        return new Family()
                .setId(id)
                .setMembers(members != null
                        ? members.stream().map(ProbandParam::toIndividual).collect(Collectors.toList())
                        : Collections.emptyList());
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("FamilyParam{");
        sb.append("id='").append(id).append('\'');
        sb.append(", members=").append(members);
        sb.append('}');
        return sb.toString();
    }

    public String getId() {
        return id;
    }

    public FamilyParam setId(String id) {
        this.id = id;
        return this;
    }

    public List<ProbandParam> getMembers() {
        return members;
    }

    public FamilyParam setMembers(List<ProbandParam> members) {
        this.members = members;
        return this;
    }
}
