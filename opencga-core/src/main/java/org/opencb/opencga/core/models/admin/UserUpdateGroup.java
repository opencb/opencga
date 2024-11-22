package org.opencb.opencga.core.models.admin;

import java.util.List;

public class UserUpdateGroup {
    private List<String> studyIds;
    private List<String> groupIds;

    public UserUpdateGroup() {
    }

    public UserUpdateGroup(List<String> studyIds, List<String> groupIds) {
        this.studyIds = studyIds;
        this.groupIds = groupIds;
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("UserUpdateGroup{");
        sb.append("studyIds=").append(studyIds);
        sb.append(", groupIds=").append(groupIds);
        sb.append('}');
        return sb.toString();
    }

    public List<String> getStudyIds() {
        return studyIds;
    }

    public UserUpdateGroup setStudyIds(List<String> studyIds) {
        this.studyIds = studyIds;
        return this;
    }

    public List<String> getGroupIds() {
        return groupIds;
    }

    public UserUpdateGroup setGroupIds(List<String> groupIds) {
        this.groupIds = groupIds;
        return this;
    }
}
