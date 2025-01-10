package com.zettagenomics.opencga.enterprise.catalog.utils;

import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.opencb.opencga.catalog.exceptions.CatalogException;
import org.opencb.opencga.catalog.exceptions.CatalogParameterException;
import org.opencb.opencga.catalog.managers.OrganizationManager;
import org.opencb.opencga.catalog.managers.StudyManager;
import org.opencb.opencga.core.models.JwtPayload;
import org.opencb.opencga.core.models.organizations.Organization;
import org.opencb.opencga.core.models.study.Group;
import org.opencb.opencga.core.models.study.Study;
import org.opencb.opencga.core.models.study.StudyInternal;

import java.util.*;

public class FederationUtils {

    public FederationUtils() {
    }

    public static String getFederationServerId(Map<String, String> queryParams, JwtPayload tokenPayload) throws CatalogException {
        if (CollectionUtils.isEmpty(tokenPayload.getFederations())) {
            throw new CatalogException("User does not belong to any federation.");
        }
        String study = queryParams.get("study");
        String project = queryParams.get("project");
        if (StringUtils.isEmpty(study) && StringUtils.isEmpty(project)) {
            throw new CatalogParameterException("Missing project or study from the query parameters");
        } else if (StringUtils.isNotEmpty(study)) {
            for (JwtPayload.Federation federation : tokenPayload.getFederations()) {
                if (federation.getStudyIds().contains(study)) {
                    return federation.getId();
                }
            }
        } else if (StringUtils.isNotEmpty(project)) {
            for (JwtPayload.Federation federation : tokenPayload.getFederations()) {
                if (federation.getProjectIds().contains(project)) {
                    return federation.getId();
                }
            }
        }
        throw new CatalogException("User does not belong to any federation that contains the project or study provided.");
    }

    public static void removeStudyFieldsForStorage(Organization organization, Study study) {
        study.setPermissionRules(Collections.emptyMap());
        study.setNotes(Collections.emptyList());
        study.setVariableSets(Collections.emptyList());
        study.setInternal(StudyInternal.init());
        study.setAttributes(Collections.emptyMap());
        study.setUri(null);

        // Set groups
        Set<String> users = new HashSet<>();
        users.add(organization.getOwner());
        users.addAll(organization.getAdmins());
        List<Group> groups = Arrays.asList(
                new Group(StudyManager.MEMBERS, new ArrayList<>(users)),
                new Group(StudyManager.ADMINS, new ArrayList<>(users))
        );
        study.setGroups(groups);
    }


}
