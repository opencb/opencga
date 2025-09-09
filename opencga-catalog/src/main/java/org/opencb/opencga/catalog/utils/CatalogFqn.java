package org.opencb.opencga.catalog.utils;

import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.opencb.opencga.core.models.JwtPayload;

import java.util.HashSet;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

public final class CatalogFqn {

    public static final String PROJECT_FQN_FORMAT = "projectId:studyId";
    public static final String STUDY_FQN_FORMAT = "organizationId@projectId:studyId";

    private String organizationId;
    private String projectId;
    private String studyId;

    private String projectUuid;
    private String studyUuid;

    private String providedId;

    private static final String USER_PATTERN = "[A-Za-z][[-_.]?[A-Za-z0-9]?]*";
    private static final String PROJECT_PATTERN = "[A-Za-z0-9][[-_.]?[A-Za-z0-9]?]*";
    private static final String STUDY_PATTERN = "[A-Za-z0-9\\-_.]+|\\*";
    public static final Pattern ORGANIZATION_PROJECT_STUDY_PATTERN = Pattern.compile("^(" + USER_PATTERN + ")@(" + PROJECT_PATTERN
            + "):(" + STUDY_PATTERN + ")$");
    public static final Pattern PROJECT_STUDY_PATTERN = Pattern.compile("^(" + PROJECT_PATTERN + "):(" + STUDY_PATTERN + ")$");

    private CatalogFqn(String organizationId, String id) {
        this.organizationId = organizationId;
        this.providedId = id;
    }

    public static CatalogFqn fromOrganization(String organizationId, JwtPayload payload) {
        String orgId = StringUtils.isNotEmpty(organizationId) ? organizationId : payload.getOrganization();
        return new CatalogFqn(orgId, organizationId);
    }

    public static CatalogFqn fromProjectFqn(String projectFqn) {
        if (StringUtils.isEmpty(projectFqn)) {
            throw new IllegalArgumentException("Missing project fqn");
        }

        if (UuidUtils.isOpenCgaUuid(projectFqn)) {
            throw new IllegalArgumentException("Project fqn cannot be an OpenCGA uuid");
        }

        String[] split = projectFqn.split("@", 2);
        if (split.length == 2) {
            return new CatalogFqn(split[0], projectFqn).setProjectId(split[1]);
        } else {
            throw new IllegalArgumentException("Provided string '" + projectFqn + "' is not a valid project fqn.");
        }
    }

    public static CatalogFqn extractFqnFromProject(String projectStr, JwtPayload payload) {
        if (StringUtils.isEmpty(projectStr)) {
            return new CatalogFqn(payload.getOrganization(), projectStr);
        }

        if (UuidUtils.isOpenCgaUuid(projectStr)) {
            return new CatalogFqn(payload.getOrganization(), projectStr).setProjectUuid(projectStr);
        }

        String[] split = projectStr.split("@");
        if (split.length == 2) {
            return new CatalogFqn(split[0], projectStr)
                    .setProjectId(split[1]);
        } else {
            return new CatalogFqn(payload.getOrganization(), projectStr).setProjectId(projectStr);
        }
    }

    public static CatalogFqn extractFqnFromProjectFqn(String projectFqn) {
        if (StringUtils.isEmpty(projectFqn)) {
            throw new IllegalArgumentException("Missing project fqn");
        }

        String[] split = projectFqn.split("@");
        if (split.length == 2) {
            return new CatalogFqn(split[0], projectFqn)
                    .setProjectId(split[1]);
        } else {
            throw new IllegalArgumentException("Provided string '" + projectFqn + "' is not a valid project fqn.");
        }
    }

    public static CatalogFqn extractFqnFromStudyFqn(String studyFqn) {
        if (StringUtils.isEmpty(studyFqn)) {
            throw new IllegalArgumentException("Missing study fqn");
        }
        Matcher matcher = ORGANIZATION_PROJECT_STUDY_PATTERN.matcher(studyFqn);
        if (matcher.find()) {
            // studyStr contains the full path (organization@project:study)
            String organizationId = matcher.group(1);
            String projectId = matcher.group(2);
            String studyId = matcher.group(3);
            return new CatalogFqn(organizationId, studyFqn)
                    .setProjectId(projectId)
                    .setStudyId(studyId);
        } else {
            throw new IllegalArgumentException("Provided string '" + studyFqn + "' is not a valid study FQN. "
                    + "The accepted pattern is [" + STUDY_FQN_FORMAT + "]");
        }
    }

    public static CatalogFqn extractFqnFromStudy(String studyStr, JwtPayload payload) {
        if (StringUtils.isEmpty(studyStr)) {
            return new CatalogFqn(payload.getOrganization(), studyStr);
        }

        if (UuidUtils.isOpenCgaUuid(studyStr)) {
            return new CatalogFqn(payload.getOrganization(), studyStr).setStudyUuid(studyStr);
        }

        Matcher matcher = ORGANIZATION_PROJECT_STUDY_PATTERN.matcher(studyStr);
        if (matcher.find()) {
            // studyStr contains the full path (organization@project:study)

            // Check for federated studies
            Set<String> federatedStudies = new HashSet<>();
            if (CollectionUtils.isNotEmpty(payload.getFederations())) {
                federatedStudies = payload.getFederations().stream()
                        .flatMap(federation -> federation.getStudyIds().stream())
                        .collect(Collectors.toSet());
            }

            // If the study is federated, the organization that we should be using (where it should be stored) is the one from the payload
            String organizationId = federatedStudies.contains(studyStr) ? payload.getOrganization() : matcher.group(1);
            String projectId = matcher.group(2);
            String studyId = matcher.group(3);
            return new CatalogFqn(organizationId, studyStr)
                    .setProjectId(projectId)
                    .setStudyId(studyId);
        } else {
            matcher = PROJECT_STUDY_PATTERN.matcher(studyStr);
            if (matcher.find()) {
                // studyStr contains the path (project:study)
                String projectId = matcher.group(1);
                String studyId = matcher.group(2);
                return new CatalogFqn(payload.getOrganization(), studyStr)
                        .setProjectId(projectId)
                        .setStudyId(studyId);
            } else {
                // studyStr only contains the study information
                return new CatalogFqn(payload.getOrganization(), studyStr).setStudyId(studyStr);
            }
        }
    }

    public String getProvidedId() {
        return providedId;
    }

    public String getOrganizationId() {
        return organizationId;
    }

    public String getProjectId() {
        return projectId;
    }

    public CatalogFqn setProjectId(String projectId) {
        this.projectId = projectId;
        return this;
    }

    public String getStudyId() {
        return studyId;
    }

    public CatalogFqn setStudyId(String studyId) {
        this.studyId = studyId;
        return this;
    }

    public String getProjectUuid() {
        return projectUuid;
    }

    public CatalogFqn setProjectUuid(String projectUuid) {
        this.projectUuid = projectUuid;
        return this;
    }

    public String getStudyUuid() {
        return studyUuid;
    }

    public CatalogFqn setStudyUuid(String studyUuid) {
        this.studyUuid = studyUuid;
        return this;
    }

    public String toProjectFqn() {
        // check if any of the fields is null
        if (StringUtils.isEmpty(organizationId) || StringUtils.isEmpty(projectId)) {
            throw new IllegalArgumentException("Missing organization or project");
        }
        return organizationId + "@" + projectId;
    }
}
