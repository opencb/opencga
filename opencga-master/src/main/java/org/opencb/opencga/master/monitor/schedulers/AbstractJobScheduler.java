package org.opencb.opencga.master.monitor.schedulers;

import org.apache.commons.lang3.time.StopWatch;
import org.opencb.commons.datastore.core.Query;
import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.opencga.catalog.db.api.StudyDBAdaptor;
import org.opencb.opencga.catalog.db.api.UserDBAdaptor;
import org.opencb.opencga.catalog.exceptions.CatalogException;
import org.opencb.opencga.catalog.managers.CatalogManager;
import org.opencb.opencga.catalog.managers.OrganizationManager;
import org.opencb.opencga.core.api.ParamConstants;
import org.opencb.opencga.core.common.TimeUtils;
import org.opencb.opencga.core.models.job.Job;
import org.opencb.opencga.core.models.organizations.Organization;
import org.opencb.opencga.core.models.study.Group;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

public abstract class AbstractJobScheduler {

    private Map<String, UserRole> userRoles;

    protected final CatalogManager catalogManager;
    protected final String token;

    private final Logger logger = LoggerFactory.getLogger(AbstractJobScheduler.class);

    public AbstractJobScheduler(CatalogManager catalogManager, String token) {
        this.catalogManager = catalogManager;
        this.token = token;
    }

    public abstract Iterator<Job> schedule();

    protected void getUserRoles() throws CatalogException {
        StopWatch stopWatch = StopWatch.createStarted();
        this.userRoles = new HashMap<>();

        List<String> organizationIds = catalogManager.getOrganizationManager().getOrganizationIds(token);
        for (String organizationId : organizationIds) {
            if (ParamConstants.ADMIN_ORGANIZATION.equals(organizationId)) {
                QueryOptions options = new QueryOptions(QueryOptions.INCLUDE, UserDBAdaptor.QueryParams.ID.key());
                catalogManager.getUserManager().search(organizationId, new Query(), options, token).getResults()
                        .forEach(user -> getUserRole(organizationId, user.getId()).setSuperAdmin(true));
            } else {
                Organization organization = catalogManager.getOrganizationManager().get(organizationId,
                        OrganizationManager.INCLUDE_ORGANIZATION_ADMINS, token).first();
                getUserRole(organizationId, organization.getOwner()).addOrganizationOwner(organizationId);
                organization.getAdmins().forEach(user -> getUserRole(organizationId, user).addOrganizationAdmin(organizationId));

                QueryOptions options = new QueryOptions(QueryOptions.INCLUDE, Arrays.asList(StudyDBAdaptor.QueryParams.FQN.key(),
                        StudyDBAdaptor.QueryParams.GROUPS.key()));
                catalogManager.getStudyManager().searchInOrganization(organizationId, new Query(), options, token).getResults()
                        .forEach(study -> {
                            for (Group group : study.getGroups()) {
                                if (ParamConstants.ADMINS_GROUP.equals(group.getId())) {
                                    group.getUserIds().forEach(user -> getUserRole(organizationId, user).addStudyAdmin(study.getFqn()));
                                }
                            }
                        });
            }
        }

        logger.debug("Time spent fetching user roles: {}", TimeUtils.durationToString(stopWatch));
    }

    protected JobScheduler.UserRole getUserRole(String organizationId, String userId) {
        String id = organizationId + "@" + userId;
        if (!this.userRoles.containsKey(id)) {
            this.userRoles.put(id, new JobScheduler.UserRole());
        }
        return this.userRoles.get(id);
    }

    protected static class UserRole {

        private boolean isSuperAdmin;
        private final Set<String> organizationOwners;
        private final Set<String> organizationAdmins;
        private final Set<String> studyAdmins;

        UserRole() {
            this.organizationOwners = new HashSet<>();
            this.organizationAdmins = new HashSet<>();
            this.studyAdmins = new HashSet<>();
        }

        public boolean isSuperAdmin() {
            return isSuperAdmin;
        }

        public UserRole setSuperAdmin(boolean superAdmin) {
            isSuperAdmin = superAdmin;
            return this;
        }

        public void addOrganizationOwner(String userId) {
            organizationOwners.add(userId);
        }

        public void addOrganizationAdmin(String userId) {
            organizationAdmins.add(userId);
        }

        public void addStudyAdmin(String userId) {
            studyAdmins.add(userId);
        }

        public boolean isOrganizationOwner(String organizationId) {
            return organizationOwners.contains(organizationId);
        }

        public boolean isOrganizationAdmin(String organizationId) {
            return organizationAdmins.contains(organizationId);
        }

        public boolean isStudyAdmin(String studyFqn) {
            return studyAdmins.contains(studyFqn);
        }
    }

}
