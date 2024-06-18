package org.opencb.opencga.master.monitor.schedulers;

import org.apache.commons.lang3.time.StopWatch;
import org.opencb.commons.datastore.core.Query;
import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.opencga.catalog.db.api.StudyDBAdaptor;
import org.opencb.opencga.catalog.db.api.UserDBAdaptor;
import org.opencb.opencga.catalog.exceptions.CatalogException;
import org.opencb.opencga.catalog.managers.CatalogManager;
import org.opencb.opencga.catalog.managers.OrganizationManager;
import org.opencb.opencga.catalog.utils.CatalogFqn;
import org.opencb.opencga.core.api.ParamConstants;
import org.opencb.opencga.core.common.TimeUtils;
import org.opencb.opencga.core.models.job.Job;
import org.opencb.opencga.core.models.organizations.Organization;
import org.opencb.opencga.core.models.study.Group;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

public class JobScheduler {

    private TreeMap<Float, Job> jobTreeMap;

    private final CatalogManager catalogManager;
    private final String token;

    private Map<String, UserRole> userRoles;

    private static final float PRIORITY_WEIGHT = 0.6F;
    private static final float IDLE_TIME_WEIGHT = 0.4F;
//    private static final float OPERATION_WEIGHT = 0.2F;
//    private static final float USER_PRIORITY_WEIGHT = 0.2F;

    private final Logger logger = LoggerFactory.getLogger(JobScheduler.class);


    public JobScheduler(CatalogManager catalogManager, String token) {
        this.catalogManager = catalogManager;
        this.token = token;
        this.jobTreeMap = new TreeMap<>();
    }

    public void addJobs(List<Job> pendingJobs, List<Job> queuedJobs, List<Job> runningJobs) {
        jobTreeMap.clear();
        try {
            getUserRoles();
        } catch (CatalogException e) {
            throw new RuntimeException("Scheduler exception: " + e.getMessage(), e);
        }

        StopWatch stopWatch = StopWatch.createStarted();
        for (Job job : pendingJobs) {
            float priority = getPriorityWeight(job);
            jobTreeMap.put(priority, job);
        }
        logger.debug("Time spent scheduling jobs: {}", TimeUtils.durationToString(stopWatch));
    }

    private void getUserRoles() throws CatalogException {
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

    private UserRole getUserRole(String organizationId, String userId) {
        String id = organizationId + "@" + userId;
        if (!this.userRoles.containsKey(id)) {
            this.userRoles.put(id, new UserRole());
        }
        return this.userRoles.get(id);
    }

    public Job getNext() {
        return jobTreeMap.pollLastEntry().getValue();
    }

    public void emtpy() {
        jobTreeMap.clear();
    }

    private float extractPriority(Job job) {
        float value = 0;
        // Priority
        switch (job.getPriority()) {
            case URGENT:
                value += PRIORITY_WEIGHT;
                break;
            case HIGH:
                value += PRIORITY_WEIGHT * 4 / 5;
                break;
            case MEDIUM:
                value += PRIORITY_WEIGHT * 3 / 5;
                break;
            case LOW:
                value += PRIORITY_WEIGHT * 2 / 5;
                break;
            case UNKNOWN:
            default:
                value += PRIORITY_WEIGHT * 1 / 5;
                break;
        }

        // Idle time
        Date currentDate = TimeUtils.getDate();
        Calendar currentCal = new GregorianCalendar();
        currentCal.setTime(currentDate);

        Date jobDate = TimeUtils.toDate(job.getCreationDate());
        Calendar cal = new GregorianCalendar();
        cal.setTime(jobDate);

        cal.add(Calendar.HOUR, 24);
        if (cal.before(currentCal)) { // Job was created more than 24 hours ago
            value += IDLE_TIME_WEIGHT;
        } else {
            cal.add(Calendar.HOUR, -6); // 18 hours
            if (cal.before(currentCal)) {
                value += IDLE_TIME_WEIGHT * 6 / 7;
            } else {
                cal.add(Calendar.HOUR, -6); // 12 hours
                if (cal.before(currentCal)) {
                    value += IDLE_TIME_WEIGHT * 5 / 7;
                } else {
                    cal.add(Calendar.HOUR, -4); // 8 hours
                    if (cal.before(currentCal)) {
                        value += IDLE_TIME_WEIGHT * 4 / 7;
                    } else {
                        cal.add(Calendar.HOUR, -4); // 4 hours
                        if (cal.before(currentCal)) {
                            value += IDLE_TIME_WEIGHT * 3 / 7;
                        } else {
                            cal.add(Calendar.HOUR, -2); // 2 hours
                            if (cal.before(currentCal)) {
                                value += IDLE_TIME_WEIGHT * 2 / 7;
                            } else {
                                cal.add(Calendar.HOUR, -1); // 1 hour
                                if (cal.before(currentCal)) {
                                    value += IDLE_TIME_WEIGHT * 1 / 7;
                                }
                            }
                        }

                    }

                }
            }
        }

        // TODO: Check operation and user priority
        return value;
    }

    private float getPriorityWeight(Job job) {
        float appPriority;
        switch (job.getPriority()) {
            case URGENT:
                appPriority = 1f;
                break;
            case HIGH:
                appPriority = 0.8f;
                break;
            case MEDIUM:
                appPriority = 0.5f;
                break;
            case LOW:
            case UNKNOWN:
            default:
                appPriority = 0.2f;
                break;
        }

        // Increase priority depending on the time the job has been waiting in PENDING status
        Calendar todayCalendar = Calendar.getInstance();
        Date date = TimeUtils.toDate(job.getInternal().getRegistrationDate());
        Calendar calendar = Calendar.getInstance();
        calendar.setTime(date);
        while (appPriority < 1f) {
            calendar.add(Calendar.HOUR, 24);
            if (calendar.before(todayCalendar)) {
                appPriority = Math.max(1f, appPriority + 0.05f);
            } else {
                break;
            }
        }

        // User's priority
        float usersPriority;
        switch (job.getPriority()) {
            case URGENT:
                usersPriority = 1f;
                break;
            case HIGH:
                usersPriority = 0.8f;
                break;
            case MEDIUM:
                usersPriority = 0.5f;
                break;
            case LOW:
            case UNKNOWN:
            default:
                usersPriority = 0.2f;
                break;
        }

        // Adjust user's priority depending on the user's role
        String userId = job.getUserId();
        String organizationId = CatalogFqn.extractFqnFromStudyFqn(job.getStudy().getId()).getOrganizationId();

        UserRole userRole = getUserRole(organizationId, userId);
        if (userRole.isSuperAdmin) {
            usersPriority = usersPriority * 1;
        } else if (userRole.isOrganizationOwner(organizationId) || userRole.isOrganizationAdmin(organizationId)) {
            usersPriority = usersPriority * 0.8f;
        } else if (userRole.isStudyAdmin(job.getStudy().getId())) {
            usersPriority = usersPriority * 0.6f;
        } else {
            usersPriority = usersPriority * 0.4f;
        }

        return appPriority * 0.6f + usersPriority * 0.4f;
    }

    public Iterator<Job> iterator() {
        return jobTreeMap.values().iterator();
    }

    private static class UserRole {

        private boolean isSuperAdmin;
        private Set<String> organizationOwners;
        private Set<String> organizationAdmins;
        private Set<String> studyAdmins;

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
