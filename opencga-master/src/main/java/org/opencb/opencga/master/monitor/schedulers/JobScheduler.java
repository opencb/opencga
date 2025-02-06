package org.opencb.opencga.master.monitor.schedulers;

import org.apache.commons.lang3.StringUtils;
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
        } else if (userRole.isOrganizationOwner(organizationId)) {
            usersPriority = usersPriority * 0.8f;
        } else if (userRole.isOrganizationAdmin(organizationId)) {
            usersPriority = usersPriority * 0.75f;
        } else if (userRole.isStudyAdmin(job.getStudy().getId())) {
            usersPriority = usersPriority * 0.6f;
        } else {
            usersPriority = usersPriority * 0.4f;
        }

        return appPriority * 0.6f + usersPriority * 0.4f;
    }

    public Iterator<Job> schedule(List<Job> pendingJobs, List<Job> queuedJobs, List<Job> runningJobs) {
        Date currentDate = new Date();
        TreeMap<Float, List<Job>> jobTreeMap = new TreeMap<>();

        try {
            getUserRoles();
        } catch (CatalogException e) {
            throw new RuntimeException("Scheduler exception: " + e.getMessage(), e);
        }

        Map<String, Boolean> organizationJobStatus = new HashMap<>();
        try {
            List<String> organizationIds = catalogManager.getOrganizationManager().getOrganizationIds(token);
            for (String organizationId : organizationIds) {
                boolean exceeds = catalogManager.getJobManager().exceedsExecutionLimitQuota(organizationId);
                organizationJobStatus.put(organizationId, exceeds);
            }
        } catch (CatalogException e) {
            throw new RuntimeException("Couldn't get the execution quota status: " + e.getMessage(), e);
        }

        Map<String, List<Long>> rescheduleJobs = new HashMap<>();

        StopWatch stopWatch = StopWatch.createStarted();
        for (Job job : pendingJobs) {
            if (StringUtils.isNotEmpty(job.getScheduledStartTime())) {
                Date date = TimeUtils.toDate(job.getScheduledStartTime());
                if (date.after(currentDate)) {
                    logger.debug("Job '{}' can't be queued yet. It is scheduled to start at '{}'.", job.getId(),
                            job.getScheduledStartTime());
                    continue;
                }
            }

            // Check if execution limit quota has been exceeded
            String organizationId = CatalogFqn.extractFqnFromStudyFqn(job.getStudy().getId()).getOrganizationId();
            if (organizationJobStatus.get(organizationId)) {
                logger.debug("Job '{}' can't be queued yet. The organization '{}' has exceeded the execution limit quota."
                        + " It will be scheduled for next month.", job.getId(), organizationId);
                rescheduleJobs.putIfAbsent(job.getStudy().getId(), new ArrayList<>());
                rescheduleJobs.get(job.getStudy().getId()).add(job.getUid());
                continue;
            }

            float priority = getPriorityWeight(job);
            if (!jobTreeMap.containsKey(priority)) {
                jobTreeMap.put(priority, new ArrayList<>());
            }
            jobTreeMap.get(priority).add(job);
        }
        logger.debug("Time spent scheduling jobs: {}", TimeUtils.durationToString(stopWatch));

        stopWatch.reset();
        stopWatch.start();
        // Obtain iterator
        List<Job> allJobs = new ArrayList<>();
        for (Float priority : jobTreeMap.descendingKeySet()) {
            allJobs.addAll(jobTreeMap.get(priority));
        }
        logger.debug("Time spent creating iterator: {}", TimeUtils.durationToString(stopWatch));

        // Reschedule jobs that have exceeded the execution limit quota for next month
        if (!rescheduleJobs.isEmpty()) {
            stopWatch.reset();
            stopWatch.start();

            for (Map.Entry<String, List<Long>> entry : rescheduleJobs.entrySet()) {
                String studyFqn = entry.getKey();
                List<Long> jobUids = entry.getValue();

                Date firstDayOfNextMonth = TimeUtils.getFirstDayOfNextMonth(currentDate);
                String scheduledStartTime = TimeUtils.getTime(firstDayOfNextMonth);
                logger.debug("Rescheduling all pending jobs from study '{}' for next month ({}): {}", studyFqn, scheduledStartTime,
                        jobUids);
                try {
                    catalogManager.getJobManager().rescheduleJobs(studyFqn, jobUids, scheduledStartTime, "Execution limit quota exceeded"
                            + " for this month. Automatically rescheduling job for date '" + scheduledStartTime + "'.", token);
                } catch (CatalogException e) {
                    throw new RuntimeException("Couldn't reschedule jobs for next month: " + e.getMessage(), e);
                }
            }
            logger.debug("Time spent rescheduling jobs for next month: {}", TimeUtils.durationToString(stopWatch));
        }

        return allJobs.iterator();
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
