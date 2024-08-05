package org.opencb.opencga.master.monitor.schedulers;

import org.apache.commons.lang3.time.StopWatch;
import org.opencb.opencga.catalog.exceptions.CatalogException;
import org.opencb.opencga.catalog.managers.CatalogManager;
import org.opencb.opencga.catalog.utils.CatalogFqn;
import org.opencb.opencga.core.common.TimeUtils;
import org.opencb.opencga.core.models.job.Job;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

public class JobScheduler extends AbstractJobScheduler {

    private static final float PRIORITY_WEIGHT = 0.6F;
    private static final float IDLE_TIME_WEIGHT = 0.4F;
//    private static final float OPERATION_WEIGHT = 0.2F;
//    private static final float USER_PRIORITY_WEIGHT = 0.2F;

    private final Logger logger = LoggerFactory.getLogger(JobScheduler.class);


    public JobScheduler(CatalogManager catalogManager, String token) {
        super(catalogManager, token);
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
        if (userRole.isSuperAdmin()) {
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

    @Override
    public Iterator<Job> schedule() {
        // Check if there are queued jobs
        return null;
    }

    public Iterator<Job> schedule(List<Job> pendingJobs, List<Job> queuedJobs, List<Job> runningJobs) {
        TreeMap<Float, List<Job>> jobTreeMap = new TreeMap<>();

        try {
            getUserRoles();
        } catch (CatalogException e) {
            throw new RuntimeException("Scheduler exception: " + e.getMessage(), e);
        }

        StopWatch stopWatch = StopWatch.createStarted();
        for (Job job : pendingJobs) {
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

        return allJobs.iterator();
    }



}
