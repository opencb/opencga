package org.opencb.opencga.master.monitor.schedulers;

import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.opencb.commons.datastore.core.Query;
import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.opencga.catalog.exceptions.CatalogException;
import org.opencb.opencga.catalog.managers.*;
import org.opencb.opencga.core.common.TimeUtils;
import org.opencb.opencga.core.config.ExecutionQueue;
import org.opencb.opencga.core.models.common.Enums;
import org.opencb.opencga.core.models.job.Job;
import org.opencb.opencga.core.models.job.JobInternal;
import org.opencb.opencga.core.models.job.JobStudyParam;
import org.opencb.opencga.core.models.job.ToolInfo;
import org.opencb.opencga.core.models.organizations.Organization;
import org.opencb.opencga.core.response.OpenCGAResult;
import org.opencb.opencga.core.testclassification.duration.ShortTests;

import java.util.*;

import static org.junit.Assert.*;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

@Category(ShortTests.class)
public class JobSchedulerTest {

    @Mock
    private CatalogManager catalogManager;
    @Mock
    private JobManager jobManager;
    @Mock
    private UserManager userManager;
    @Mock
    private OrganizationManager organizationManager;
    @Mock
    private StudyManager studyManager;

    private JobScheduler jobScheduler;
    private List<ExecutionQueue> queues;
    private String token = "test-token";
    private String organizationId = "test-org";

    @Before
    public void setUp() throws CatalogException {
        MockitoAnnotations.initMocks(this);

        // Setup catalog manager mocks
        when(catalogManager.getJobManager()).thenReturn(jobManager);
        when(catalogManager.getUserManager()).thenReturn(userManager);
        when(catalogManager.getOrganizationManager()).thenReturn(organizationManager);
        when(catalogManager.getStudyManager()).thenReturn(studyManager);

        // Setup default mocks for user roles
        when(userManager.search(anyString(), any(Query.class), any(QueryOptions.class), anyString()))
                .thenReturn(new OpenCGAResult<>(0, Collections.emptyList(), 0, Collections.emptyList(), 0));

        Organization organization = new Organization().setOwner("owner").setAdmins(Collections.emptyList());
        when(organizationManager.get(anyString(), any(QueryOptions.class), anyString()))
                .thenReturn(new OpenCGAResult<>(0, Collections.emptyList(), 1, Collections.singletonList(organization), 1L));

        when(studyManager.searchInOrganization(anyString(), any(Query.class), any(QueryOptions.class), anyString()))
                .thenReturn(new OpenCGAResult<>(0, Collections.emptyList(), 0, Collections.emptyList(), 0));

        // Create test queues
        queues = Arrays.asList(
                new ExecutionQueue().setId("small-queue").setProcessorType(ExecutionQueue.ProcessorType.CPU).setCpu(2).setMemory("4GB"),
                new ExecutionQueue().setId("medium-queue").setProcessorType(ExecutionQueue.ProcessorType.CPU).setCpu(8).setMemory("16GB"),
                new ExecutionQueue().setId("large-queue").setProcessorType(ExecutionQueue.ProcessorType.CPU).setCpu(16).setMemory("32GB"),
                new ExecutionQueue().setId("local-queue").setProcessorType(ExecutionQueue.ProcessorType.GPU).setCpu(4).setMemory("8GB")
        );

        jobScheduler = new JobScheduler(catalogManager, queues, token);
    }

    @Test
    public void testScheduleJobsWithoutQuotaExceeded() throws CatalogException {
        // Setup: quota not exceeded
        when(jobManager.exceedsExecutionLimitQuota(organizationId)).thenReturn(false);

        List<Job> pendingJobs = createTestJobs();
        Set<String> exhaustedQueues = Collections.emptySet();

        Map<String, List<Job>> result = jobScheduler.schedule(organizationId, pendingJobs, exhaustedQueues);

        // Verify jobs are assigned to appropriate queues
        assertTrue(result.containsKey("small-queue"));
        assertTrue(result.containsKey("medium-queue"));
        assertEquals(1, result.get("small-queue").size());
        assertEquals(1, result.get("medium-queue").size());
    }

    @Test
    public void testScheduleJobsWithQuotaExceeded() throws CatalogException {
        // Setup: quota exceeded
        when(jobManager.exceedsExecutionLimitQuota(organizationId)).thenReturn(true);

        List<Job> pendingJobs = createTestJobs();
        Set<String> exhaustedQueues = Collections.emptySet();

        Map<String, List<Job>> result = jobScheduler.schedule(organizationId, pendingJobs, exhaustedQueues);

        // Verify no jobs are scheduled when quota is exceeded
        assertTrue(result.isEmpty() || result.values().stream().allMatch(List::isEmpty));

        // Verify reschedule method is called
        verify(jobManager, atLeastOnce()).rescheduleJobs(anyString(), anyList(), anyString(), anyString(), eq(token));
    }

    @Test
    public void testScheduleJobsWithExhaustedQueues() throws CatalogException {
        // Setup: quota not exceeded but some queues are exhausted
        when(jobManager.exceedsExecutionLimitQuota(organizationId)).thenReturn(false);

        List<Job> pendingJobs = createTestJobs();
        Set<String> exhaustedQueues = new HashSet<>(Arrays.asList("small-queue", "medium-queue"));

        Map<String, List<Job>> result = jobScheduler.schedule(organizationId, pendingJobs, exhaustedQueues);

        // Verify exhausted queues are not used
        assertFalse(result.containsKey("small-queue"));
        assertFalse(result.containsKey("medium-queue"));

        // Jobs should be assigned to available queues or not scheduled at all
        assertTrue(result.isEmpty() || result.keySet().stream().noneMatch(exhaustedQueues::contains));
    }

    @Test
    public void testScheduleJobsWithAllQueuesExhausted() throws CatalogException {
        // Setup: quota not exceeded but all queues are exhausted
        when(jobManager.exceedsExecutionLimitQuota(organizationId)).thenReturn(false);

        List<Job> pendingJobs = createTestJobs();
        Set<String> exhaustedQueues = new HashSet<>(Arrays.asList("small-queue", "medium-queue", "large-queue", "local-queue"));

        Map<String, List<Job>> result = jobScheduler.schedule(organizationId, pendingJobs, exhaustedQueues);

        // Verify no jobs are scheduled when all queues are exhausted
        assertTrue(result.isEmpty());
    }

    @Test
    public void testScheduleJobsWithMixedConditions() throws CatalogException {
        // Setup: quota not exceeded, some queues exhausted
        when(jobManager.exceedsExecutionLimitQuota(organizationId)).thenReturn(false);

        List<Job> pendingJobs = createTestJobs();
        Set<String> exhaustedQueues = new HashSet<>(Arrays.asList("small-queue"));

        Map<String, List<Job>> result = jobScheduler.schedule(organizationId, pendingJobs, exhaustedQueues);

        // Verify exhausted queue is not used
        assertFalse(result.containsKey("small-queue"));

        // Other queues should still be available
        assertTrue(result.containsKey("medium-queue"));
        assertEquals(1, result.get("medium-queue").size());
    }

    @Test
    public void testScheduleJobsWithScheduledStartTime() throws CatalogException {
        // Setup: quota not exceeded
        when(jobManager.exceedsExecutionLimitQuota(organizationId)).thenReturn(false);

        // Create job with future scheduled start time
        Date futureDate = new Date(System.currentTimeMillis() + 86400000); // Tomorrow
        Job futureJob = createJobWithScheduledTime("future-job", futureDate);

        List<Job> pendingJobs = Collections.singletonList(futureJob);
        Set<String> exhaustedQueues = Collections.emptySet();

        Map<String, List<Job>> result = jobScheduler.schedule(organizationId, pendingJobs, exhaustedQueues);

        // Verify future scheduled job is not executed yet
        assertTrue(result.isEmpty() || result.values().stream().allMatch(List::isEmpty));
    }

    @Test
    public void testScheduleJobsWithPastScheduledTime() throws CatalogException {
        // Setup: quota not exceeded
        when(jobManager.exceedsExecutionLimitQuota(organizationId)).thenReturn(false);

        // Create job with past scheduled start time
        Date pastDate = new Date(System.currentTimeMillis() - 86400000); // Yesterday
        Job pastJob = createJobWithScheduledTime("past-job", pastDate);

        List<Job> pendingJobs = Collections.singletonList(pastJob);
        Set<String> exhaustedQueues = Collections.emptySet();

        Map<String, List<Job>> result = jobScheduler.schedule(organizationId, pendingJobs, exhaustedQueues);

        // Verify past scheduled job is executed
        assertFalse(result.isEmpty());
        assertTrue(result.values().stream().anyMatch(jobs -> !jobs.isEmpty()));
    }

    private List<Job> createTestJobs() {
        List<Job> jobs = new ArrayList<>();

        // Small job for small queue
        Job smallJob = new Job()
                .setId("small-job")
                .setUserId("user1")
                .setStudy(new JobStudyParam(organizationId + "@project:study1"))
                .setPriority(Enums.Priority.MEDIUM)
                .setInternal(new JobInternal().setRegistrationDate(TimeUtils.getTime()))
                .setTool(new ToolInfo().setMinimumRequirements(
                        new org.opencb.opencga.core.models.job.MinimumRequirements()
                                .setProcessorType(ExecutionQueue.ProcessorType.CPU)
                                .setCpu("2")
                                .setMemory("4GB")
                                .setQueue("small-queue")
                ));
        jobs.add(smallJob);

        // Medium job for medium queue
        Job mediumJob = new Job()
                .setId("medium-job")
                .setUserId("user2")
                .setStudy(new JobStudyParam(organizationId + "@project:study1"))
                .setPriority(Enums.Priority.HIGH)
                .setInternal(new JobInternal().setRegistrationDate(TimeUtils.getTime()))
                .setTool(new ToolInfo().setMinimumRequirements(
                        new org.opencb.opencga.core.models.job.MinimumRequirements()
                                .setProcessorType(ExecutionQueue.ProcessorType.CPU)
                                .setCpu("4")
                                .setMemory("8GB")
                                .setQueue("medium-queue")
                ));
        jobs.add(mediumJob);

        return jobs;
    }

    private Job createJobWithScheduledTime(String jobId, Date scheduledTime) {
        return new Job()
                .setId(jobId)
                .setUserId("user1")
                .setStudy(new JobStudyParam(organizationId + "@project:study1"))
                .setPriority(Enums.Priority.MEDIUM)
                .setScheduledStartTime(TimeUtils.getTime(scheduledTime))
                .setInternal(new JobInternal().setRegistrationDate(TimeUtils.getTime()))
                .setTool(new ToolInfo().setMinimumRequirements(
                        new org.opencb.opencga.core.models.job.MinimumRequirements()
                                .setProcessorType(ExecutionQueue.ProcessorType.CPU)
                                .setCpu("2")
                                .setMemory("4GB")
                                .setQueue("small-queue")
                ));
    }
}