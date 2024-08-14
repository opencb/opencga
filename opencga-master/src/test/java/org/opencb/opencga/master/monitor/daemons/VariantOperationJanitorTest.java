package org.opencb.opencga.master.monitor.daemons;

import org.junit.Before;
import org.junit.Test;
import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.opencga.TestParamConstants;
import org.opencb.opencga.analysis.variant.operations.VariantAnnotationIndexOperationTool;
import org.opencb.opencga.catalog.managers.AbstractManagerTest;
import org.opencb.opencga.core.config.OperationExecutionConfig;
import org.opencb.opencga.core.models.common.Enums;
import org.opencb.opencga.core.models.job.Job;
import org.opencb.opencga.core.models.job.JobInternal;
import org.opencb.opencga.core.models.project.ProjectInternalVariant;
import org.opencb.opencga.core.models.study.StudyInternalVariant;
import org.opencb.opencga.core.models.variant.InternalVariantOperationIndex;
import org.opencb.opencga.core.models.variant.OperationIndexStatus;
import org.opencb.opencga.master.monitor.models.PrivateJobUpdateParams;

import java.util.List;

import static org.junit.Assert.*;

public class VariantOperationJanitorTest extends AbstractManagerTest {

    private VariantOperationJanitor janitor;

    @Override
    @Before
    public void setUp() throws Exception {
        super.setUp();

        String expiringToken = this.catalogManager.getUserManager().loginAsAdmin(TestParamConstants.ADMIN_PASSWORD).getToken();

        // Change to immediate
        catalogManager.getConfiguration().getAnalysis().getOperations().getVariantAnnotationIndex().setPolicy(OperationExecutionConfig.Policy.IMMEDIATE).setMaxAttempts(2);

        janitor = new VariantOperationJanitor(catalogManager, expiringToken);

        // Set all operations to PENDING
        catalogManager.getProjectManager().setProjectInternalVariant(projectFqn1, new ProjectInternalVariant(
                        new InternalVariantOperationIndex(new OperationIndexStatus(OperationIndexStatus.PENDING)),
                        new InternalVariantOperationIndex(new OperationIndexStatus(OperationIndexStatus.PENDING))),
                QueryOptions.empty(), expiringToken);

        catalogManager.getStudyManager().setStudyInternalVariant(studyFqn, new StudyInternalVariant(
                new InternalVariantOperationIndex(new OperationIndexStatus(OperationIndexStatus.PENDING))),
                QueryOptions.empty(), expiringToken);
    }

    @Test
    public void checkPendingVariantOperations() throws Exception {
        janitor.checkPendingVariantOperations();
        Job lastJobExecution = janitor.findLastJobExecution(studyFqn, VariantAnnotationIndexOperationTool.ID);
        assertNotNull(lastJobExecution);
        assertEquals(Enums.ExecutionStatus.PENDING, lastJobExecution.getInternal().getStatus().getId());
        assertTrue(lastJobExecution.getTags().contains(VariantOperationJanitor.TAG));
        assertEquals(1, lastJobExecution.getAttributes().get(VariantOperationJanitor.ATTEMPT));
        String jobId = lastJobExecution.getId();
        // Set status to ERROR
        PrivateJobUpdateParams updateParams = new PrivateJobUpdateParams().setInternal(new JobInternal(new Enums.ExecutionStatus(Enums.ExecutionStatus.ERROR)));
        catalogManager.getJobManager().update(studyFqn, lastJobExecution.getId(), updateParams, QueryOptions.empty(), ownerToken);


        janitor.checkPendingVariantOperations();
        lastJobExecution = janitor.findLastJobExecution(studyFqn, VariantAnnotationIndexOperationTool.ID);
        assertNotNull(lastJobExecution);
        assertEquals(Enums.ExecutionStatus.PENDING, lastJobExecution.getInternal().getStatus().getId());
        assertTrue(lastJobExecution.getTags().contains(VariantOperationJanitor.TAG));
        assertEquals(2, lastJobExecution.getAttributes().get(VariantOperationJanitor.ATTEMPT));
        assertEquals(1, ((List<String>) lastJobExecution.getAttributes().get(VariantOperationJanitor.FAILED_ATTEMPT_JOB_IDS)).size());
        assertEquals(jobId, ((List<String>) lastJobExecution.getAttributes().get(VariantOperationJanitor.FAILED_ATTEMPT_JOB_IDS)).get(0));
        // Set status to ERROR
        catalogManager.getJobManager().update(studyFqn, lastJobExecution.getId(), updateParams, QueryOptions.empty(), ownerToken);

        // Check a third time (This time it shouldn't create a new job)
        janitor.checkPendingVariantOperations();
        lastJobExecution = janitor.findLastJobExecution(studyFqn, VariantAnnotationIndexOperationTool.ID);
        assertNotNull(lastJobExecution);
        assertEquals(Enums.ExecutionStatus.ERROR, lastJobExecution.getInternal().getStatus().getId());
        assertEquals(2, lastJobExecution.getAttributes().get(VariantOperationJanitor.ATTEMPT));
        assertEquals(1, ((List<String>) lastJobExecution.getAttributes().get(VariantOperationJanitor.FAILED_ATTEMPT_JOB_IDS)).size());
        assertEquals(jobId, ((List<String>) lastJobExecution.getAttributes().get(VariantOperationJanitor.FAILED_ATTEMPT_JOB_IDS)).get(0));
        assertTrue(lastJobExecution.getTags().contains(VariantOperationJanitor.TAG));

    }

//    @Mock
//    private CatalogManager catalogManager;
//
//    @Mock
//    private Configuration configuration;
//
//    private VariantOperationOrchestrator variantOperationOrchestrator;
//
//    @BeforeEach
//    public void setUp() {
//        MockitoAnnotations.initMocks(this);
//        variantOperationOrchestrator = new VariantOperationOrchestrator(catalogManager, configuration, "token");
//    }
//
//    @Test
//    public void checkPendingVariantOperationsNeverPolicy() throws Exception {
//        when(configuration.getAnalysis().getOperations().getVariantAnnotationIndex().getPolicy())
//                .thenReturn(OperationExecutionConfig.Policy.NEVER);
//
//        variantOperationOrchestrator.checkPendingVariantOperations();
//
//        verify(catalogManager, never()).getOrganizationManager();
//    }
//
//    @Test
//    public void checkPendingVariantOperationsNightlyPolicyNotNightTime() throws Exception {
//        when(configuration.getAnalysis().getOperations().getVariantAnnotationIndex().getPolicy())
//                .thenReturn(OperationExecutionConfig.Policy.NIGHTLY);
//
//        variantOperationOrchestrator.checkPendingVariantOperations();
//
//        verify(catalogManager, never()).getOrganizationManager();
//    }
//
//    @Test
//    public void noPendingJobsReturnsTrueWhenNoJobs() throws Exception {
//        when(catalogManager.getJobManager().count(anyString(), any(), anyString()))
//                .thenReturn(new OpenCGAResult<>(0, Collections.emptyList(), 0, Collections.emptyList(), 0));
//
//        assertTrue(variantOperationOrchestrator.noPendingJobs(Collections.singletonList("studyId"), Collections.singletonList("toolId")));
//    }
//
//    @Test
//    public void noPendingJobsReturnsFalseWhenJobsExist() throws Exception {
//        when(catalogManager.getJobManager().count(anyString(), any(), anyString()))
//                .thenReturn(new OpenCGAResult<>(1, Collections.emptyList(), 1, Collections.emptyList(), 1));
//
//        assertFalse(variantOperationOrchestrator.noPendingJobs(Collections.singletonList("studyId"), Collections.singletonList("toolId")));
//    }
}