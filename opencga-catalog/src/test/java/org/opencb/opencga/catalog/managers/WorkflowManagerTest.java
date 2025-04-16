package org.opencb.opencga.catalog.managers;

import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.opencb.commons.datastore.core.Query;
import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.opencga.catalog.db.api.ExternalToolDBAdaptor;
import org.opencb.opencga.catalog.exceptions.CatalogAuthorizationException;
import org.opencb.opencga.catalog.exceptions.CatalogException;
import org.opencb.opencga.catalog.utils.ParamUtils;
import org.opencb.opencga.core.models.externalTool.*;
import org.opencb.opencga.core.models.externalTool.workflow.WorkflowCreateParams;
import org.opencb.opencga.core.response.OpenCGAResult;
import org.opencb.opencga.core.testclassification.duration.MediumTests;

import java.util.Arrays;
import java.util.Collections;

import static org.junit.Assert.*;

@Category(MediumTests.class)
public class WorkflowManagerTest extends AbstractManagerTest {

    private WorkflowManager workflowManager;

    @Before
    public void setUp() throws Exception {
        super.setUp();
        workflowManager = catalogManager.getWorkflowManager();
    }

    @Test
    public void importWorkflow() throws CatalogException {
        WorkflowRepositoryParams params = new WorkflowRepositoryParams("nf-core/rnaseq");
        OpenCGAResult<ExternalTool> result = workflowManager.importWorkflow(studyFqn, params, INCLUDE_RESULT, ownerToken);
        assertEquals(1, result.getNumInserted());
        assertEquals(1, result.first().getVersion());

        // Update imported workflow
        result = workflowManager.importWorkflow(studyFqn, params, INCLUDE_RESULT, ownerToken);
        assertEquals(2, result.first().getVersion());

        params = new WorkflowRepositoryParams("nf-core/proteinfold");
        result = workflowManager.importWorkflow(studyFqn, params, INCLUDE_RESULT, ownerToken);
        assertEquals(1, result.getNumInserted());
        assertEquals(1, result.first().getVersion());

        params = new WorkflowRepositoryParams("nf-core/methylseq");
        result = workflowManager.importWorkflow(studyFqn, params, INCLUDE_RESULT, ownerToken);
        assertEquals(1, result.getNumInserted());
        assertEquals(1, result.first().getVersion());

        params = new WorkflowRepositoryParams("nf-core/pacvar");
        result = workflowManager.importWorkflow(studyFqn, params, INCLUDE_RESULT, ownerToken);
        assertEquals(1, result.getNumInserted());
        assertEquals(1, result.first().getVersion());
    }

    @Test
    public void createWorkflowTest() throws CatalogException {
        WorkflowCreateParams workflowCreateParams = new WorkflowCreateParams()
                .setId("workflow")
                .setScope(ExternalToolScope.OTHER)
                .setWorkflow(new Workflow().setScripts(Collections.singletonList(new WorkflowScript("pipeline.nf", "echo 'Hello world!'", true))));
        OpenCGAResult<ExternalTool> result = workflowManager.create(studyFqn, workflowCreateParams, INCLUDE_RESULT, ownerToken);
        assertEquals(1, result.getNumResults());
        assertNotNull(result.first());
        assertEquals(workflowCreateParams.getId(), result.first().getId());

        // Add repository to workflow
        workflowCreateParams.setId("workflow2");
        workflowCreateParams.getWorkflow().setRepository(new WorkflowRepository("blabla"));
        CatalogException catalogException = assertThrows(CatalogException.class,
                () -> workflowManager.create(studyFqn, workflowCreateParams, INCLUDE_RESULT, ownerToken));
        assertTrue(catalogException.getMessage().contains("script") && catalogException.getMessage().contains("repository"));

        // Remove script from workflow
        workflowCreateParams.getWorkflow().setScripts(Collections.emptyList());
        result = workflowManager.create(studyFqn, workflowCreateParams, INCLUDE_RESULT, ownerToken);
        assertEquals(1, result.getNumResults());
        assertNotNull(result.first());
        assertEquals(workflowCreateParams.getId(), result.first().getId());

        // Remove script and add two scripts with 2 mains
        workflowCreateParams.setId("workflow3");
        workflowCreateParams.getWorkflow().setRepository(null);
        workflowCreateParams.getWorkflow().setScripts(Arrays.asList(
                new WorkflowScript("script1", "echo 'Hello'", true),
                new WorkflowScript("script2", "echo 'World'", true)
        ));
        catalogException = assertThrows(CatalogException.class,
                () -> workflowManager.create(studyFqn, workflowCreateParams, INCLUDE_RESULT, ownerToken));
        assertTrue(catalogException.getMessage().contains("script") && catalogException.getMessage().contains("main"));

        // Add one single script without main
        workflowCreateParams.getWorkflow().setScripts(Collections.singletonList(
                new WorkflowScript("script1", "echo 'Hello'", false)
        ));
        catalogException = assertThrows(CatalogException.class,
                () -> workflowManager.create(studyFqn, workflowCreateParams, INCLUDE_RESULT, ownerToken));
        assertTrue(catalogException.getMessage().contains("script") && catalogException.getMessage().contains("main"));
    }

    @Test
    public void workflowSearchTest() throws CatalogException {
        WorkflowCreateParams workflowCreateParams = new WorkflowCreateParams()
                .setId("workflow")
                .setScope(ExternalToolScope.OTHER)
                .setWorkflow(new Workflow().setScripts(Collections.singletonList(new WorkflowScript("pipeline.nf", "echo 'Hello world!'", true))));
        workflowManager.create(studyFqn, workflowCreateParams, INCLUDE_RESULT, ownerToken);

        workflowCreateParams = new WorkflowCreateParams()
                .setId("workflow2")
                .setScope(ExternalToolScope.OTHER)
                .setDraft(true)
                .setWorkflow(new Workflow().setScripts(Collections.singletonList(new WorkflowScript("pipeline.nf", "echo 'Hello world!'", true))));
        workflowManager.create(studyFqn, workflowCreateParams, INCLUDE_RESULT, ownerToken);

        OpenCGAResult<ExternalTool> search = workflowManager.search(studyFqn, new Query(), QueryOptions.empty(), ownerToken);
        assertEquals(2, search.getNumResults());

        Query query = new Query(ExternalToolDBAdaptor.QueryParams.DRAFT.key(), true);
        search = workflowManager.search(studyFqn, query, QueryOptions.empty(), ownerToken);
        assertEquals(1, search.getNumResults());
        assertEquals("workflow2", search.first().getId());
        assertTrue(search.first().isDraft());

        query = new Query(ExternalToolDBAdaptor.QueryParams.DRAFT.key(), false);
        search = workflowManager.search(studyFqn, query, QueryOptions.empty(), ownerToken);
        assertEquals(1, search.getNumResults());
        assertEquals("workflow", search.first().getId());
        assertFalse(search.first().isDraft());
    }

    @Test
    public void updateWorkflowTest() throws CatalogException {
        WorkflowCreateParams workflowCreateParams = new WorkflowCreateParams()
                .setId("workflow")
                .setScope(ExternalToolScope.OTHER)
                .setWorkflow(new Workflow().setScripts(Collections.singletonList(new WorkflowScript("pipeline.nf", "echo 'Hello world!'", true))));
        workflowManager.create(studyFqn, workflowCreateParams, INCLUDE_RESULT, ownerToken);

        ExternalToolUpdateParams updateParams = new ExternalToolUpdateParams()
                .setName("newName")
                .setScope(ExternalToolScope.OTHER)
                .setDraft(true)
                .setCreationDate("20240101000000")
                .setModificationDate("20240201000000")
                .setDescription("description");

        OpenCGAResult<ExternalTool> update = workflowManager.update(studyFqn, workflowCreateParams.getId(), updateParams, INCLUDE_RESULT, ownerToken);
        assertEquals(1, update.getNumUpdated());
        ExternalTool updatedExternalTool = update.first();
        assertEquals(updateParams.getName(), updatedExternalTool.getName());
        assertEquals(updateParams.isDraft(), updatedExternalTool.isDraft());
        assertEquals(updateParams.getCreationDate(), updatedExternalTool.getCreationDate());
        assertEquals(updateParams.getModificationDate(), updatedExternalTool.getModificationDate());
        assertEquals(updateParams.getDescription(), updatedExternalTool.getDescription());
    }

    @Test
    public void deleteWorkflowTest() throws CatalogException {
        WorkflowCreateParams workflowCreateParams = new WorkflowCreateParams()
                .setId("workflow")
                .setScope(ExternalToolScope.OTHER)
                .setWorkflow(new Workflow().setScripts(Collections.singletonList(new WorkflowScript("pipeline.nf", "echo 'Hello world!'", true))));
        workflowManager.create(studyFqn, workflowCreateParams, QueryOptions.empty(), ownerToken);

        OpenCGAResult<ExternalTool> result = workflowManager.delete(studyFqn, workflowCreateParams.getId(), QueryOptions.empty(), ownerToken);
        assertEquals(1, result.getNumDeleted());

        CatalogException exception = assertThrows(CatalogException.class,
                () -> workflowManager.get(studyFqn, workflowCreateParams.getId(), QueryOptions.empty(), ownerToken));
        assertTrue(exception.getMessage().contains("not found"));
    }

    @Test
    public void updateWorkflowAclTest() throws CatalogException {
        WorkflowCreateParams workflowCreateParams = new WorkflowCreateParams()
                .setId("workflow")
                .setScope(ExternalToolScope.OTHER)
                .setWorkflow(new Workflow().setScripts(Collections.singletonList(new WorkflowScript("pipeline.nf", "echo 'Hello world!'", true))));
        workflowManager.create(studyFqn, workflowCreateParams, QueryOptions.empty(), ownerToken);

        CatalogAuthorizationException catalogAuthorizationException = assertThrows(CatalogAuthorizationException.class,
                () -> workflowManager.get(studyFqn, workflowCreateParams.getId(), QueryOptions.empty(), noAccessToken1));
        assertTrue(catalogAuthorizationException.getMessage().contains("denied"));

        workflowManager.updateAcl(studyFqn, noAccessUserId1, new ExternalToolAclUpdateParams(Collections.singletonList(workflowCreateParams.getId()), Collections.singletonList("VIEW")),
                ParamUtils.AclAction.ADD, ownerToken);
        OpenCGAResult<ExternalTool> result = workflowManager.get(studyFqn, workflowCreateParams.getId(), QueryOptions.empty(), noAccessToken1);
        assertEquals(1, result.getNumResults());
    }

}
