package org.opencb.opencga.catalog.managers;

import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.opencb.commons.datastore.core.Query;
import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.opencga.catalog.db.api.WorkflowDBAdaptor;
import org.opencb.opencga.catalog.exceptions.CatalogAuthorizationException;
import org.opencb.opencga.catalog.exceptions.CatalogException;
import org.opencb.opencga.catalog.utils.ParamUtils;
import org.opencb.opencga.core.models.workflow.*;
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
    public void createWorkflowTest() throws CatalogException {
        Workflow workflow = new Workflow()
                .setId("workflow")
                .setType(Workflow.Type.OTHER)
                .setScripts(Collections.singletonList(new WorkflowScript("pipeline.nf", "echo 'Hello world!'", true)));
        OpenCGAResult<Workflow> result = workflowManager.create(studyFqn, workflow, INCLUDE_RESULT, ownerToken);
        assertEquals(1, result.getNumResults());
        assertNotNull(result.first());
        assertEquals(workflow.getId(), result.first().getId());

        // Add repository to workflow
        workflow.setId("workflow2");
        workflow.setRepository(new WorkflowRepository("blabla"));
        CatalogException catalogException = assertThrows(CatalogException.class,
                () -> workflowManager.create(studyFqn, workflow, INCLUDE_RESULT, ownerToken));
        assertTrue(catalogException.getMessage().contains("script") && catalogException.getMessage().contains("repository"));

        // Remove script from workflow
        workflow.setScripts(Collections.emptyList());
        result = workflowManager.create(studyFqn, workflow, INCLUDE_RESULT, ownerToken);
        assertEquals(1, result.getNumResults());
        assertNotNull(result.first());
        assertEquals(workflow.getId(), result.first().getId());

        // Remove script and add two scripts with 2 mains
        workflow.setId("workflow3");
        workflow.setRepository(null);
        workflow.setScripts(Arrays.asList(
                new WorkflowScript("script1", "echo 'Hello'", true),
                new WorkflowScript("script2", "echo 'World'", true)
        ));
        catalogException = assertThrows(CatalogException.class,
                () -> workflowManager.create(studyFqn, workflow, INCLUDE_RESULT, ownerToken));
        assertTrue(catalogException.getMessage().contains("script") && catalogException.getMessage().contains("main"));

        // Add one single script without main
        workflow.setScripts(Collections.singletonList(
                new WorkflowScript("script1", "echo 'Hello'", false)
        ));
        catalogException = assertThrows(CatalogException.class,
                () -> workflowManager.create(studyFqn, workflow, INCLUDE_RESULT, ownerToken));
        assertTrue(catalogException.getMessage().contains("script") && catalogException.getMessage().contains("main"));
    }

    @Test
    public void workflowSearchTest() throws CatalogException {
        Workflow workflow = new Workflow()
                .setId("workflow")
                .setType(Workflow.Type.OTHER)
                .setScripts(Collections.singletonList(new WorkflowScript("pipeline.nf", "echo 'Hello world!'", true)));
        workflowManager.create(studyFqn, workflow, INCLUDE_RESULT, ownerToken);

        workflow = new Workflow()
                .setId("workflow2")
                .setType(Workflow.Type.OTHER)
                .setDraft(true)
                .setScripts(Collections.singletonList(new WorkflowScript("pipeline.nf", "echo 'Hello world!'", true)));
        workflowManager.create(studyFqn, workflow, INCLUDE_RESULT, ownerToken);

        OpenCGAResult<Workflow> search = workflowManager.search(studyFqn, new Query(), QueryOptions.empty(), ownerToken);
        assertEquals(2, search.getNumResults());

        Query query = new Query(WorkflowDBAdaptor.QueryParams.DRAFT.key(), true);
        search = workflowManager.search(studyFqn, query, QueryOptions.empty(), ownerToken);
        assertEquals(1, search.getNumResults());
        assertEquals("workflow2", search.first().getId());
        assertTrue(search.first().isDraft());

        query = new Query(WorkflowDBAdaptor.QueryParams.DRAFT.key(), false);
        search = workflowManager.search(studyFqn, query, QueryOptions.empty(), ownerToken);
        assertEquals(1, search.getNumResults());
        assertEquals("workflow", search.first().getId());
        assertFalse(search.first().isDraft());
    }

    @Test
    public void updateWorkflowTest() throws CatalogException {
        Workflow workflow = new Workflow()
                .setId("workflow")
                .setType(Workflow.Type.OTHER)
                .setScripts(Collections.singletonList(new WorkflowScript("pipeline.nf", "echo 'Hello world!'", true)));
        workflowManager.create(studyFqn, workflow, INCLUDE_RESULT, ownerToken);

        WorkflowUpdateParams updateParams = new WorkflowUpdateParams()
                .setName("newName")
                .setType(Workflow.Type.OTHER)
                .setDraft(true)
                .setCreationDate("20240101000000")
                .setModificationDate("20240201000000")
                .setDescription("description");

        OpenCGAResult<Workflow> update = workflowManager.update(studyFqn, workflow.getId(), updateParams, INCLUDE_RESULT, ownerToken);
        assertEquals(1, update.getNumUpdated());
        Workflow updatedWorkflow = update.first();
        assertEquals(updateParams.getName(), updatedWorkflow.getName());
        assertEquals(updateParams.isDraft(), updatedWorkflow.isDraft());
        assertEquals(updateParams.getCreationDate(), updatedWorkflow.getCreationDate());
        assertEquals(updateParams.getModificationDate(), updatedWorkflow.getModificationDate());
        assertEquals(updateParams.getDescription(), updatedWorkflow.getDescription());
    }

    @Test
    public void deleteWorkflowTest() throws CatalogException {
        Workflow workflow = new Workflow()
                .setId("workflow")
                .setType(Workflow.Type.OTHER)
                .setScripts(Collections.singletonList(new WorkflowScript("pipeline.nf", "echo 'Hello world!'", true)));
        workflowManager.create(studyFqn, workflow, QueryOptions.empty(), ownerToken);

        OpenCGAResult<Workflow> result = workflowManager.delete(studyFqn, workflow.getId(), QueryOptions.empty(), ownerToken);
        assertEquals(1, result.getNumDeleted());

        CatalogException exception = assertThrows(CatalogException.class,
                () -> workflowManager.get(studyFqn, workflow.getId(), QueryOptions.empty(), ownerToken));
        assertTrue(exception.getMessage().contains("not found"));
    }

    @Test
    public void updateWorkflowAclTest() throws CatalogException {
        Workflow workflow = new Workflow()
                .setId("workflow")
                .setType(Workflow.Type.OTHER)
                .setScripts(Collections.singletonList(new WorkflowScript("pipeline.nf", "echo 'Hello world!'", true)));
        workflowManager.create(studyFqn, workflow, QueryOptions.empty(), ownerToken);

        CatalogAuthorizationException catalogAuthorizationException = assertThrows(CatalogAuthorizationException.class,
                () -> workflowManager.get(studyFqn, workflow.getId(), QueryOptions.empty(), noAccessToken1));
        assertTrue(catalogAuthorizationException.getMessage().contains("denied"));

        workflowManager.updateAcl(studyFqn, noAccessUserId1, new WorkflowAclUpdateParams(Collections.singletonList(workflow.getId()), Collections.singletonList("VIEW")),
                ParamUtils.AclAction.ADD, ownerToken);
        OpenCGAResult<Workflow> result = workflowManager.get(studyFqn, workflow.getId(), QueryOptions.empty(), noAccessToken1);
        assertEquals(1, result.getNumResults());
    }

}
