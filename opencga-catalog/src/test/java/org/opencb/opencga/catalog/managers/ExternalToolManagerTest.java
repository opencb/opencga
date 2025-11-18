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
import org.opencb.opencga.core.api.ParamConstants;
import org.opencb.opencga.core.models.AclEntryList;
import org.opencb.opencga.core.models.externalTool.*;
import org.opencb.opencga.core.models.externalTool.custom.CustomToolCreateParams;
import org.opencb.opencga.core.models.externalTool.custom.CustomToolUpdateParams;
import org.opencb.opencga.core.models.externalTool.workflow.WorkflowCreateParams;
import org.opencb.opencga.core.models.externalTool.workflow.WorkflowUpdateParams;
import org.opencb.opencga.core.models.job.MinimumRequirements;
import org.opencb.opencga.core.response.OpenCGAResult;
import org.opencb.opencga.core.testclassification.duration.MediumTests;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.*;

@Category(MediumTests.class)
public class ExternalToolManagerTest extends AbstractManagerTest {

    private ExternalToolManager externalToolManager;

    @Before
    public void setUp() throws Exception {
        super.setUp();
        externalToolManager = catalogManager.getExternalToolManager();
    }

    @Test
    public void importWorkflow() throws CatalogException {
        WorkflowRepositoryParams params = new WorkflowRepositoryParams("nf-core/rnaseq");
        OpenCGAResult<ExternalTool> result = externalToolManager.importWorkflow(studyFqn, params, INCLUDE_RESULT, ownerToken);
        assertEquals(1, result.getNumInserted());
        assertEquals(1, result.first().getVersion());

        // Update imported workflow
        result = externalToolManager.importWorkflow(studyFqn, params, INCLUDE_RESULT, ownerToken);
        assertEquals(2, result.first().getVersion());

        params = new WorkflowRepositoryParams("nf-core/proteinfold");
        result = externalToolManager.importWorkflow(studyFqn, params, INCLUDE_RESULT, ownerToken);
        assertEquals(1, result.getNumInserted());
        assertEquals(1, result.first().getVersion());

        params = new WorkflowRepositoryParams("nf-core/methylseq");
        result = externalToolManager.importWorkflow(studyFqn, params, INCLUDE_RESULT, ownerToken);
        assertEquals(1, result.getNumInserted());
        assertEquals(1, result.first().getVersion());

        params = new WorkflowRepositoryParams("nf-core/pacvar");
        result = externalToolManager.importWorkflow(studyFqn, params, INCLUDE_RESULT, ownerToken);
        assertEquals(1, result.getNumInserted());
        assertEquals(1, result.first().getVersion());
    }

    @Test
    public void createWorkflowTest() throws CatalogException {
        WorkflowCreateParams workflowCreateParams = new WorkflowCreateParams()
                .setId("workflow")
                .setScope(ExternalToolScope.OTHER)
                .setWorkflow(new Workflow().setScripts(Collections.singletonList(new WorkflowScript("pipeline.nf", "echo 'Hello world!'", true))));
        OpenCGAResult<ExternalTool> result = externalToolManager.createWorkflow(studyFqn, workflowCreateParams, INCLUDE_RESULT, ownerToken);
        assertEquals(1, result.getNumResults());
        assertNotNull(result.first());
        assertEquals(workflowCreateParams.getId(), result.first().getId());

        // Add repository to workflow
        workflowCreateParams.setId("workflow2");
        workflowCreateParams.getWorkflow().setRepository(new WorkflowRepository("blabla"));
        CatalogException catalogException = assertThrows(CatalogException.class,
                () -> externalToolManager.createWorkflow(studyFqn, workflowCreateParams, INCLUDE_RESULT, ownerToken));
        assertTrue(catalogException.getMessage().contains("script") && catalogException.getMessage().contains("repository"));

        // Remove script from workflow
        workflowCreateParams.getWorkflow().setScripts(Collections.emptyList());
        result = externalToolManager.createWorkflow(studyFqn, workflowCreateParams, INCLUDE_RESULT, ownerToken);
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
                () -> externalToolManager.createWorkflow(studyFqn, workflowCreateParams, INCLUDE_RESULT, ownerToken));
        assertTrue(catalogException.getMessage().contains("script") && catalogException.getMessage().contains("main"));

        // Add one single script without main
        workflowCreateParams.getWorkflow().setScripts(Collections.singletonList(
                new WorkflowScript("script1", "echo 'Hello'", false)
        ));
        catalogException = assertThrows(CatalogException.class,
                () -> externalToolManager.createWorkflow(studyFqn, workflowCreateParams, INCLUDE_RESULT, ownerToken));
        assertTrue(catalogException.getMessage().contains("script") && catalogException.getMessage().contains("main"));
    }

    @Test
    public void workflowSearchTest() throws CatalogException {
        WorkflowCreateParams workflowCreateParams = new WorkflowCreateParams()
                .setId("workflow")
                .setScope(ExternalToolScope.OTHER)
                .setWorkflow(new Workflow().setScripts(Collections.singletonList(new WorkflowScript("pipeline.nf", "echo 'Hello world!'", true))));
        externalToolManager.createWorkflow(studyFqn, workflowCreateParams, INCLUDE_RESULT, ownerToken);

        workflowCreateParams = new WorkflowCreateParams()
                .setId("workflow2")
                .setScope(ExternalToolScope.OTHER)
                .setDraft(true)
                .setWorkflow(new Workflow().setScripts(Collections.singletonList(new WorkflowScript("pipeline.nf", "echo 'Hello world!'", true))));
        externalToolManager.createWorkflow(studyFqn, workflowCreateParams, INCLUDE_RESULT, ownerToken);

        OpenCGAResult<ExternalTool> search = externalToolManager.search(studyFqn, new Query(), QueryOptions.empty(), ownerToken);
        assertEquals(2, search.getNumResults());

        Query query = new Query(ExternalToolDBAdaptor.QueryParams.DRAFT.key(), true);
        search = externalToolManager.search(studyFqn, query, QueryOptions.empty(), ownerToken);
        assertEquals(1, search.getNumResults());
        assertEquals("workflow2", search.first().getId());
        assertTrue(search.first().isDraft());

        query = new Query(ExternalToolDBAdaptor.QueryParams.DRAFT.key(), false);
        search = externalToolManager.search(studyFqn, query, QueryOptions.empty(), ownerToken);
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
        externalToolManager.createWorkflow(studyFqn, workflowCreateParams, INCLUDE_RESULT, ownerToken);

        WorkflowUpdateParams updateParams = new WorkflowUpdateParams()
                .setName("newName")
                .setScope(ExternalToolScope.OTHER)
                .setDraft(true)
                .setCreationDate("20240101000000")
                .setModificationDate("20240201000000")
                .setDescription("description");

        OpenCGAResult<ExternalTool> update = externalToolManager.updateWorkflow(studyFqn, workflowCreateParams.getId(), updateParams, INCLUDE_RESULT, ownerToken);
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
        externalToolManager.createWorkflow(studyFqn, workflowCreateParams, QueryOptions.empty(), ownerToken);

        OpenCGAResult<ExternalTool> result = externalToolManager.delete(studyFqn, workflowCreateParams.getId(), QueryOptions.empty(), ownerToken);
        assertEquals(1, result.getNumDeleted());

        CatalogException exception = assertThrows(CatalogException.class,
                () -> externalToolManager.get(studyFqn, workflowCreateParams.getId(), QueryOptions.empty(), ownerToken));
        assertTrue(exception.getMessage().contains("not found"));
    }

    @Test
    public void updateWorkflowAclTest() throws CatalogException {
        WorkflowCreateParams workflowCreateParams = new WorkflowCreateParams()
                .setId("workflow")
                .setScope(ExternalToolScope.OTHER)
                .setWorkflow(new Workflow().setScripts(Collections.singletonList(new WorkflowScript("pipeline.nf", "echo 'Hello world!'", true))));
        externalToolManager.createWorkflow(studyFqn, workflowCreateParams, QueryOptions.empty(), ownerToken);

        CatalogAuthorizationException catalogAuthorizationException = assertThrows(CatalogAuthorizationException.class,
                () -> externalToolManager.get(studyFqn, workflowCreateParams.getId(), QueryOptions.empty(), noAccessToken1));
        assertTrue(catalogAuthorizationException.getMessage().contains("denied"));

        externalToolManager.updateAcl(studyFqn, noAccessUserId1, new ExternalToolAclUpdateParams(Collections.singletonList(workflowCreateParams.getId()), Collections.singletonList("VIEW")),
                ParamUtils.AclAction.ADD, ownerToken);
        OpenCGAResult<ExternalTool> result = externalToolManager.get(studyFqn, workflowCreateParams.getId(), QueryOptions.empty(), noAccessToken1);
        assertEquals(1, result.getNumResults());
    }

    @Test
    public void getWorkflowAclTest() throws CatalogException {
        WorkflowCreateParams workflowCreateParams = new WorkflowCreateParams()
                .setId("workflow")
                .setScope(ExternalToolScope.OTHER)
                .setWorkflow(new Workflow().setScripts(Collections.singletonList(new WorkflowScript("pipeline.nf", "echo 'Hello world!'", true))));
        externalToolManager.createWorkflow(studyFqn, workflowCreateParams, QueryOptions.empty(), ownerToken);

        // Add ACL for user
        externalToolManager.updateAcl(studyFqn, noAccessUserId1, new ExternalToolAclUpdateParams(Collections.singletonList(workflowCreateParams.getId()), Collections.singletonList("VIEW")),
                ParamUtils.AclAction.ADD, ownerToken);

        // Get ACLs
        OpenCGAResult<AclEntryList<ExternalToolPermissions>> acls = externalToolManager.getAcls(studyFqn, Collections.singletonList(workflowCreateParams.getId()), noAccessUserId1, false, ownerToken);
        assertEquals(1, acls.getNumResults());
        assertFalse(acls.first().getAcl().isEmpty());
    }

    @Test
    public void removeWorkflowAclTest() throws CatalogException {
        WorkflowCreateParams workflowCreateParams = new WorkflowCreateParams()
                .setId("workflow")
                .setScope(ExternalToolScope.OTHER)
                .setWorkflow(new Workflow().setScripts(Collections.singletonList(new WorkflowScript("pipeline.nf", "echo 'Hello world!'", true))));
        externalToolManager.createWorkflow(studyFqn, workflowCreateParams, QueryOptions.empty(), ownerToken);

        // Add ACL for user
        externalToolManager.updateAcl(studyFqn, noAccessUserId1, new ExternalToolAclUpdateParams(Collections.singletonList(workflowCreateParams.getId()), Collections.singletonList("VIEW")),
                ParamUtils.AclAction.ADD, ownerToken);

        OpenCGAResult<ExternalTool> result = externalToolManager.get(studyFqn, workflowCreateParams.getId(), QueryOptions.empty(), noAccessToken1);
        assertEquals(1, result.getNumResults());

        // Remove ACL
        externalToolManager.updateAcl(studyFqn, noAccessUserId1, new ExternalToolAclUpdateParams(Collections.singletonList(workflowCreateParams.getId()), Collections.singletonList("VIEW")),
                ParamUtils.AclAction.REMOVE, ownerToken);

        CatalogAuthorizationException catalogAuthorizationException = assertThrows(CatalogAuthorizationException.class,
                () -> externalToolManager.get(studyFqn, workflowCreateParams.getId(), QueryOptions.empty(), noAccessToken1));
        assertTrue(catalogAuthorizationException.getMessage().contains("denied"));
    }

    @Test
    public void createCustomToolTest() throws CatalogException {
        CustomToolCreateParams toolCreateParams = new CustomToolCreateParams()
                .setId("customTool")
                .setName("My Custom Tool")
                .setScope(ExternalToolScope.OTHER)
                .setContainer(new Container("myimage", "1.0", "", "", "", ""));
        OpenCGAResult<ExternalTool> result = externalToolManager.createCustomTool(studyFqn, toolCreateParams, INCLUDE_RESULT, ownerToken);
        assertEquals(1, result.getNumResults());
        assertNotNull(result.first());
        assertEquals(toolCreateParams.getId(), result.first().getId());
        assertEquals(ExternalToolType.CUSTOM_TOOL, result.first().getType());
        assertNotNull(result.first().getContainer());
        assertNull(result.first().getWorkflow());

        // Validate required container
        CustomToolCreateParams invalidParams = new CustomToolCreateParams()
                .setId("customTool2")
                .setScope(ExternalToolScope.OTHER);
        CatalogException catalogException = assertThrows(CatalogException.class,
                () -> externalToolManager.createCustomTool(studyFqn, invalidParams, INCLUDE_RESULT, ownerToken));
        assertTrue(catalogException.getMessage().contains("container"));
    }

    @Test
    public void updateCustomToolTest() throws CatalogException {
        CustomToolCreateParams toolCreateParams = new CustomToolCreateParams()
                .setId("customTool")
                .setScope(ExternalToolScope.OTHER)
                .setContainer(new Container("myimage", "1.0", "", "", "", ""));
        externalToolManager.createCustomTool(studyFqn, toolCreateParams, QueryOptions.empty(), ownerToken);

        CustomToolUpdateParams updateParams = new CustomToolUpdateParams()
                .setName("Updated Custom Tool")
                .setDescription("Updated description")
                .setContainer(new Container("myimage", "2.0", "", "", "", ""));

        OpenCGAResult<ExternalTool> update = externalToolManager.updateCustomTool(studyFqn, toolCreateParams.getId(), updateParams, INCLUDE_RESULT, ownerToken);
        assertEquals(1, update.getNumUpdated());
        ExternalTool updatedTool = update.first();
        assertEquals(updateParams.getName(), updatedTool.getName());
        assertEquals(updateParams.getDescription(), updatedTool.getDescription());
        assertEquals("2.0", updatedTool.getContainer().getTag());
    }

    @Test
    public void createVariantWalkerToolTest() throws CatalogException {
        CustomToolCreateParams toolCreateParams = new CustomToolCreateParams()
                .setId("variantWalker")
                .setName("My Variant Walker")
                .setScope(ExternalToolScope.OTHER)
                .setContainer(new Container("walkerimage", "1.0", "", "", "", ""));
        OpenCGAResult<ExternalTool> result = externalToolManager.createVariantWalkerTool(studyFqn, toolCreateParams, INCLUDE_RESULT, ownerToken);
        assertEquals(1, result.getNumResults());
        assertNotNull(result.first());
        assertEquals(toolCreateParams.getId(), result.first().getId());
        assertEquals(ExternalToolType.VARIANT_WALKER, result.first().getType());
        assertNotNull(result.first().getContainer());
        assertNull(result.first().getWorkflow());
    }

    @Test
    public void updateVariantWalkerToolTest() throws CatalogException {
        CustomToolCreateParams toolCreateParams = new CustomToolCreateParams()
                .setId("variantWalker")
                .setScope(ExternalToolScope.OTHER)
                .setContainer(new Container("walkerimage", "1.0", "", "", "", ""));
        externalToolManager.createVariantWalkerTool(studyFqn, toolCreateParams, QueryOptions.empty(), ownerToken);

        CustomToolUpdateParams updateParams = new CustomToolUpdateParams()
                .setName("Updated Variant Walker")
                .setDescription("Updated variant walker description")
                .setDraft(true)
                .setTags(Arrays.asList("updated", "variant-walker"))
                .setContainer(new Container("walkerimage", "2.0", "sha256:abc123", "", "", ""));

        OpenCGAResult<ExternalTool> update = externalToolManager.updateCustomTool(studyFqn, toolCreateParams.getId(), updateParams, INCLUDE_RESULT, ownerToken);
        assertEquals(1, update.getNumUpdated());
        ExternalTool updatedTool = update.first();
        assertEquals(updateParams.getName(), updatedTool.getName());
        assertEquals(updateParams.getDescription(), updatedTool.getDescription());
        assertTrue(updatedTool.isDraft());
        assertEquals("2.0", updatedTool.getContainer().getTag());
        assertEquals("sha256:abc123", updatedTool.getContainer().getDigest());
        assertTrue(updatedTool.getTags().contains("updated"));
        assertTrue(updatedTool.getTags().contains("variant-walker"));
        // Verify it's still a VARIANT_WALKER type
        assertEquals(ExternalToolType.VARIANT_WALKER, updatedTool.getType());
    }

    // ====================================
    // Enhanced Update Tests for All Tool Types
    // ====================================

    @Test
    public void updateWorkflowScriptsTest() throws CatalogException {
        WorkflowCreateParams workflowParams = new WorkflowCreateParams()
                .setId("workflow-scripts")
                .setScope(ExternalToolScope.OTHER)
                .setWorkflow(new Workflow().setScripts(Collections.singletonList(
                        new WorkflowScript("pipeline.nf", "echo 'original'", true))));
        externalToolManager.createWorkflow(studyFqn, workflowParams, QueryOptions.empty(), ownerToken);

        // Update scripts
        WorkflowUpdateParams updateParams = new WorkflowUpdateParams()
                .setWorkflow(new Workflow().setScripts(Arrays.asList(
                        new WorkflowScript("pipeline.nf", "echo 'updated'", true),
                        new WorkflowScript("utils.nf", "echo 'utilities'", false)
                )));

        OpenCGAResult<ExternalTool> update = externalToolManager.updateWorkflow(studyFqn, "workflow-scripts", updateParams, INCLUDE_RESULT, ownerToken);
        assertEquals(1, update.getNumUpdated());
        ExternalTool updatedTool = update.first();
        assertEquals(2, updatedTool.getWorkflow().getScripts().size());
        assertTrue(updatedTool.getWorkflow().getScripts().stream().anyMatch(s -> s.getName().equals("pipeline.nf") && s.isMain()));
        assertTrue(updatedTool.getWorkflow().getScripts().stream().anyMatch(s -> s.getName().equals("utils.nf") && !s.isMain()));
    }

    @Test
    public void updateWorkflowRepositoryTest() throws CatalogException {
        WorkflowCreateParams workflowParams = new WorkflowCreateParams()
                .setId("workflow-repo")
                .setScope(ExternalToolScope.OTHER)
                .setWorkflow(new Workflow().setRepository(new WorkflowRepository("github.com/user/repo1")));
        externalToolManager.createWorkflow(studyFqn, workflowParams, QueryOptions.empty(), ownerToken);

        // Update repository
        WorkflowUpdateParams updateParams = new WorkflowUpdateParams()
                .setWorkflow(new Workflow().setRepository(new WorkflowRepository("github.com/user/repo2")
                        .setTag("v2.0")
                        .setDescription("Updated repository")
                        .setAuthor("New Author")));

        OpenCGAResult<ExternalTool> update = externalToolManager.updateWorkflow(studyFqn, "workflow-repo", updateParams, INCLUDE_RESULT, ownerToken);
        assertEquals(1, update.getNumUpdated());
        ExternalTool updatedTool = update.first();
        assertEquals("github.com/user/repo2", updatedTool.getWorkflow().getRepository().getName());
        assertEquals("v2.0", updatedTool.getWorkflow().getRepository().getTag());
        assertEquals("Updated repository", updatedTool.getWorkflow().getRepository().getDescription());
        assertEquals("New Author", updatedTool.getWorkflow().getRepository().getAuthor());
    }

    @Test
    public void updateWorkflowManagerTest() throws CatalogException {
        WorkflowCreateParams workflowParams = new WorkflowCreateParams()
                .setId("workflow-manager")
                .setScope(ExternalToolScope.OTHER)
                .setWorkflow(new Workflow().setScripts(Collections.singletonList(
                        new WorkflowScript("pipeline.nf", "echo 'test'", true))));
        externalToolManager.createWorkflow(studyFqn, workflowParams, QueryOptions.empty(), ownerToken);

        // Update workflow manager
        WorkflowUpdateParams updateParams = new WorkflowUpdateParams()
                .setWorkflow(new Workflow().setManager(new WorkflowSystem()
                        .setId(WorkflowSystem.SystemId.NEXTFLOW)
                        .setVersion("23.04.0")));

        OpenCGAResult<ExternalTool> update = externalToolManager.updateWorkflow(studyFqn, "workflow-manager", updateParams, INCLUDE_RESULT, ownerToken);
        assertEquals(1, update.getNumUpdated());
        ExternalTool updatedTool = update.first();
        assertEquals(WorkflowSystem.SystemId.NEXTFLOW, updatedTool.getWorkflow().getManager().getId());
        assertEquals("23.04.0", updatedTool.getWorkflow().getManager().getVersion());
    }

    @Test
    public void updateWorkflowTagsTest() throws CatalogException {
        WorkflowCreateParams workflowParams = new WorkflowCreateParams()
                .setId("workflow-tags")
                .setScope(ExternalToolScope.OTHER)
                .setTags(Arrays.asList("original", "tag1"))
                .setWorkflow(new Workflow().setScripts(Collections.singletonList(
                        new WorkflowScript("pipeline.nf", "echo 'test'", true))));
        externalToolManager.createWorkflow(studyFqn, workflowParams, QueryOptions.empty(), ownerToken);

        // Update tags
        WorkflowUpdateParams updateParams = new WorkflowUpdateParams()
                .setTags(Arrays.asList("updated", "tag2", "tag3"));

        OpenCGAResult<ExternalTool> update = externalToolManager.updateWorkflow(studyFqn, "workflow-tags", updateParams, INCLUDE_RESULT, ownerToken);
        assertEquals(1, update.getNumUpdated());
        ExternalTool updatedTool = update.first();
        assertEquals(3, updatedTool.getTags().size());
        assertTrue(updatedTool.getTags().contains("updated"));
        assertTrue(updatedTool.getTags().contains("tag2"));
        assertTrue(updatedTool.getTags().contains("tag3"));
        assertFalse(updatedTool.getTags().contains("original"));
    }

    @Test
    public void updateWorkflowScopeTest() throws CatalogException {
        WorkflowCreateParams workflowParams = new WorkflowCreateParams()
                .setId("workflow-scope")
                .setScope(ExternalToolScope.OTHER)
                .setWorkflow(new Workflow().setScripts(Collections.singletonList(
                        new WorkflowScript("pipeline.nf", "echo 'test'", true))));
        externalToolManager.createWorkflow(studyFqn, workflowParams, QueryOptions.empty(), ownerToken);

        // Update scope
        WorkflowUpdateParams updateParams = new WorkflowUpdateParams()
                .setScope(ExternalToolScope.SECONDARY_ANALYSIS);

        OpenCGAResult<ExternalTool> update = externalToolManager.updateWorkflow(studyFqn, "workflow-scope", updateParams, INCLUDE_RESULT, ownerToken);
        assertEquals(1, update.getNumUpdated());
        assertEquals(ExternalToolScope.SECONDARY_ANALYSIS, update.first().getScope());
    }

    @Test
    public void updateWorkflowDraftStatusTest() throws CatalogException {
        WorkflowCreateParams workflowParams = new WorkflowCreateParams()
                .setId("workflow-draft")
                .setScope(ExternalToolScope.OTHER)
                .setDraft(true)
                .setWorkflow(new Workflow().setScripts(Collections.singletonList(
                        new WorkflowScript("pipeline.nf", "echo 'test'", true))));
        externalToolManager.createWorkflow(studyFqn, workflowParams, QueryOptions.empty(), ownerToken);

        // Publish the workflow (set draft to false)
        WorkflowUpdateParams updateParams = new WorkflowUpdateParams().setDraft(false);

        OpenCGAResult<ExternalTool> update = externalToolManager.updateWorkflow(studyFqn, "workflow-draft", updateParams, INCLUDE_RESULT, ownerToken);
        assertEquals(1, update.getNumUpdated());
        assertFalse(update.first().isDraft());

        // Set back to draft
        updateParams = new WorkflowUpdateParams().setDraft(true);
        update = externalToolManager.updateWorkflow(studyFqn, "workflow-draft", updateParams, INCLUDE_RESULT, ownerToken);
        assertTrue(update.first().isDraft());
    }

    @Test
    public void updateCustomToolContainerTest() throws CatalogException {
        CustomToolCreateParams toolParams = new CustomToolCreateParams()
                .setId("custom-container-update")
                .setScope(ExternalToolScope.OTHER)
                .setContainer(new Container("myimage", "1.0", "", "", "", ""));
        externalToolManager.createCustomTool(studyFqn, toolParams, QueryOptions.empty(), ownerToken);

        // Update container with full details
        CustomToolUpdateParams updateParams = new CustomToolUpdateParams()
                .setContainer(new Container(
                        "docker.io/myorg/myimage",
                        "2.0.1",
                        "sha256:abcdef123456",
                        "docker run myimage",
                        "myuser",
                        "mypassword"
                ));

        OpenCGAResult<ExternalTool> update = externalToolManager.updateCustomTool(studyFqn, "custom-container-update", updateParams, INCLUDE_RESULT, ownerToken);
        assertEquals(1, update.getNumUpdated());
        Container updatedContainer = update.first().getContainer();
        assertEquals("docker.io/myorg/myimage", updatedContainer.getName());
        assertEquals("2.0.1", updatedContainer.getTag());
        assertEquals("sha256:abcdef123456", updatedContainer.getDigest());
        assertEquals("docker run myimage", updatedContainer.getCommandLine());
        assertEquals("myuser", updatedContainer.getUser());
        assertEquals("mypassword", updatedContainer.getPassword());
    }

    @Test
    public void updateCustomToolMultipleFieldsTest() throws CatalogException {
        CustomToolCreateParams toolParams = new CustomToolCreateParams()
                .setId("custom-multi-update")
                .setName("Original Name")
                .setScope(ExternalToolScope.OTHER)
                .setDraft(false)
                .setContainer(new Container("myimage", "1.0", "", "", "", ""));
        externalToolManager.createCustomTool(studyFqn, toolParams, QueryOptions.empty(), ownerToken);

        // Update multiple fields at once
        CustomToolUpdateParams updateParams = new CustomToolUpdateParams()
                .setName("Updated Name")
                .setDescription("Comprehensive update")
                .setScope(ExternalToolScope.RESEARCH_ANALYSIS)
                .setDraft(true)
                .setTags(Arrays.asList("updated", "comprehensive", "test"))
                .setContainer(new Container("myimage", "3.0", "sha256:xyz", "", "", ""));

        OpenCGAResult<ExternalTool> update = externalToolManager.updateCustomTool(studyFqn, "custom-multi-update", updateParams, INCLUDE_RESULT, ownerToken);
        assertEquals(1, update.getNumUpdated());
        ExternalTool updatedTool = update.first();
        assertEquals("Updated Name", updatedTool.getName());
        assertEquals("Comprehensive update", updatedTool.getDescription());
        assertEquals(ExternalToolScope.RESEARCH_ANALYSIS, updatedTool.getScope());
        assertTrue(updatedTool.isDraft());
        assertEquals(3, updatedTool.getTags().size());
        assertEquals("3.0", updatedTool.getContainer().getTag());
    }

    @Test
    public void updateVariantWalkerMultipleFieldsTest() throws CatalogException {
        CustomToolCreateParams toolParams = new CustomToolCreateParams()
                .setId("walker-multi-update")
                .setName("Original Walker")
                .setScope(ExternalToolScope.OTHER)
                .setContainer(new Container("walkerimage", "1.0", "", "", "", ""));
        externalToolManager.createVariantWalkerTool(studyFqn, toolParams, QueryOptions.empty(), ownerToken);

        // Update multiple fields
        CustomToolUpdateParams updateParams = new CustomToolUpdateParams()
                .setName("Advanced Variant Walker")
                .setDescription("Enhanced variant analysis tool")
                .setScope(ExternalToolScope.CLINICAL_INTERPRETATION_ANALYSIS)
                .setDraft(false)
                .setTags(Arrays.asList("variant-analysis", "clinical", "walker"))
                .setContainer(new Container("walkerimage", "5.0", "", "walker --input ${INPUT}", "", ""));

        OpenCGAResult<ExternalTool> update = externalToolManager.updateCustomTool(studyFqn, "walker-multi-update", updateParams, INCLUDE_RESULT, ownerToken);
        assertEquals(1, update.getNumUpdated());
        ExternalTool updatedTool = update.first();
        assertEquals("Advanced Variant Walker", updatedTool.getName());
        assertEquals("Enhanced variant analysis tool", updatedTool.getDescription());
        assertEquals(ExternalToolScope.CLINICAL_INTERPRETATION_ANALYSIS, updatedTool.getScope());
        assertFalse(updatedTool.isDraft());
        assertEquals(3, updatedTool.getTags().size());
        assertEquals("5.0", updatedTool.getContainer().getTag());
        assertEquals("walker --input ${INPUT}", updatedTool.getContainer().getCommandLine());
        // Verify it's still a VARIANT_WALKER type
        assertEquals(ExternalToolType.VARIANT_WALKER, updatedTool.getType());
    }

    @Test
    public void updateWorkflowAttributesTest() throws CatalogException {
        WorkflowCreateParams workflowParams = new WorkflowCreateParams()
                .setId("workflow-attributes")
                .setScope(ExternalToolScope.OTHER)
                .setAttributes(Collections.singletonMap("key1", "value1"))
                .setWorkflow(new Workflow().setScripts(Collections.singletonList(
                        new WorkflowScript("pipeline.nf", "echo 'test'", true))));
        externalToolManager.createWorkflow(studyFqn, workflowParams, QueryOptions.empty(), ownerToken);

        // Update attributes
        Map<String, Object> newAttributes = new HashMap<>();
        newAttributes.put("key2", "value2");
        newAttributes.put("key3", Arrays.asList("item1", "item2"));
        newAttributes.put("nested", Collections.singletonMap("subkey", "subvalue"));

        WorkflowUpdateParams updateParams = new WorkflowUpdateParams()
                .setAttributes(newAttributes);

        OpenCGAResult<ExternalTool> update = externalToolManager.updateWorkflow(studyFqn, "workflow-attributes", updateParams, INCLUDE_RESULT, ownerToken);
        assertEquals(1, update.getNumUpdated());
        Map<String, Object> updatedAttributes = update.first().getAttributes();
        assertEquals("value2", updatedAttributes.get("key2"));
        assertTrue(updatedAttributes.containsKey("key3"));
        assertTrue(updatedAttributes.containsKey("nested"));
    }

    @Test
    public void updateToolMinimumRequirementsTest() throws CatalogException {
        WorkflowCreateParams workflowParams = new WorkflowCreateParams()
                .setId("workflow-requirements")
                .setScope(ExternalToolScope.OTHER)
                .setWorkflow(new Workflow().setScripts(Collections.singletonList(
                        new WorkflowScript("pipeline.nf", "echo 'test'", true))));
        externalToolManager.createWorkflow(studyFqn, workflowParams, QueryOptions.empty(), ownerToken);

        // Update minimum requirements
        MinimumRequirements requirements = new MinimumRequirements()
                .setCpu("8")
                .setMemory("32GB")
                .setDisk("500GB");

        WorkflowUpdateParams updateParams = new WorkflowUpdateParams()
                .setMinimumRequirements(requirements);

        OpenCGAResult<ExternalTool> update = externalToolManager.updateWorkflow(studyFqn, "workflow-requirements", updateParams, INCLUDE_RESULT, ownerToken);
        assertEquals(1, update.getNumUpdated());
        MinimumRequirements updatedReqs = update.first().getMinimumRequirements();
        assertEquals("8", updatedReqs.getCpu());
        assertEquals("32GB", updatedReqs.getMemory());
        assertEquals("500GB", updatedReqs.getDisk());
    }

    @Test
    public void createWorkflowWithValidMinimumRequirementsGBTest() throws CatalogException {
        // Test with GB unit directly attached
        WorkflowCreateParams workflowParams = new WorkflowCreateParams()
                .setId("workflow-req-gb")
                .setScope(ExternalToolScope.OTHER)
                .setMinimumRequirements(new MinimumRequirements().setCpu("4").setMemory("16GB").setDisk("100GB"))
                .setWorkflow(new Workflow().setScripts(Collections.singletonList(
                        new WorkflowScript("pipeline.nf", "echo 'test'", true))));
        OpenCGAResult<ExternalTool> result = externalToolManager.createWorkflow(studyFqn, workflowParams, INCLUDE_RESULT, ownerToken);
        assertEquals(1, result.getNumResults());
        MinimumRequirements reqs = result.first().getMinimumRequirements();
        assertEquals("4", reqs.getCpu());
        assertEquals("16GB", reqs.getMemory());
        assertEquals("100GB", reqs.getDisk());
    }

    @Test
    public void createWorkflowWithValidMinimumRequirementsMBTest() throws CatalogException {
        // Test with MB unit directly attached
        WorkflowCreateParams workflowParams = new WorkflowCreateParams()
                .setId("workflow-req-mb")
                .setScope(ExternalToolScope.OTHER)
                .setMinimumRequirements(new MinimumRequirements().setCpu("2").setMemory("512MB"))
                .setWorkflow(new Workflow().setScripts(Collections.singletonList(
                        new WorkflowScript("pipeline.nf", "echo 'test'", true))));
        OpenCGAResult<ExternalTool> result = externalToolManager.createWorkflow(studyFqn, workflowParams, INCLUDE_RESULT, ownerToken);
        assertEquals(1, result.getNumResults());
        MinimumRequirements reqs = result.first().getMinimumRequirements();
        assertEquals("2", reqs.getCpu());
        assertEquals("512MB", reqs.getMemory());
    }

    @Test
    public void createWorkflowWithMemoryNoUnitTest() throws CatalogException {
        // Test memory without unit - should automatically add GB
        WorkflowCreateParams workflowParams = new WorkflowCreateParams()
                .setId("workflow-req-no-unit")
                .setScope(ExternalToolScope.OTHER)
                .setMinimumRequirements(new MinimumRequirements().setCpu("8").setMemory("32"))
                .setWorkflow(new Workflow().setScripts(Collections.singletonList(
                        new WorkflowScript("pipeline.nf", "echo 'test'", true))));
        OpenCGAResult<ExternalTool> result = externalToolManager.createWorkflow(studyFqn, workflowParams, INCLUDE_RESULT, ownerToken);
        assertEquals(1, result.getNumResults());
        MinimumRequirements reqs = result.first().getMinimumRequirements();
        assertEquals("8", reqs.getCpu());
        assertEquals("32.0GB", reqs.getMemory());  // Should have GB added automatically
    }

    @Test
    public void createWorkflowWithMemorySpaceSeparatorTest() throws CatalogException {
        // Test memory with space separator
        WorkflowCreateParams workflowParams = new WorkflowCreateParams()
                .setId("workflow-req-space")
                .setScope(ExternalToolScope.OTHER)
                .setMinimumRequirements(new MinimumRequirements().setCpu("4").setMemory("16 GB"))
                .setWorkflow(new Workflow().setScripts(Collections.singletonList(
                        new WorkflowScript("pipeline.nf", "echo 'test'", true))));
        OpenCGAResult<ExternalTool> result = externalToolManager.createWorkflow(studyFqn, workflowParams, INCLUDE_RESULT, ownerToken);
        assertEquals(1, result.getNumResults());
        MinimumRequirements reqs = result.first().getMinimumRequirements();
        assertEquals("4", reqs.getCpu());
        assertEquals("16 GB", reqs.getMemory());
    }

    @Test
    public void createWorkflowWithMemoryDotSeparatorTest() throws CatalogException {
        // Test memory with dot separator
        WorkflowCreateParams workflowParams = new WorkflowCreateParams()
                .setId("workflow-req-dot")
                .setScope(ExternalToolScope.OTHER)
                .setMinimumRequirements(new MinimumRequirements().setCpu("2").setMemory("8.GB"))
                .setWorkflow(new Workflow().setScripts(Collections.singletonList(
                        new WorkflowScript("pipeline.nf", "echo 'test'", true))));
        OpenCGAResult<ExternalTool> result = externalToolManager.createWorkflow(studyFqn, workflowParams, INCLUDE_RESULT, ownerToken);
        assertEquals(1, result.getNumResults());
        MinimumRequirements reqs = result.first().getMinimumRequirements();
        assertEquals("2", reqs.getCpu());
        assertEquals("8.GB", reqs.getMemory());
    }

    @Test
    public void createWorkflowWithDecimalCpuTest() throws CatalogException {
        // Test with decimal CPU value
        WorkflowCreateParams workflowParams = new WorkflowCreateParams()
                .setId("workflow-req-decimal")
                .setScope(ExternalToolScope.OTHER)
                .setMinimumRequirements(new MinimumRequirements().setCpu("2.5").setMemory("16GB"))
                .setWorkflow(new Workflow().setScripts(Collections.singletonList(
                        new WorkflowScript("pipeline.nf", "echo 'test'", true))));
        OpenCGAResult<ExternalTool> result = externalToolManager.createWorkflow(studyFqn, workflowParams, INCLUDE_RESULT, ownerToken);
        assertEquals(1, result.getNumResults());
        MinimumRequirements reqs = result.first().getMinimumRequirements();
        assertEquals("2.5", reqs.getCpu());
        assertEquals("16GB", reqs.getMemory());
    }

    @Test
    public void createWorkflowWithDecimalMemoryValueTest() throws CatalogException {
        // Test memory with decimal value and space separator
        WorkflowCreateParams workflowParams = new WorkflowCreateParams()
                .setId("workflow-req-decimal-mem")
                .setScope(ExternalToolScope.OTHER)
                .setMinimumRequirements(new MinimumRequirements().setCpu("4").setMemory("16.5 GB"))
                .setWorkflow(new Workflow().setScripts(Collections.singletonList(
                        new WorkflowScript("pipeline.nf", "echo 'test'", true))));
        OpenCGAResult<ExternalTool> result = externalToolManager.createWorkflow(studyFqn, workflowParams, INCLUDE_RESULT, ownerToken);
        assertEquals(1, result.getNumResults());
        MinimumRequirements reqs = result.first().getMinimumRequirements();
        assertEquals("4", reqs.getCpu());
        assertEquals("16.5 GB", reqs.getMemory());
    }

    @Test
    public void createWorkflowWithDecimalMemoryNoSeparatorTest() throws CatalogException {
        // Test memory with decimal value directly attached to unit
        WorkflowCreateParams workflowParams = new WorkflowCreateParams()
                .setId("workflow-req-decimal-nosep")
                .setScope(ExternalToolScope.OTHER)
                .setMinimumRequirements(new MinimumRequirements().setCpu("4").setMemory("16.5GB"))
                .setWorkflow(new Workflow().setScripts(Collections.singletonList(
                        new WorkflowScript("pipeline.nf", "echo 'test'", true))));
        OpenCGAResult<ExternalTool> result = externalToolManager.createWorkflow(studyFqn, workflowParams, INCLUDE_RESULT, ownerToken);
        assertEquals(1, result.getNumResults());
        MinimumRequirements reqs = result.first().getMinimumRequirements();
        assertEquals("4", reqs.getCpu());
        assertEquals("16.5GB", reqs.getMemory());
    }

    @Test
    public void createWorkflowWithLowercaseMemoryUnitTest() throws CatalogException {
        // Test that memory units are case-insensitive (converted to uppercase for validation)
        WorkflowCreateParams workflowParams = new WorkflowCreateParams()
                .setId("workflow-req-lowercase")
                .setScope(ExternalToolScope.OTHER)
                .setMinimumRequirements(new MinimumRequirements().setCpu("4").setMemory("16gb"))
                .setWorkflow(new Workflow().setScripts(Collections.singletonList(
                        new WorkflowScript("pipeline.nf", "echo 'test'", true))));
        OpenCGAResult<ExternalTool> result = externalToolManager.createWorkflow(studyFqn, workflowParams, INCLUDE_RESULT, ownerToken);
        assertEquals(1, result.getNumResults());
        MinimumRequirements reqs = result.first().getMinimumRequirements();
        assertEquals("4", reqs.getCpu());
        assertEquals("16gb", reqs.getMemory());  // Original case is preserved
    }

    @Test
    public void createWorkflowWithMixedCaseMemoryUnitTest() throws CatalogException {
        // Test that memory units with mixed case work
        WorkflowCreateParams workflowParams = new WorkflowCreateParams()
                .setId("workflow-req-mixedcase")
                .setScope(ExternalToolScope.OTHER)
                .setMinimumRequirements(new MinimumRequirements().setCpu("4").setMemory("512Mb"))
                .setWorkflow(new Workflow().setScripts(Collections.singletonList(
                        new WorkflowScript("pipeline.nf", "echo 'test'", true))));
        OpenCGAResult<ExternalTool> result = externalToolManager.createWorkflow(studyFqn, workflowParams, INCLUDE_RESULT, ownerToken);
        assertEquals(1, result.getNumResults());
        MinimumRequirements reqs = result.first().getMinimumRequirements();
        assertEquals("4", reqs.getCpu());
        assertEquals("512Mb", reqs.getMemory());
    }

    @Test
    public void createWorkflowWithNegativeCpuTest() {
        // Test with negative CPU - should fail
        WorkflowCreateParams workflowParams = new WorkflowCreateParams()
                .setId("workflow-req-neg-cpu")
                .setScope(ExternalToolScope.OTHER)
                .setMinimumRequirements(new MinimumRequirements().setCpu("-4").setMemory("16GB"))
                .setWorkflow(new Workflow().setScripts(Collections.singletonList(
                        new WorkflowScript("pipeline.nf", "echo 'test'", true))));
        CatalogException exception = assertThrows(CatalogException.class,
                () -> externalToolManager.createWorkflow(studyFqn, workflowParams, INCLUDE_RESULT, ownerToken));
        assertTrue(exception.getMessage().contains("CPU") && exception.getMessage().contains("positive"));
    }

    @Test
    public void createWorkflowWithZeroCpuTest() {
        // Test with zero CPU - should fail
        WorkflowCreateParams workflowParams = new WorkflowCreateParams()
                .setId("workflow-req-zero-cpu")
                .setScope(ExternalToolScope.OTHER)
                .setMinimumRequirements(new MinimumRequirements().setCpu("0").setMemory("16GB"))
                .setWorkflow(new Workflow().setScripts(Collections.singletonList(
                        new WorkflowScript("pipeline.nf", "echo 'test'", true))));
        CatalogException exception = assertThrows(CatalogException.class,
                () -> externalToolManager.createWorkflow(studyFqn, workflowParams, INCLUDE_RESULT, ownerToken));
        assertTrue(exception.getMessage().contains("CPU") && exception.getMessage().contains("positive"));
    }

    @Test
    public void createWorkflowWithInvalidCpuTest() {
        // Test with non-numeric CPU - should fail
        WorkflowCreateParams workflowParams = new WorkflowCreateParams()
                .setId("workflow-req-invalid-cpu")
                .setScope(ExternalToolScope.OTHER)
                .setMinimumRequirements(new MinimumRequirements().setCpu("invalid").setMemory("16GB"))
                .setWorkflow(new Workflow().setScripts(Collections.singletonList(
                        new WorkflowScript("pipeline.nf", "echo 'test'", true))));
        CatalogException exception = assertThrows(CatalogException.class,
                () -> externalToolManager.createWorkflow(studyFqn, workflowParams, INCLUDE_RESULT, ownerToken));
        assertTrue(exception.getMessage().contains("CPU") && exception.getMessage().contains("positive"));
    }

    @Test
    public void createWorkflowWithNegativeMemoryTest() {
        // Test with negative memory - should fail
        WorkflowCreateParams workflowParams = new WorkflowCreateParams()
                .setId("workflow-req-neg-mem")
                .setScope(ExternalToolScope.OTHER)
                .setMinimumRequirements(new MinimumRequirements().setCpu("4").setMemory("-16GB"))
                .setWorkflow(new Workflow().setScripts(Collections.singletonList(
                        new WorkflowScript("pipeline.nf", "echo 'test'", true))));
        CatalogException exception = assertThrows(CatalogException.class,
                () -> externalToolManager.createWorkflow(studyFqn, workflowParams, INCLUDE_RESULT, ownerToken));
        assertTrue(exception.getMessage().contains("memory") && exception.getMessage().contains("positive"));
    }

    @Test
    public void createWorkflowWithZeroMemoryTest() {
        // Test with zero memory - should fail
        WorkflowCreateParams workflowParams = new WorkflowCreateParams()
                .setId("workflow-req-zero-mem")
                .setScope(ExternalToolScope.OTHER)
                .setMinimumRequirements(new MinimumRequirements().setCpu("4").setMemory("0GB"))
                .setWorkflow(new Workflow().setScripts(Collections.singletonList(
                        new WorkflowScript("pipeline.nf", "echo 'test'", true))));
        CatalogException exception = assertThrows(CatalogException.class,
                () -> externalToolManager.createWorkflow(studyFqn, workflowParams, INCLUDE_RESULT, ownerToken));
        assertTrue(exception.getMessage().contains("memory") && exception.getMessage().contains("positive"));
    }

    @Test
    public void createWorkflowWithInvalidMemoryTest() {
        // Test with non-numeric memory - should fail
        WorkflowCreateParams workflowParams = new WorkflowCreateParams()
                .setId("workflow-req-invalid-mem")
                .setScope(ExternalToolScope.OTHER)
                .setMinimumRequirements(new MinimumRequirements().setCpu("4").setMemory("invalidGB"))
                .setWorkflow(new Workflow().setScripts(Collections.singletonList(
                        new WorkflowScript("pipeline.nf", "echo 'test'", true))));
        CatalogException exception = assertThrows(CatalogException.class,
                () -> externalToolManager.createWorkflow(studyFqn, workflowParams, INCLUDE_RESULT, ownerToken));
        assertTrue(exception.getMessage().contains("memory") && exception.getMessage().contains("positive"));
    }

    @Test
    public void createWorkflowWithInvalidMemoryUnitTest() {
        // Test with invalid memory unit - should fail
        WorkflowCreateParams workflowParams = new WorkflowCreateParams()
                .setId("workflow-req-invalid-unit")
                .setScope(ExternalToolScope.OTHER)
                .setMinimumRequirements(new MinimumRequirements().setCpu("4").setMemory("16TB"))
                .setWorkflow(new Workflow().setScripts(Collections.singletonList(
                        new WorkflowScript("pipeline.nf", "echo 'test'", true))));
        CatalogException exception = assertThrows(CatalogException.class,
                () -> externalToolManager.createWorkflow(studyFqn, workflowParams, INCLUDE_RESULT, ownerToken));
        assertTrue(exception.getMessage().contains("unit") && exception.getMessage().contains("not valid"));
    }

    @Test
    public void createWorkflowWithMissingCpuTest() {
        // Test with missing CPU - should fail
        WorkflowCreateParams workflowParams = new WorkflowCreateParams()
                .setId("workflow-req-missing-cpu")
                .setScope(ExternalToolScope.OTHER)
                .setMinimumRequirements(new MinimumRequirements().setMemory("16GB"))
                .setWorkflow(new Workflow().setScripts(Collections.singletonList(
                        new WorkflowScript("pipeline.nf", "echo 'test'", true))));
        CatalogException exception = assertThrows(CatalogException.class,
                () -> externalToolManager.createWorkflow(studyFqn, workflowParams, INCLUDE_RESULT, ownerToken));
        assertTrue(exception.getMessage().contains("cpu"));
    }

    @Test
    public void createWorkflowWithMissingMemoryTest() {
        // Test with missing memory - should fail
        WorkflowCreateParams workflowParams = new WorkflowCreateParams()
                .setId("workflow-req-missing-mem")
                .setScope(ExternalToolScope.OTHER)
                .setMinimumRequirements(new MinimumRequirements().setCpu("4"))
                .setWorkflow(new Workflow().setScripts(Collections.singletonList(
                        new WorkflowScript("pipeline.nf", "echo 'test'", true))));
        CatalogException exception = assertThrows(CatalogException.class,
                () -> externalToolManager.createWorkflow(studyFqn, workflowParams, INCLUDE_RESULT, ownerToken));
        assertTrue(exception.getMessage().contains("memory"));
    }

    @Test
    public void createWorkflowWithNullMinimumRequirementsTest() throws CatalogException {
        // Test with null minimum requirements - should be allowed
        WorkflowCreateParams workflowParams = new WorkflowCreateParams()
                .setId("workflow-no-req")
                .setScope(ExternalToolScope.OTHER)
                .setWorkflow(new Workflow().setScripts(Collections.singletonList(
                        new WorkflowScript("pipeline.nf", "echo 'test'", true))));
        OpenCGAResult<ExternalTool> result = externalToolManager.createWorkflow(studyFqn, workflowParams, INCLUDE_RESULT, ownerToken);
        assertEquals(1, result.getNumResults());
        assertNull(result.first().getMinimumRequirements());
    }

    @Test
    public void createCustomToolWithValidMinimumRequirementsTest() throws CatalogException {
        // Test that minimum requirements work for custom tools as well
        CustomToolCreateParams toolParams = new CustomToolCreateParams()
                .setId("custom-with-req")
                .setScope(ExternalToolScope.OTHER)
                .setContainer(new Container("myimage", "1.0", "", "", "", ""))
                .setMinimumRequirements(new MinimumRequirements().setCpu("8").setMemory("64GB").setDisk("1000GB"));
        OpenCGAResult<ExternalTool> result = externalToolManager.createCustomTool(studyFqn, toolParams, INCLUDE_RESULT, ownerToken);
        assertEquals(1, result.getNumResults());
        MinimumRequirements reqs = result.first().getMinimumRequirements();
        assertEquals("8", reqs.getCpu());
        assertEquals("64GB", reqs.getMemory());
        assertEquals("1000GB", reqs.getDisk());
    }

    @Test
    public void createCustomToolWithInvalidMinimumRequirementsTest() {
        // Test that validation works for custom tools as well
        CustomToolCreateParams toolParams = new CustomToolCreateParams()
                .setId("custom-invalid-req")
                .setScope(ExternalToolScope.OTHER)
                .setContainer(new Container("myimage", "1.0", "", "", "", ""))
                .setMinimumRequirements(new MinimumRequirements().setCpu("invalid").setMemory("16GB"));
        CatalogException exception = assertThrows(CatalogException.class,
                () -> externalToolManager.createCustomTool(studyFqn, toolParams, INCLUDE_RESULT, ownerToken));
        assertTrue(exception.getMessage().contains("CPU") && exception.getMessage().contains("positive"));
    }

    @Test
    public void searchByTypeTest() throws CatalogException {
        // Create different types of tools
        WorkflowCreateParams workflowParams = new WorkflowCreateParams()
                .setId("workflow1")
                .setScope(ExternalToolScope.OTHER)
                .setWorkflow(new Workflow().setScripts(Collections.singletonList(new WorkflowScript("pipeline.nf", "echo 'Hello world!'", true))));
        externalToolManager.createWorkflow(studyFqn, workflowParams, QueryOptions.empty(), ownerToken);

        CustomToolCreateParams customToolParams = new CustomToolCreateParams()
                .setId("customTool1")
                .setScope(ExternalToolScope.OTHER)
                .setContainer(new Container("myimage", "1.0", "", "", "", ""));
        externalToolManager.createCustomTool(studyFqn, customToolParams, QueryOptions.empty(), ownerToken);

        CustomToolCreateParams walkerParams = new CustomToolCreateParams()
                .setId("walker1")
                .setScope(ExternalToolScope.OTHER)
                .setContainer(new Container("walkerimage", "1.0", "", "", "", ""));
        externalToolManager.createVariantWalkerTool(studyFqn, walkerParams, QueryOptions.empty(), ownerToken);

        // Search by type
        Query query = new Query(ExternalToolDBAdaptor.QueryParams.TYPE.key(), ExternalToolType.WORKFLOW);
        OpenCGAResult<ExternalTool> search = externalToolManager.search(studyFqn, query, QueryOptions.empty(), ownerToken);
        assertEquals(1, search.getNumResults());
        assertEquals("workflow1", search.first().getId());

        query = new Query(ExternalToolDBAdaptor.QueryParams.TYPE.key(), ExternalToolType.CUSTOM_TOOL);
        search = externalToolManager.search(studyFqn, query, QueryOptions.empty(), ownerToken);
        assertEquals(1, search.getNumResults());
        assertEquals("customTool1", search.first().getId());

        query = new Query(ExternalToolDBAdaptor.QueryParams.TYPE.key(), ExternalToolType.VARIANT_WALKER);
        search = externalToolManager.search(studyFqn, query, QueryOptions.empty(), ownerToken);
        assertEquals(1, search.getNumResults());
        assertEquals("walker1", search.first().getId());
    }

    @Test
    public void searchByScopeTest() throws CatalogException {
        WorkflowCreateParams workflowParams1 = new WorkflowCreateParams()
                .setId("workflow1")
                .setScope(ExternalToolScope.RESEARCH_ANALYSIS)
                .setWorkflow(new Workflow().setScripts(Collections.singletonList(new WorkflowScript("pipeline.nf", "echo 'Hello world!'", true))));
        externalToolManager.createWorkflow(studyFqn, workflowParams1, QueryOptions.empty(), ownerToken);

        WorkflowCreateParams workflowParams2 = new WorkflowCreateParams()
                .setId("workflow2")
                .setScope(ExternalToolScope.OTHER)
                .setWorkflow(new Workflow().setScripts(Collections.singletonList(new WorkflowScript("pipeline.nf", "echo 'Hello world!'", true))));
        externalToolManager.createWorkflow(studyFqn, workflowParams2, QueryOptions.empty(), ownerToken);

        // Search by scope
        Query query = new Query(ExternalToolDBAdaptor.QueryParams.SCOPE.key(), ExternalToolScope.RESEARCH_ANALYSIS);
        OpenCGAResult<ExternalTool> search = externalToolManager.search(studyFqn, query, QueryOptions.empty(), ownerToken);
        assertEquals(1, search.getNumResults());
        assertEquals("workflow1", search.first().getId());

        query = new Query(ExternalToolDBAdaptor.QueryParams.SCOPE.key(), ExternalToolScope.OTHER);
        search = externalToolManager.search(studyFqn, query, QueryOptions.empty(), ownerToken);
        assertEquals(1, search.getNumResults());
        assertEquals("workflow2", search.first().getId());
    }

    @Test
    public void searchByTagsTest() throws CatalogException {
        WorkflowCreateParams workflowParams1 = new WorkflowCreateParams()
                .setId("workflow1")
                .setScope(ExternalToolScope.OTHER)
                .setTags(Arrays.asList("rna", "sequencing"))
                .setWorkflow(new Workflow().setScripts(Collections.singletonList(new WorkflowScript("pipeline.nf", "echo 'Hello world!'", true))));
        externalToolManager.createWorkflow(studyFqn, workflowParams1, QueryOptions.empty(), ownerToken);

        WorkflowCreateParams workflowParams2 = new WorkflowCreateParams()
                .setId("workflow2")
                .setScope(ExternalToolScope.OTHER)
                .setTags(Arrays.asList("dna", "variant-calling"))
                .setWorkflow(new Workflow().setScripts(Collections.singletonList(new WorkflowScript("pipeline.nf", "echo 'Hello world!'", true))));
        externalToolManager.createWorkflow(studyFqn, workflowParams2, QueryOptions.empty(), ownerToken);

        // Search by tags
        Query query = new Query(ExternalToolDBAdaptor.QueryParams.TAGS.key(), "rna");
        OpenCGAResult<ExternalTool> search = externalToolManager.search(studyFqn, query, QueryOptions.empty(), ownerToken);
        assertEquals(1, search.getNumResults());
        assertEquals("workflow1", search.first().getId());

        query = new Query(ExternalToolDBAdaptor.QueryParams.TAGS.key(), "variant-calling");
        search = externalToolManager.search(studyFqn, query, QueryOptions.empty(), ownerToken);
        assertEquals(1, search.getNumResults());
        assertEquals("workflow2", search.first().getId());
    }

    @Test
    public void countTest() throws CatalogException {
        WorkflowCreateParams workflowParams = new WorkflowCreateParams()
                .setId("workflow1")
                .setScope(ExternalToolScope.OTHER)
                .setWorkflow(new Workflow().setScripts(Collections.singletonList(new WorkflowScript("pipeline.nf", "echo 'Hello world!'", true))));
        externalToolManager.createWorkflow(studyFqn, workflowParams, QueryOptions.empty(), ownerToken);

        CustomToolCreateParams customToolParams = new CustomToolCreateParams()
                .setId("customTool1")
                .setScope(ExternalToolScope.OTHER)
                .setContainer(new Container("myimage", "1.0", "", "", "", ""));
        externalToolManager.createCustomTool(studyFqn, customToolParams, QueryOptions.empty(), ownerToken);

        OpenCGAResult<ExternalTool> count = externalToolManager.count(studyFqn, new Query(), ownerToken);
        assertEquals(2, count.getNumMatches());

        Query query = new Query(ExternalToolDBAdaptor.QueryParams.TYPE.key(), ExternalToolType.WORKFLOW);
        count = externalToolManager.count(studyFqn, query, ownerToken);
        assertEquals(1, count.getNumMatches());
    }

    @Test
    public void getByUuidTest() throws CatalogException {
        WorkflowCreateParams workflowParams = new WorkflowCreateParams()
                .setId("workflow1")
                .setScope(ExternalToolScope.OTHER)
                .setWorkflow(new Workflow().setScripts(Collections.singletonList(new WorkflowScript("pipeline.nf", "echo 'Hello world!'", true))));
        OpenCGAResult<ExternalTool> createResult = externalToolManager.createWorkflow(studyFqn, workflowParams, INCLUDE_RESULT, ownerToken);
        String uuid = createResult.first().getUuid();

        OpenCGAResult<ExternalTool> result = externalToolManager.get(studyFqn, uuid, QueryOptions.empty(), ownerToken);
        assertEquals(1, result.getNumResults());
        assertEquals("workflow1", result.first().getId());
        assertEquals(uuid, result.first().getUuid());
    }

    @Test
    public void multipleGetTest() throws CatalogException {
        WorkflowCreateParams workflowParams1 = new WorkflowCreateParams()
                .setId("workflow1")
                .setScope(ExternalToolScope.OTHER)
                .setWorkflow(new Workflow().setScripts(Collections.singletonList(new WorkflowScript("pipeline.nf", "echo 'Hello world!'", true))));
        externalToolManager.createWorkflow(studyFqn, workflowParams1, QueryOptions.empty(), ownerToken);

        WorkflowCreateParams workflowParams2 = new WorkflowCreateParams()
                .setId("workflow2")
                .setScope(ExternalToolScope.OTHER)
                .setWorkflow(new Workflow().setScripts(Collections.singletonList(new WorkflowScript("pipeline.nf", "echo 'Hello world!'", true))));
        externalToolManager.createWorkflow(studyFqn, workflowParams2, QueryOptions.empty(), ownerToken);

        OpenCGAResult<ExternalTool> result = externalToolManager.get(studyFqn, Arrays.asList("workflow1", "workflow2"), QueryOptions.empty(), true, ownerToken);
        assertEquals(2, result.getNumResults());
    }

    @Test
    public void deleteMultipleWorkflowsTest() throws CatalogException {
        WorkflowCreateParams workflowParams1 = new WorkflowCreateParams()
                .setId("workflow1")
                .setScope(ExternalToolScope.OTHER)
                .setWorkflow(new Workflow().setScripts(Collections.singletonList(new WorkflowScript("pipeline.nf", "echo 'Hello world!'", true))));
        externalToolManager.createWorkflow(studyFqn, workflowParams1, QueryOptions.empty(), ownerToken);

        WorkflowCreateParams workflowParams2 = new WorkflowCreateParams()
                .setId("workflow2")
                .setScope(ExternalToolScope.OTHER)
                .setWorkflow(new Workflow().setScripts(Collections.singletonList(new WorkflowScript("pipeline.nf", "echo 'Hello world!'", true))));
        externalToolManager.createWorkflow(studyFqn, workflowParams2, QueryOptions.empty(), ownerToken);

        OpenCGAResult<ExternalTool> result = externalToolManager.delete(studyFqn, Arrays.asList("workflow1", "workflow2"), QueryOptions.empty(), ownerToken);
        assertEquals(2, result.getNumDeleted());

        OpenCGAResult<ExternalTool> search = externalToolManager.search(studyFqn, new Query(), QueryOptions.empty(), ownerToken);
        assertEquals(0, search.getNumResults());
    }

    @Test
    public void deleteByQueryTest() throws CatalogException {
        WorkflowCreateParams workflowParams = new WorkflowCreateParams()
                .setId("workflow1")
                .setScope(ExternalToolScope.OTHER)
                .setWorkflow(new Workflow().setScripts(Collections.singletonList(new WorkflowScript("pipeline.nf", "echo 'Hello world!'", true))));
        externalToolManager.createWorkflow(studyFqn, workflowParams, QueryOptions.empty(), ownerToken);

        CustomToolCreateParams customToolParams = new CustomToolCreateParams()
                .setId("customTool1")
                .setScope(ExternalToolScope.OTHER)
                .setContainer(new Container("myimage", "1.0", "", "", "", ""));
        externalToolManager.createCustomTool(studyFqn, customToolParams, QueryOptions.empty(), ownerToken);

        Query query = new Query(ExternalToolDBAdaptor.QueryParams.TYPE.key(), ExternalToolType.WORKFLOW);
        OpenCGAResult<ExternalTool> result = externalToolManager.delete(studyFqn, query, QueryOptions.empty(), ownerToken);
        assertEquals(1, result.getNumDeleted());

        OpenCGAResult<ExternalTool> search = externalToolManager.search(studyFqn, new Query(), QueryOptions.empty(), ownerToken);
        assertEquals(1, search.getNumResults());
        assertEquals("customTool1", search.first().getId());
    }

    @Test
    public void workflowWithRepositoryTest() throws CatalogException {
        WorkflowCreateParams workflowParams = new WorkflowCreateParams()
                .setId("workflow1")
                .setScope(ExternalToolScope.OTHER)
                .setWorkflow(new Workflow()
                        .setRepository(new WorkflowRepository("github.com/user/repo")));
        OpenCGAResult<ExternalTool> result = externalToolManager.createWorkflow(studyFqn, workflowParams, INCLUDE_RESULT, ownerToken);
        assertEquals(1, result.getNumResults());
        assertNotNull(result.first().getWorkflow().getRepository());
        assertEquals("github.com/user/repo", result.first().getWorkflow().getRepository().getName());
        assertTrue(result.first().getWorkflow().getScripts().isEmpty());
    }

    @Test
    public void workflowValidationNoMainScriptTest() {
        WorkflowCreateParams workflowParams = new WorkflowCreateParams()
                .setId("workflow1")
                .setScope(ExternalToolScope.OTHER)
                .setWorkflow(new Workflow()
                        .setScripts(Arrays.asList(
                                new WorkflowScript("script1.nf", "echo 'test1'", false),
                                new WorkflowScript("script2.nf", "echo 'test2'", false)
                        )));
        CatalogException exception = assertThrows(CatalogException.class,
                () -> externalToolManager.createWorkflow(studyFqn, workflowParams, INCLUDE_RESULT, ownerToken));
        assertTrue(exception.getMessage().contains("main"));
    }

    @Test
    public void workflowValidationMultipleMainScriptsTest() {
        WorkflowCreateParams workflowParams = new WorkflowCreateParams()
                .setId("workflow1")
                .setScope(ExternalToolScope.OTHER)
                .setWorkflow(new Workflow()
                        .setScripts(Arrays.asList(
                                new WorkflowScript("script1.nf", "echo 'test1'", true),
                                new WorkflowScript("script2.nf", "echo 'test2'", true)
                        )));
        CatalogException exception = assertThrows(CatalogException.class,
                () -> externalToolManager.createWorkflow(studyFqn, workflowParams, INCLUDE_RESULT, ownerToken));
        assertTrue(exception.getMessage().contains("main"));
    }

    @Test
    public void customToolValidationMissingContainerTest() {
        CustomToolCreateParams toolParams = new CustomToolCreateParams()
                .setId("customTool1")
                .setScope(ExternalToolScope.OTHER);
        CatalogException exception = assertThrows(CatalogException.class,
                () -> externalToolManager.createCustomTool(studyFqn, toolParams, INCLUDE_RESULT, ownerToken));
        assertTrue(exception.getMessage().contains("container") || exception.getMessage().contains("Docker"));
    }

    @Test
    public void iteratorTest() throws CatalogException {
        // Create multiple tools
        for (int i = 0; i < 5; i++) {
            WorkflowCreateParams workflowParams = new WorkflowCreateParams()
                    .setId("workflow" + i)
                    .setScope(ExternalToolScope.OTHER)
                    .setWorkflow(new Workflow().setScripts(Collections.singletonList(new WorkflowScript("pipeline.nf", "echo 'test'", true))));
            externalToolManager.createWorkflow(studyFqn, workflowParams, QueryOptions.empty(), ownerToken);
        }

        int count = 0;
        try (org.opencb.opencga.catalog.db.api.DBIterator<ExternalTool> iterator = externalToolManager.iterator(studyFqn, new Query(), QueryOptions.empty(), ownerToken)) {
            while (iterator.hasNext()) {
                ExternalTool tool = iterator.next();
                assertNotNull(tool);
                count++;
            }
        }
        assertEquals(5, count);
    }

    // ====================================
    // Tests for all @QueryParam from /search endpoint
    // ====================================

    @Test
    public void searchByIdTest() throws CatalogException {
        WorkflowCreateParams workflow1 = new WorkflowCreateParams()
                .setId("rnaseq-pipeline")
                .setScope(ExternalToolScope.OTHER)
                .setWorkflow(new Workflow().setScripts(Collections.singletonList(new WorkflowScript("pipeline.nf", "echo 'test'", true))));
        externalToolManager.createWorkflow(studyFqn, workflow1, QueryOptions.empty(), ownerToken);

        WorkflowCreateParams workflow2 = new WorkflowCreateParams()
                .setId("dna-pipeline")
                .setScope(ExternalToolScope.OTHER)
                .setWorkflow(new Workflow().setScripts(Collections.singletonList(new WorkflowScript("pipeline.nf", "echo 'test'", true))));
        externalToolManager.createWorkflow(studyFqn, workflow2, QueryOptions.empty(), ownerToken);

        // Search by exact ID
        Query query = new Query(ExternalToolDBAdaptor.QueryParams.ID.key(), "rnaseq-pipeline");
        OpenCGAResult<ExternalTool> result = externalToolManager.search(studyFqn, query, QueryOptions.empty(), ownerToken);
        assertEquals(1, result.getNumResults());
        assertEquals("rnaseq-pipeline", result.first().getId());

        // Search by multiple IDs
        query = new Query(ExternalToolDBAdaptor.QueryParams.ID.key(), "rnaseq-pipeline,dna-pipeline");
        result = externalToolManager.search(studyFqn, query, QueryOptions.empty(), ownerToken);
        assertEquals(2, result.getNumResults());
    }

    @Test
    public void searchByNameTest() throws CatalogException {
        WorkflowCreateParams workflow1 = new WorkflowCreateParams()
                .setId("workflow1")
                .setName("RNA Sequencing Pipeline")
                .setScope(ExternalToolScope.OTHER)
                .setWorkflow(new Workflow().setScripts(Collections.singletonList(new WorkflowScript("pipeline.nf", "echo 'test'", true))));
        externalToolManager.createWorkflow(studyFqn, workflow1, QueryOptions.empty(), ownerToken);

        WorkflowCreateParams workflow2 = new WorkflowCreateParams()
                .setId("workflow2")
                .setName("DNA Variant Calling")
                .setScope(ExternalToolScope.OTHER)
                .setWorkflow(new Workflow().setScripts(Collections.singletonList(new WorkflowScript("pipeline.nf", "echo 'test'", true))));
        externalToolManager.createWorkflow(studyFqn, workflow2, QueryOptions.empty(), ownerToken);

        // Search by name pattern (partial match)
        Query query = new Query(ExternalToolDBAdaptor.QueryParams.NAME.key(), "~RNA");
        OpenCGAResult<ExternalTool> result = externalToolManager.search(studyFqn, query, QueryOptions.empty(), ownerToken);
        assertTrue(result.getNumResults() >= 1);
        assertTrue(result.getResults().stream().anyMatch(t -> t.getName().contains("RNA")));
    }

    @Test
    public void searchByUuidParamTest() throws CatalogException {
        WorkflowCreateParams workflow = new WorkflowCreateParams()
                .setId("workflow1")
                .setScope(ExternalToolScope.OTHER)
                .setWorkflow(new Workflow().setScripts(Collections.singletonList(new WorkflowScript("pipeline.nf", "echo 'test'", true))));
        OpenCGAResult<ExternalTool> createResult = externalToolManager.createWorkflow(studyFqn, workflow, INCLUDE_RESULT, ownerToken);
        String uuid = createResult.first().getUuid();

        // Search by UUID
        Query query = new Query(ExternalToolDBAdaptor.QueryParams.UUID.key(), uuid);
        OpenCGAResult<ExternalTool> result = externalToolManager.search(studyFqn, query, QueryOptions.empty(), ownerToken);
        assertEquals(1, result.getNumResults());
        assertEquals(uuid, result.first().getUuid());
    }

    @Test
    public void searchByDraftTest() throws CatalogException {
        WorkflowCreateParams draftWorkflow = new WorkflowCreateParams()
                .setId("draft-workflow")
                .setDraft(true)
                .setScope(ExternalToolScope.OTHER)
                .setWorkflow(new Workflow().setScripts(Collections.singletonList(new WorkflowScript("pipeline.nf", "echo 'test'", true))));
        externalToolManager.createWorkflow(studyFqn, draftWorkflow, QueryOptions.empty(), ownerToken);

        WorkflowCreateParams publishedWorkflow = new WorkflowCreateParams()
                .setId("published-workflow")
                .setDraft(false)
                .setScope(ExternalToolScope.OTHER)
                .setWorkflow(new Workflow().setScripts(Collections.singletonList(new WorkflowScript("pipeline.nf", "echo 'test'", true))));
        externalToolManager.createWorkflow(studyFqn, publishedWorkflow, QueryOptions.empty(), ownerToken);

        // Search only drafts
        Query query = new Query(ExternalToolDBAdaptor.QueryParams.DRAFT.key(), true);
        OpenCGAResult<ExternalTool> result = externalToolManager.search(studyFqn, query, QueryOptions.empty(), ownerToken);
        assertTrue(result.getNumResults() >= 1);
        assertTrue(result.getResults().stream().allMatch(ExternalTool::isDraft));

        // Search only published (non-draft)
        query = new Query(ExternalToolDBAdaptor.QueryParams.DRAFT.key(), false);
        result = externalToolManager.search(studyFqn, query, QueryOptions.empty(), ownerToken);
        assertTrue(result.getNumResults() >= 1);
        assertTrue(result.getResults().stream().noneMatch(ExternalTool::isDraft));
    }

    @Test
    public void searchByInternalRegistrationUserIdTest() throws CatalogException {
        WorkflowCreateParams workflow = new WorkflowCreateParams()
                .setId("workflow1")
                .setScope(ExternalToolScope.OTHER)
                .setWorkflow(new Workflow().setScripts(Collections.singletonList(new WorkflowScript("pipeline.nf", "echo 'test'", true))));
        externalToolManager.createWorkflow(studyFqn, workflow, QueryOptions.empty(), ownerToken);

        // Search by registration user ID (should be the owner)
        Query query = new Query(ExternalToolDBAdaptor.QueryParams.INTERNAL_REGISTRATION_USER_ID.key(), orgOwnerUserId);
        OpenCGAResult<ExternalTool> result = externalToolManager.search(studyFqn, query, QueryOptions.empty(), ownerToken);
        assertTrue(result.getNumResults() >= 1);
        assertTrue(result.getResults().stream()
                .allMatch(t -> t.getInternal().getRegistrationUserId().equals(orgOwnerUserId)));
    }

    @Test
    public void searchByWorkflowRepositoryNameTest() throws CatalogException {
        WorkflowCreateParams workflow1 = new WorkflowCreateParams()
                .setId("workflow1")
                .setScope(ExternalToolScope.OTHER)
                .setWorkflow(new Workflow()
                        .setRepository(new WorkflowRepository("nf-core/rnaseq")));
        externalToolManager.createWorkflow(studyFqn, workflow1, QueryOptions.empty(), ownerToken);

        WorkflowCreateParams workflow2 = new WorkflowCreateParams()
                .setId("workflow2")
                .setScope(ExternalToolScope.OTHER)
                .setWorkflow(new Workflow()
                        .setRepository(new WorkflowRepository("nf-core/sarek")));
        externalToolManager.createWorkflow(studyFqn, workflow2, QueryOptions.empty(), ownerToken);

        // Search by repository name
        Query query = new Query(ExternalToolDBAdaptor.QueryParams.WORKFLOW_REPOSITORY_NAME.key(), "nf-core/rnaseq");
        OpenCGAResult<ExternalTool> result = externalToolManager.search(studyFqn, query, QueryOptions.empty(), ownerToken);
        assertEquals(1, result.getNumResults());
        assertEquals("nf-core/rnaseq", result.first().getWorkflow().getRepository().getName());

        // Search by repository name pattern
        query = new Query(ExternalToolDBAdaptor.QueryParams.WORKFLOW_REPOSITORY_NAME.key(), "~nf-core");
        result = externalToolManager.search(studyFqn, query, QueryOptions.empty(), ownerToken);
        assertTrue(result.getNumResults() >= 2);
    }

    @Test
    public void searchByContainerNameTest() throws CatalogException {
        CustomToolCreateParams tool1 = new CustomToolCreateParams()
                .setId("tool1")
                .setScope(ExternalToolScope.OTHER)
                .setContainer(new Container("quay.io/biocontainers/samtools", "1.15", "", "", "", ""));
        externalToolManager.createCustomTool(studyFqn, tool1, QueryOptions.empty(), ownerToken);

        CustomToolCreateParams tool2 = new CustomToolCreateParams()
                .setId("tool2")
                .setScope(ExternalToolScope.OTHER)
                .setContainer(new Container("docker.io/broadinstitute/gatk", "4.2.0.0", "", "", "", ""));
        externalToolManager.createCustomTool(studyFqn, tool2, QueryOptions.empty(), ownerToken);

        // Search by container name
        Query query = new Query(ExternalToolDBAdaptor.QueryParams.CONTAINER_NAME.key(), "quay.io/biocontainers/samtools");
        OpenCGAResult<ExternalTool> result = externalToolManager.search(studyFqn, query, QueryOptions.empty(), ownerToken);
        assertEquals(1, result.getNumResults());
        assertEquals("quay.io/biocontainers/samtools", result.first().getContainer().getName());

        // Search by container name pattern
        query = new Query(ExternalToolDBAdaptor.QueryParams.CONTAINER_NAME.key(), "~biocontainers");
        result = externalToolManager.search(studyFqn, query, QueryOptions.empty(), ownerToken);
        assertTrue(result.getNumResults() >= 1);
    }

    @Test
    public void searchByCreationDateTest() throws CatalogException {
        String today = "20251111000000";

        WorkflowCreateParams workflow = new WorkflowCreateParams()
                .setId("workflow1")
                .setScope(ExternalToolScope.OTHER)
                .setCreationDate(today)
                .setWorkflow(new Workflow().setScripts(Collections.singletonList(new WorkflowScript("pipeline.nf", "echo 'test'", true))));
        externalToolManager.createWorkflow(studyFqn, workflow, QueryOptions.empty(), ownerToken);

        // Search by creation date
        Query query = new Query(ExternalToolDBAdaptor.QueryParams.CREATION_DATE.key(), today);
        OpenCGAResult<ExternalTool> result = externalToolManager.search(studyFqn, query, QueryOptions.empty(), ownerToken);
        assertTrue(result.getNumResults() >= 1);

        // Search by creation date range (>=)
        query = new Query(ExternalToolDBAdaptor.QueryParams.CREATION_DATE.key(), ">=" + today);
        result = externalToolManager.search(studyFqn, query, QueryOptions.empty(), ownerToken);
        assertTrue(result.getNumResults() >= 1);
    }

    @Test
    public void searchByModificationDateTest() throws CatalogException {
        WorkflowCreateParams workflow = new WorkflowCreateParams()
                .setId("workflow1")
                .setScope(ExternalToolScope.OTHER)
                .setWorkflow(new Workflow().setScripts(Collections.singletonList(new WorkflowScript("pipeline.nf", "echo 'test'", true))));
        externalToolManager.createWorkflow(studyFqn, workflow, QueryOptions.empty(), ownerToken);

        // Update to change modification date
        WorkflowUpdateParams updateParams = new WorkflowUpdateParams().setDescription("Updated description");
        externalToolManager.updateWorkflow(studyFqn, "workflow1", updateParams, QueryOptions.empty(), ownerToken);

        // Search by modification date (should find recently modified)
        String today = "20251111";
        Query query = new Query(ExternalToolDBAdaptor.QueryParams.MODIFICATION_DATE.key(), ">=" + today);
        OpenCGAResult<ExternalTool> result = externalToolManager.search(studyFqn, query, QueryOptions.empty(), ownerToken);
        assertTrue(result.getNumResults() >= 1);
    }

    @Test
    public void searchCombinedParametersTest() throws CatalogException {
        // Create workflows with different combinations
        WorkflowCreateParams workflow1 = new WorkflowCreateParams()
                .setId("secondary-workflow")
                .setName("Secondary Analysis Pipeline")
                .setScope(ExternalToolScope.SECONDARY_ANALYSIS)
                .setTags(Arrays.asList("variant-calling", "secondary"))
                .setDraft(false)
                .setWorkflow(new Workflow().setScripts(Collections.singletonList(new WorkflowScript("pipeline.nf", "echo 'test'", true))));
        externalToolManager.createWorkflow(studyFqn, workflow1, QueryOptions.empty(), ownerToken);

        WorkflowCreateParams workflow2 = new WorkflowCreateParams()
                .setId("research-workflow")
                .setName("Research Analysis Pipeline")
                .setScope(ExternalToolScope.RESEARCH_ANALYSIS)
                .setTags(Arrays.asList("variant-calling", "research"))
                .setDraft(true)
                .setWorkflow(new Workflow().setScripts(Collections.singletonList(new WorkflowScript("pipeline.nf", "echo 'test'", true))));
        externalToolManager.createWorkflow(studyFqn, workflow2, QueryOptions.empty(), ownerToken);

        CustomToolCreateParams customTool = new CustomToolCreateParams()
                .setId("custom-tool")
                .setScope(ExternalToolScope.OTHER)
                .setTags(Collections.singletonList("quality-control"))
                .setContainer(new Container("myimage", "1.0", "", "", "", ""));
        externalToolManager.createCustomTool(studyFqn, customTool, QueryOptions.empty(), ownerToken);

        // Combined query: type=WORKFLOW AND scope=SECONDARY_ANALYSIS AND draft=false
        Query query = new Query()
                .append(ExternalToolDBAdaptor.QueryParams.TYPE.key(), ExternalToolType.WORKFLOW)
                .append(ExternalToolDBAdaptor.QueryParams.SCOPE.key(), ExternalToolScope.SECONDARY_ANALYSIS)
                .append(ExternalToolDBAdaptor.QueryParams.DRAFT.key(), false);
        OpenCGAResult<ExternalTool> result = externalToolManager.search(studyFqn, query, QueryOptions.empty(), ownerToken);
        assertEquals(1, result.getNumResults());
        assertEquals("secondary-workflow", result.first().getId());

        // Combined query: tags contains "variant-calling" AND type=WORKFLOW
        query = new Query()
                .append(ExternalToolDBAdaptor.QueryParams.TAGS.key(), "variant-calling")
                .append(ExternalToolDBAdaptor.QueryParams.TYPE.key(), ExternalToolType.WORKFLOW);
        result = externalToolManager.search(studyFqn, query, QueryOptions.empty(), ownerToken);
        assertEquals(2, result.getNumResults());
        assertTrue(result.getResults().stream().allMatch(t -> t.getType() == ExternalToolType.WORKFLOW));
        assertTrue(result.getResults().stream().allMatch(t -> t.getTags().contains("variant-calling")));

        // Combined query: scope IN (SECONDARY_ANALYSIS, RESEARCH_ANALYSIS) AND draft=true
        query = new Query()
                .append(ExternalToolDBAdaptor.QueryParams.SCOPE.key(), ExternalToolScope.SECONDARY_ANALYSIS + "," + ExternalToolScope.RESEARCH_ANALYSIS)
                .append(ExternalToolDBAdaptor.QueryParams.DRAFT.key(), true);
        result = externalToolManager.search(studyFqn, query, QueryOptions.empty(), ownerToken);
        assertEquals(1, result.getNumResults());
        assertEquals("research-workflow", result.first().getId());
    }

    @Test
    public void searchWithPaginationTest() throws CatalogException {
        // Create multiple workflows
        for (int i = 0; i < 10; i++) {
            WorkflowCreateParams workflow = new WorkflowCreateParams()
                    .setId("workflow-" + i)
                    .setScope(ExternalToolScope.OTHER)
                    .setWorkflow(new Workflow().setScripts(Collections.singletonList(new WorkflowScript("pipeline.nf", "echo 'test'", true))));
            externalToolManager.createWorkflow(studyFqn, workflow, QueryOptions.empty(), ownerToken);
        }

        // Test with limit
        QueryOptions options = new QueryOptions(QueryOptions.LIMIT, 3);
        OpenCGAResult<ExternalTool> result = externalToolManager.search(studyFqn, new Query(), options, ownerToken);
        assertEquals(3, result.getNumResults());

        // Test with skip
        options = new QueryOptions()
                .append(QueryOptions.LIMIT, 3)
                .append(QueryOptions.SKIP, 3);
        result = externalToolManager.search(studyFqn, new Query(), options, ownerToken);
        assertEquals(3, result.getNumResults());

        // Test with count
        options = new QueryOptions(QueryOptions.COUNT, true);
        result = externalToolManager.search(studyFqn, new Query(), options, ownerToken);
        assertTrue(result.getNumMatches() >= 10);
    }

    @Test
    public void searchWithIncludeExcludeTest() throws CatalogException {
        WorkflowCreateParams workflow = new WorkflowCreateParams()
                .setId("workflow1")
                .setName("Test Workflow")
                .setDescription("Test Description")
                .setScope(ExternalToolScope.OTHER)
                .setWorkflow(new Workflow().setScripts(Collections.singletonList(new WorkflowScript("pipeline.nf", "echo 'test'", true))));
        externalToolManager.createWorkflow(studyFqn, workflow, QueryOptions.empty(), ownerToken);

        // Test with INCLUDE
        QueryOptions options = new QueryOptions(QueryOptions.INCLUDE, "id,name");
        OpenCGAResult<ExternalTool> result = externalToolManager.search(studyFqn, new Query(), options, ownerToken);
        assertTrue(result.getNumResults() >= 1);
        ExternalTool tool = result.first();
        assertNotNull(tool.getId());
        assertNotNull(tool.getName());

        // Test with EXCLUDE
        options = new QueryOptions(QueryOptions.EXCLUDE, "workflow,attributes");
        result = externalToolManager.search(studyFqn, new Query(), options, ownerToken);
        assertTrue(result.getNumResults() >= 1);
    }

    @Test
    public void searchByMultipleTypesTest() throws CatalogException {
        // Create different types of tools
        WorkflowCreateParams workflow = new WorkflowCreateParams()
                .setId("workflow1")
                .setScope(ExternalToolScope.OTHER)
                .setWorkflow(new Workflow().setScripts(Collections.singletonList(new WorkflowScript("pipeline.nf", "echo 'test'", true))));
        externalToolManager.createWorkflow(studyFqn, workflow, QueryOptions.empty(), ownerToken);

        CustomToolCreateParams customTool = new CustomToolCreateParams()
                .setId("customTool1")
                .setScope(ExternalToolScope.OTHER)
                .setContainer(new Container("myimage", "1.0", "", "", "", ""));
        externalToolManager.createCustomTool(studyFqn, customTool, QueryOptions.empty(), ownerToken);

        CustomToolCreateParams walker = new CustomToolCreateParams()
                .setId("walker1")
                .setScope(ExternalToolScope.OTHER)
                .setContainer(new Container("walkerimage", "1.0", "", "", "", ""));
        externalToolManager.createVariantWalkerTool(studyFqn, walker, QueryOptions.empty(), ownerToken);

        // Search for multiple types at once
        Query query = new Query(ExternalToolDBAdaptor.QueryParams.TYPE.key(),
                ExternalToolType.WORKFLOW + "," + ExternalToolType.CUSTOM_TOOL);
        OpenCGAResult<ExternalTool> result = externalToolManager.search(studyFqn, query, QueryOptions.empty(), ownerToken);
        assertEquals(2, result.getNumResults());
        assertTrue(result.getResults().stream()
                .allMatch(t -> t.getType() == ExternalToolType.WORKFLOW || t.getType() == ExternalToolType.CUSTOM_TOOL));
    }

    @Test
    public void searchByMultipleTagsTest() throws CatalogException {
        WorkflowCreateParams workflow1 = new WorkflowCreateParams()
                .setId("workflow1")
                .setScope(ExternalToolScope.OTHER)
                .setTags(Arrays.asList("rna", "sequencing", "alignment"))
                .setWorkflow(new Workflow().setScripts(Collections.singletonList(new WorkflowScript("pipeline.nf", "echo 'test'", true))));
        externalToolManager.createWorkflow(studyFqn, workflow1, QueryOptions.empty(), ownerToken);

        WorkflowCreateParams workflow2 = new WorkflowCreateParams()
                .setId("workflow2")
                .setScope(ExternalToolScope.OTHER)
                .setTags(Arrays.asList("dna", "variant-calling"))
                .setWorkflow(new Workflow().setScripts(Collections.singletonList(new WorkflowScript("pipeline.nf", "echo 'test'", true))));
        externalToolManager.createWorkflow(studyFqn, workflow2, QueryOptions.empty(), ownerToken);

        // Search for workflows with specific tag
        Query query = new Query(ExternalToolDBAdaptor.QueryParams.TAGS.key(), "rna");
        OpenCGAResult<ExternalTool> result = externalToolManager.search(studyFqn, query, QueryOptions.empty(), ownerToken);
        assertEquals(1, result.getNumResults());
        assertTrue(result.first().getTags().contains("rna"));

        // Search for workflows with multiple tags (OR operation)
        query = new Query(ExternalToolDBAdaptor.QueryParams.TAGS.key(), "rna,variant-calling");
        result = externalToolManager.search(studyFqn, query, QueryOptions.empty(), ownerToken);
        assertEquals(2, result.getNumResults());
    }

    // ====================================
    // Tests using ParamConstants (REST API parameter names)
    // ====================================

    @Test
    public void searchByIdUsingParamConstantsTest() throws CatalogException {
        WorkflowCreateParams workflow = new WorkflowCreateParams()
                .setId("rest-workflow")
                .setScope(ExternalToolScope.OTHER)
                .setWorkflow(new Workflow().setScripts(Collections.singletonList(new WorkflowScript("pipeline.nf", "echo 'test'", true))));
        externalToolManager.createWorkflow(studyFqn, workflow, QueryOptions.empty(), ownerToken);

        // Use ParamConstants parameter name (as REST API would)
        Query query = new Query(ParamConstants.EXTERNAL_TOOL_ID_PARAM, "rest-workflow");
        OpenCGAResult<ExternalTool> result = externalToolManager.search(studyFqn, query, QueryOptions.empty(), ownerToken);
        assertEquals(1, result.getNumResults());
        assertEquals("rest-workflow", result.first().getId());
    }

    @Test
    public void searchByNameUsingParamConstantsTest() throws CatalogException {
        WorkflowCreateParams workflow = new WorkflowCreateParams()
                .setId("workflow-rest")
                .setName("REST API Workflow Test")
                .setScope(ExternalToolScope.OTHER)
                .setWorkflow(new Workflow().setScripts(Collections.singletonList(new WorkflowScript("pipeline.nf", "echo 'test'", true))));
        externalToolManager.createWorkflow(studyFqn, workflow, QueryOptions.empty(), ownerToken);

        // Use ParamConstants parameter name
        Query query = new Query(ParamConstants.EXTERNAL_TOOL_NAME_PARAM, "~REST API");
        OpenCGAResult<ExternalTool> result = externalToolManager.search(studyFqn, query, QueryOptions.empty(), ownerToken);
        assertTrue(result.getNumResults() >= 1);
        assertTrue(result.getResults().stream().anyMatch(t -> t.getName().contains("REST API")));
    }

    @Test
    public void searchByUuidUsingParamConstantsTest() throws CatalogException {
        WorkflowCreateParams workflow = new WorkflowCreateParams()
                .setId("workflow-uuid-test")
                .setScope(ExternalToolScope.OTHER)
                .setWorkflow(new Workflow().setScripts(Collections.singletonList(new WorkflowScript("pipeline.nf", "echo 'test'", true))));
        OpenCGAResult<ExternalTool> createResult = externalToolManager.createWorkflow(studyFqn, workflow, INCLUDE_RESULT, ownerToken);
        String uuid = createResult.first().getUuid();

        // Use ParamConstants parameter name
        Query query = new Query(ParamConstants.EXTERNAL_TOOL_UUID_PARAM, uuid);
        OpenCGAResult<ExternalTool> result = externalToolManager.search(studyFqn, query, QueryOptions.empty(), ownerToken);
        assertEquals(1, result.getNumResults());
        assertEquals(uuid, result.first().getUuid());
    }

    @Test
    public void searchByTypeUsingParamConstantsTest() throws CatalogException {
        WorkflowCreateParams workflow = new WorkflowCreateParams()
                .setId("workflow-type-test")
                .setScope(ExternalToolScope.OTHER)
                .setWorkflow(new Workflow().setScripts(Collections.singletonList(new WorkflowScript("pipeline.nf", "echo 'test'", true))));
        externalToolManager.createWorkflow(studyFqn, workflow, QueryOptions.empty(), ownerToken);

        CustomToolCreateParams customTool = new CustomToolCreateParams()
                .setId("custom-type-test")
                .setScope(ExternalToolScope.OTHER)
                .setContainer(new Container("myimage", "1.0", "", "", "", ""));
        externalToolManager.createCustomTool(studyFqn, customTool, QueryOptions.empty(), ownerToken);

        // Use ParamConstants parameter name
        Query query = new Query(ParamConstants.EXTERNAL_TOOL_TYPE_PARAM, ExternalToolType.WORKFLOW);
        OpenCGAResult<ExternalTool> result = externalToolManager.search(studyFqn, query, QueryOptions.empty(), ownerToken);
        assertTrue(result.getNumResults() >= 1);
        assertTrue(result.getResults().stream().allMatch(t -> t.getType() == ExternalToolType.WORKFLOW));

        query = new Query(ParamConstants.EXTERNAL_TOOL_TYPE_PARAM, ExternalToolType.CUSTOM_TOOL);
        result = externalToolManager.search(studyFqn, query, QueryOptions.empty(), ownerToken);
        assertTrue(result.getNumResults() >= 1);
        assertTrue(result.getResults().stream().allMatch(t -> t.getType() == ExternalToolType.CUSTOM_TOOL));
    }

    @Test
    public void searchByScopeUsingParamConstantsTest() throws CatalogException {
        WorkflowCreateParams workflow1 = new WorkflowCreateParams()
                .setId("secondary-scope-test")
                .setScope(ExternalToolScope.SECONDARY_ANALYSIS)
                .setWorkflow(new Workflow().setScripts(Collections.singletonList(new WorkflowScript("pipeline.nf", "echo 'test'", true))));
        externalToolManager.createWorkflow(studyFqn, workflow1, QueryOptions.empty(), ownerToken);

        WorkflowCreateParams workflow2 = new WorkflowCreateParams()
                .setId("research-scope-test")
                .setScope(ExternalToolScope.RESEARCH_ANALYSIS)
                .setWorkflow(new Workflow().setScripts(Collections.singletonList(new WorkflowScript("pipeline.nf", "echo 'test'", true))));
        externalToolManager.createWorkflow(studyFqn, workflow2, QueryOptions.empty(), ownerToken);

        // Use ParamConstants parameter name
        Query query = new Query(ParamConstants.EXTERNAL_TOOL_SCOPE_PARAM, ExternalToolScope.SECONDARY_ANALYSIS);
        OpenCGAResult<ExternalTool> result = externalToolManager.search(studyFqn, query, QueryOptions.empty(), ownerToken);
        assertTrue(result.getNumResults() >= 1);
        assertTrue(result.getResults().stream().allMatch(t -> t.getScope() == ExternalToolScope.SECONDARY_ANALYSIS));

        query = new Query(ParamConstants.EXTERNAL_TOOL_SCOPE_PARAM, ExternalToolScope.RESEARCH_ANALYSIS);
        result = externalToolManager.search(studyFqn, query, QueryOptions.empty(), ownerToken);
        assertTrue(result.getNumResults() >= 1);
        assertTrue(result.getResults().stream().allMatch(t -> t.getScope() == ExternalToolScope.RESEARCH_ANALYSIS));
    }

    @Test
    public void searchByTagsUsingParamConstantsTest() throws CatalogException {
        WorkflowCreateParams workflow = new WorkflowCreateParams()
                .setId("workflow-tags-test")
                .setScope(ExternalToolScope.OTHER)
                .setTags(Arrays.asList("rest-api", "testing", "validation"))
                .setWorkflow(new Workflow().setScripts(Collections.singletonList(new WorkflowScript("pipeline.nf", "echo 'test'", true))));
        externalToolManager.createWorkflow(studyFqn, workflow, QueryOptions.empty(), ownerToken);

        // Use ParamConstants parameter name
        Query query = new Query(ParamConstants.EXTERNAL_TOOL_TAGS_PARAM, "rest-api");
        OpenCGAResult<ExternalTool> result = externalToolManager.search(studyFqn, query, QueryOptions.empty(), ownerToken);
        assertTrue(result.getNumResults() >= 1);
        assertTrue(result.getResults().stream().anyMatch(t -> t.getTags().contains("rest-api")));

        // Multiple tags
        query = new Query(ParamConstants.EXTERNAL_TOOL_TAGS_PARAM, "testing,validation");
        result = externalToolManager.search(studyFqn, query, QueryOptions.empty(), ownerToken);
        assertTrue(result.getNumResults() >= 1);
    }

    @Test
    public void searchByDraftUsingParamConstantsTest() throws CatalogException {
        WorkflowCreateParams draftWorkflow = new WorkflowCreateParams()
                .setId("draft-rest-test")
                .setDraft(true)
                .setScope(ExternalToolScope.OTHER)
                .setWorkflow(new Workflow().setScripts(Collections.singletonList(new WorkflowScript("pipeline.nf", "echo 'test'", true))));
        externalToolManager.createWorkflow(studyFqn, draftWorkflow, QueryOptions.empty(), ownerToken);

        WorkflowCreateParams publishedWorkflow = new WorkflowCreateParams()
                .setId("published-rest-test")
                .setDraft(false)
                .setScope(ExternalToolScope.OTHER)
                .setWorkflow(new Workflow().setScripts(Collections.singletonList(new WorkflowScript("pipeline.nf", "echo 'test'", true))));
        externalToolManager.createWorkflow(studyFqn, publishedWorkflow, QueryOptions.empty(), ownerToken);

        // Use ParamConstants parameter name
        Query query = new Query(ParamConstants.EXTERNAL_TOOL_DRAFT_PARAM, true);
        OpenCGAResult<ExternalTool> result = externalToolManager.search(studyFqn, query, QueryOptions.empty(), ownerToken);
        assertTrue(result.getNumResults() >= 1);
        assertTrue(result.getResults().stream().allMatch(ExternalTool::isDraft));

        query = new Query(ParamConstants.EXTERNAL_TOOL_DRAFT_PARAM, false);
        result = externalToolManager.search(studyFqn, query, QueryOptions.empty(), ownerToken);
        assertTrue(result.getNumResults() >= 1);
        assertTrue(result.getResults().stream().noneMatch(ExternalTool::isDraft));
    }

    @Test
    public void searchByInternalRegistrationUserIdUsingParamConstantsTest() throws CatalogException {
        WorkflowCreateParams workflow = new WorkflowCreateParams()
                .setId("workflow-user-test")
                .setScope(ExternalToolScope.OTHER)
                .setWorkflow(new Workflow().setScripts(Collections.singletonList(new WorkflowScript("pipeline.nf", "echo 'test'", true))));
        externalToolManager.createWorkflow(studyFqn, workflow, QueryOptions.empty(), ownerToken);

        // Use ParamConstants parameter name
        Query query = new Query(ParamConstants.EXTERNAL_TOOL_INTERNAL_REGISTRATION_USER_ID_PARAM, orgOwnerUserId);
        OpenCGAResult<ExternalTool> result = externalToolManager.search(studyFqn, query, QueryOptions.empty(), ownerToken);
        assertTrue(result.getNumResults() >= 1);
        assertTrue(result.getResults().stream()
                .allMatch(t -> t.getInternal().getRegistrationUserId().equals(orgOwnerUserId)));
    }

    @Test
    public void searchByWorkflowRepositoryNameUsingParamConstantsTest() throws CatalogException {
        WorkflowCreateParams workflow1 = new WorkflowCreateParams()
                .setId("repo-workflow-1")
                .setScope(ExternalToolScope.OTHER)
                .setWorkflow(new Workflow()
                        .setRepository(new WorkflowRepository("github.com/company/pipeline1")));
        externalToolManager.createWorkflow(studyFqn, workflow1, QueryOptions.empty(), ownerToken);

        WorkflowCreateParams workflow2 = new WorkflowCreateParams()
                .setId("repo-workflow-2")
                .setScope(ExternalToolScope.OTHER)
                .setWorkflow(new Workflow()
                        .setRepository(new WorkflowRepository("github.com/company/pipeline2")));
        externalToolManager.createWorkflow(studyFqn, workflow2, QueryOptions.empty(), ownerToken);

        // Use ParamConstants parameter name (special name for REST API)
        Query query = new Query(ParamConstants.EXTERNAL_TOOL_WORKFLOW_REPOSITORY_NAME_PARAM, "github.com/company/pipeline1");
        OpenCGAResult<ExternalTool> result = externalToolManager.search(studyFqn, query, QueryOptions.empty(), ownerToken);
        assertEquals(1, result.getNumResults());
        assertEquals("github.com/company/pipeline1", result.first().getWorkflow().getRepository().getName());

        // Pattern search
        query = new Query(ParamConstants.EXTERNAL_TOOL_WORKFLOW_REPOSITORY_NAME_PARAM, "~github.com/company");
        result = externalToolManager.search(studyFqn, query, QueryOptions.empty(), ownerToken);
        assertTrue(result.getNumResults() >= 2);
    }

    @Test
    public void searchByContainerNameUsingParamConstantsTest() throws CatalogException {
        CustomToolCreateParams tool1 = new CustomToolCreateParams()
                .setId("container-tool-1")
                .setScope(ExternalToolScope.OTHER)
                .setContainer(new Container("docker.io/library/alpine", "3.15", "", "", "", ""));
        externalToolManager.createCustomTool(studyFqn, tool1, QueryOptions.empty(), ownerToken);

        CustomToolCreateParams tool2 = new CustomToolCreateParams()
                .setId("container-tool-2")
                .setScope(ExternalToolScope.OTHER)
                .setContainer(new Container("docker.io/library/ubuntu", "20.04", "", "", "", ""));
        externalToolManager.createCustomTool(studyFqn, tool2, QueryOptions.empty(), ownerToken);

        // Use ParamConstants parameter name (special name for REST API)
        Query query = new Query(ParamConstants.EXTERNAL_TOOL_CONTAINER_NAME_PARAM, "docker.io/library/alpine");
        OpenCGAResult<ExternalTool> result = externalToolManager.search(studyFqn, query, QueryOptions.empty(), ownerToken);
        assertEquals(1, result.getNumResults());
        assertEquals("docker.io/library/alpine", result.first().getContainer().getName());

        // Pattern search
        query = new Query(ParamConstants.EXTERNAL_TOOL_CONTAINER_NAME_PARAM, "~docker.io/library");
        result = externalToolManager.search(studyFqn, query, QueryOptions.empty(), ownerToken);
        assertTrue(result.getNumResults() >= 2);
    }

    @Test
    public void searchByCreationDateUsingParamConstantsTest() throws CatalogException {
        String today = "20251111000000";

        WorkflowCreateParams workflow = new WorkflowCreateParams()
                .setId("workflow-date-test")
                .setScope(ExternalToolScope.OTHER)
                .setCreationDate(today)
                .setWorkflow(new Workflow().setScripts(Collections.singletonList(new WorkflowScript("pipeline.nf", "echo 'test'", true))));
        externalToolManager.createWorkflow(studyFqn, workflow, QueryOptions.empty(), ownerToken);

        // Use ParamConstants parameter name
        Query query = new Query(ParamConstants.CREATION_DATE_PARAM, today);
        OpenCGAResult<ExternalTool> result = externalToolManager.search(studyFqn, query, QueryOptions.empty(), ownerToken);
        assertTrue(result.getNumResults() >= 1);

        // Range query
        query = new Query(ParamConstants.CREATION_DATE_PARAM, ">=" + today);
        result = externalToolManager.search(studyFqn, query, QueryOptions.empty(), ownerToken);
        assertTrue(result.getNumResults() >= 1);
    }

    @Test
    public void searchByModificationDateUsingParamConstantsTest() throws CatalogException {
        WorkflowCreateParams workflow = new WorkflowCreateParams()
                .setId("workflow-mod-date-test")
                .setScope(ExternalToolScope.OTHER)
                .setWorkflow(new Workflow().setScripts(Collections.singletonList(new WorkflowScript("pipeline.nf", "echo 'test'", true))));
        externalToolManager.createWorkflow(studyFqn, workflow, QueryOptions.empty(), ownerToken);

        // Update to change modification date
        WorkflowUpdateParams updateParams = new WorkflowUpdateParams().setDescription("Modified for date test");
        externalToolManager.updateWorkflow(studyFqn, "workflow-mod-date-test", updateParams, QueryOptions.empty(), ownerToken);

        // Use ParamConstants parameter name
        String today = "20251111000000";
        Query query = new Query(ParamConstants.MODIFICATION_DATE_PARAM, ">=" + today);
        OpenCGAResult<ExternalTool> result = externalToolManager.search(studyFqn, query, QueryOptions.empty(), ownerToken);
        assertTrue(result.getNumResults() >= 1);
    }

    @Test
    public void searchCombinedWithParamConstantsTest() throws CatalogException {
        // Create tools with various attributes
        WorkflowCreateParams workflow1 = new WorkflowCreateParams()
                .setId("combined-workflow-1")
                .setName("Combined Test Workflow")
                .setScope(ExternalToolScope.SECONDARY_ANALYSIS)
                .setTags(Arrays.asList("rest-api", "combined"))
                .setDraft(false)
                .setWorkflow(new Workflow().setScripts(Collections.singletonList(new WorkflowScript("pipeline.nf", "echo 'test'", true))));
        externalToolManager.createWorkflow(studyFqn, workflow1, QueryOptions.empty(), ownerToken);

        WorkflowCreateParams workflow2 = new WorkflowCreateParams()
                .setId("combined-workflow-2")
                .setScope(ExternalToolScope.RESEARCH_ANALYSIS)
                .setTags(Collections.singletonList("rest-api"))
                .setDraft(true)
                .setWorkflow(new Workflow().setScripts(Collections.singletonList(new WorkflowScript("pipeline.nf", "echo 'test'", true))));
        externalToolManager.createWorkflow(studyFqn, workflow2, QueryOptions.empty(), ownerToken);

        CustomToolCreateParams customTool = new CustomToolCreateParams()
                .setId("combined-custom-tool")
                .setScope(ExternalToolScope.OTHER)
                .setTags(Collections.singletonList("rest-api"))
                .setContainer(new Container("myimage", "1.0", "", "", "", ""));
        externalToolManager.createCustomTool(studyFqn, customTool, QueryOptions.empty(), ownerToken);

        // Combined query using ParamConstants: type=WORKFLOW AND scope=SECONDARY_ANALYSIS AND draft=false
        Query query = new Query()
                .append(ParamConstants.EXTERNAL_TOOL_TYPE_PARAM, ExternalToolType.WORKFLOW)
                .append(ParamConstants.EXTERNAL_TOOL_SCOPE_PARAM, ExternalToolScope.SECONDARY_ANALYSIS)
                .append(ParamConstants.EXTERNAL_TOOL_DRAFT_PARAM, false);
        OpenCGAResult<ExternalTool> result = externalToolManager.search(studyFqn, query, QueryOptions.empty(), ownerToken);
        assertTrue(result.getNumResults() >= 1);
        assertTrue(result.getResults().stream().anyMatch(t -> t.getId().equals("combined-workflow-1")));

        // Combined query: tags=rest-api AND type=WORKFLOW AND draft=true
        query = new Query()
                .append(ParamConstants.EXTERNAL_TOOL_TAGS_PARAM, "rest-api")
                .append(ParamConstants.EXTERNAL_TOOL_TYPE_PARAM, ExternalToolType.WORKFLOW)
                .append(ParamConstants.EXTERNAL_TOOL_DRAFT_PARAM, true);
        result = externalToolManager.search(studyFqn, query, QueryOptions.empty(), ownerToken);
        assertTrue(result.getNumResults() >= 1);
        assertTrue(result.getResults().stream().anyMatch(t -> t.getId().equals("combined-workflow-2")));

        // Combined query: tags=rest-api AND type=CUSTOM_TOOL
        query = new Query()
                .append(ParamConstants.EXTERNAL_TOOL_TAGS_PARAM, "rest-api")
                .append(ParamConstants.EXTERNAL_TOOL_TYPE_PARAM, ExternalToolType.CUSTOM_TOOL);
        result = externalToolManager.search(studyFqn, query, QueryOptions.empty(), ownerToken);
        assertTrue(result.getNumResults() >= 1);
        assertTrue(result.getResults().stream().anyMatch(t -> t.getId().equals("combined-custom-tool")));
    }

}
