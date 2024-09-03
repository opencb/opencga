package org.opencb.opencga.analysis.tools;

import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.time.StopWatch;
import org.junit.Test;
import org.opencb.commons.datastore.core.ObjectMap;
import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.opencga.analysis.StorageManager;
import org.opencb.opencga.analysis.workflow.NextFlowExecutor;
import org.opencb.opencga.catalog.exceptions.CatalogException;
import org.opencb.opencga.catalog.managers.AbstractManagerTest;
import org.opencb.opencga.core.api.ParamConstants;
import org.opencb.opencga.core.config.storage.StorageConfiguration;
import org.opencb.opencga.core.exceptions.ToolException;
import org.opencb.opencga.core.models.file.File;
import org.opencb.opencga.core.models.file.FileCreateParams;
import org.opencb.opencga.core.models.workflow.NextFlowRunParams;
import org.opencb.opencga.core.models.workflow.WorkflowCreateParams;
import org.opencb.opencga.core.models.workflow.WorkflowRepository;
import org.opencb.opencga.core.models.workflow.WorkflowScript;
import org.opencb.opencga.storage.core.StorageEngineFactory;

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;

public class WorkflowExecutorTest extends AbstractManagerTest {

    @Test
    public void nextflowScriptTest() throws ToolException, CatalogException, IOException {
        InputStream inputStream = StorageManager.class.getClassLoader().getResourceAsStream("storage-configuration.yml");
        StorageConfiguration storageConfiguration = StorageConfiguration.load(inputStream, "yml");

        WorkflowCreateParams workflow = createDummyWorkflow();
        catalogManager.getWorkflowManager().create(studyFqn, workflow.toWorkflow(), QueryOptions.empty(), ownerToken);

        Path outDir = Paths.get(catalogManagerResource.createTmpOutdir("_nextflow"));

        StopWatch stopWatch = StopWatch.createStarted();
        NextFlowExecutor nextFlowExecutorTest = new NextFlowExecutor();
        NextFlowRunParams runParams = new NextFlowRunParams(workflow.getId(), 1, Collections.emptyMap());
        ObjectMap params = runParams.toObjectMap();
        params.put(ParamConstants.STUDY_PARAM, studyFqn);
        nextFlowExecutorTest.setUp(catalogManagerResource.getOpencgaHome().toString(), catalogManager,
                StorageEngineFactory.get(storageConfiguration), params, outDir, "", false, ownerToken);
//        nextFlowExecutorTest.setUp(catalogManagerResource.getOpencgaHome().toString(), runParams.toObjectMap(), outDir, ownerToken);
        nextFlowExecutorTest.start();
        System.out.println(stopWatch.getTime(TimeUnit.MILLISECONDS));
    }

    @Test
    public void nextflowScriptWithFilesTest() throws ToolException, CatalogException, IOException {
        InputStream inputStream = StorageManager.class.getClassLoader().getResourceAsStream("storage-configuration.yml");
        StorageConfiguration storageConfiguration = StorageConfiguration.load(inputStream, "yml");

        catalogManager.getFileManager().create(studyFqn, new FileCreateParams().setPath("myfile.txt").setContent("hello world").setType(File.Type.FILE), false, ownerToken);

        WorkflowCreateParams workflow = createDummyWorkflow("pipeline_cat_file.nf");
        catalogManager.getWorkflowManager().create(studyFqn, workflow.toWorkflow(), QueryOptions.empty(), ownerToken);

        Path outDir = Paths.get(catalogManagerResource.createTmpOutdir("_nextflow"));

        StopWatch stopWatch = StopWatch.createStarted();
        NextFlowExecutor nextFlowExecutorTest = new NextFlowExecutor();
        Map<String, String> workflowParams = new HashMap<>();
        workflowParams.put("in", "ocga://myfile.txt");
        NextFlowRunParams runParams = new NextFlowRunParams(workflow.getId(), 1, workflowParams);
        ObjectMap params = runParams.toObjectMap();
        params.put(ParamConstants.STUDY_PARAM, studyFqn);
        nextFlowExecutorTest.setUp(catalogManagerResource.getOpencgaHome().toString(), catalogManager,
                StorageEngineFactory.get(storageConfiguration), params, outDir, "", false, ownerToken);
//        nextFlowExecutorTest.setUp(catalogManagerResource.getOpencgaHome().toString(), runParams.toObjectMap(), outDir, ownerToken);
        nextFlowExecutorTest.start();
        System.out.println(stopWatch.getTime(TimeUnit.MILLISECONDS));
    }

    @Test
    public void nextflowDockerTest() throws ToolException, CatalogException, IOException {
        InputStream inputStream = StorageManager.class.getClassLoader().getResourceAsStream("storage-configuration.yml");
        StorageConfiguration storageConfiguration = StorageConfiguration.load(inputStream, "yml");
        WorkflowCreateParams workflow = new WorkflowCreateParams()
                .setId("workflow")
//                .setCommandLine("run nextflow-io/rnaseq-nf -with-docker");
                .setRepository(new WorkflowRepository("nextflow-io/rnaseq-nf"));
        catalogManager.getWorkflowManager().create(studyFqn, workflow.toWorkflow(), QueryOptions.empty(), ownerToken);

        Path outDir = Paths.get(catalogManagerResource.createTmpOutdir("_nextflow"));

        StopWatch stopWatch = StopWatch.createStarted();
        NextFlowExecutor nextFlowExecutorTest = new NextFlowExecutor();
        NextFlowRunParams runParams = new NextFlowRunParams(workflow.getId(), 1, Collections.emptyMap());
        ObjectMap params = runParams.toObjectMap();
        params.put(ParamConstants.STUDY_PARAM, studyFqn);
        nextFlowExecutorTest.setUp(catalogManagerResource.getOpencgaHome().toString(), catalogManager,
                StorageEngineFactory.get(storageConfiguration), params, outDir, "", false, ownerToken);
        nextFlowExecutorTest.start();
        System.out.println(stopWatch.getTime(TimeUnit.MILLISECONDS));
    }

    private WorkflowCreateParams createDummyWorkflow() throws IOException {
        return createDummyWorkflow("pipeline.nf");
    }

    private WorkflowCreateParams createDummyWorkflow(String pipelineId) throws IOException {
        InputStream inputStream = this.getClass().getClassLoader().getResourceAsStream("nextflow/" + pipelineId);
        String content = IOUtils.toString(inputStream, "UTF-8");
        return new WorkflowCreateParams()
                .setId("workflow")
                .setScripts(Collections.singletonList(new WorkflowScript("pipeline.nf", content, true)));
    }
}
