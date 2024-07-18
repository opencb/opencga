package org.opencb.opencga.analysis.tools;

import org.apache.commons.lang3.time.StopWatch;
import org.junit.Test;
import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.opencga.analysis.StorageManager;
import org.opencb.opencga.catalog.exceptions.CatalogException;
import org.opencb.opencga.catalog.managers.AbstractManagerTest;
import org.opencb.opencga.core.config.storage.StorageConfiguration;
import org.opencb.opencga.core.exceptions.ToolException;
import org.opencb.opencga.core.models.nextflow.NextFlowRunParams;
import org.opencb.opencga.core.models.nextflow.Workflow;
import org.opencb.opencga.storage.core.StorageEngineFactory;

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Collections;
import java.util.concurrent.TimeUnit;

public class WorkflowExecutorTest extends AbstractManagerTest {

    @Test
    public void myTest() throws ToolException, CatalogException, IOException {
        InputStream inputStream = StorageManager.class.getClassLoader().getResourceAsStream("storage-configuration.yml");
        StorageConfiguration storageConfiguration = StorageConfiguration.load(inputStream, "yml");

        Workflow workflow = getDummyWorkflow();
        catalogManager.getWorkflowManager().create(workflow, QueryOptions.empty(), ownerToken);

        Path outDir = Paths.get(catalogManagerResource.createTmpOutdir("_nextflow"));

        StopWatch stopWatch = StopWatch.createStarted();
        NextFlowExecutor nextFlowExecutorTest = new NextFlowExecutor();
        NextFlowRunParams runParams = new NextFlowRunParams(workflow.getId(), 1);
        nextFlowExecutorTest.setUp(catalogManagerResource.getOpencgaHome().toString(), catalogManager,
                StorageEngineFactory.get(storageConfiguration), runParams.toObjectMap(), outDir, "", false, ownerToken);
//        nextFlowExecutorTest.setUp(catalogManagerResource.getOpencgaHome().toString(), runParams.toObjectMap(), outDir, ownerToken);
        nextFlowExecutorTest.start();
        System.out.println(stopWatch.getTime(TimeUnit.MILLISECONDS));
    }

    private Workflow getDummyWorkflow() {
        String scriptContent = "params.str = 'Hello world!'\n" +
                "\n" +
                "process splitLetters {\n" +
                "    output:\n" +
                "    path 'chunk_*'\n" +
                "\n" +
                "    \"\"\"\n" +
                "    printf '${params.str}' | split -b 6 - chunk_\n" +
                "    \"\"\"\n" +
                "}\n" +
                "\n" +
                "process convertToUpper {\n" +
                "    input:\n" +
                "    path x\n" +
                "\n" +
                "    output:\n" +
                "    stdout\n" +
                "\n" +
                "    \"\"\"\n" +
                "    cat $x | tr '[a-z]' '[A-Z]'\n" +
                "    \"\"\"\n" +
                "}\n" +
                "\n" +
                "process sleep {\n" +
                "    input:\n" +
                "    val x\n" +
                "\n" +
                "    \"\"\"\n" +
                "    sleep 6\n" +
                "    \"\"\"\n" +
                "}\n" +
                "\n" +
                "workflow {\n" +
                "    splitLetters | flatten | convertToUpper | view { it.trim() } | sleep\n" +
                "}";
        return new Workflow()
                .setId("workflow")
                .setCommandLine("run pipeline.nf")
                .setType(Workflow.Type.NEXTFLOW)
                .setScripts(Collections.singletonList(new Workflow.Script("pipeline.nf", scriptContent)));
    }
}
