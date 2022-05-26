package org.opencb.opencga.catalog.managers;

import org.junit.Test;
import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.opencga.catalog.exceptions.CatalogException;
import org.opencb.opencga.core.models.job.Pipeline;
import org.opencb.opencga.core.models.job.PipelineUpdateParams;

import java.io.IOException;
import java.util.LinkedHashMap;

import static org.junit.Assert.assertEquals;
import static org.opencb.opencga.core.api.ParamConstants.INCLUDE_RESULT_PARAM;

public class PipelineManagerTest extends AbstractManagerTest {

    @Test
    public void createPipelineTest() throws IOException, CatalogException {
        Pipeline pipeline = Pipeline.load(getClass().getResource("/pipelines/pipeline-correct.yml").openStream());
        catalogManager.getPipelineManager().create(studyFqn, pipeline, QueryOptions.empty(), token);

        Pipeline pipeline2 = catalogManager.getPipelineManager().get(studyFqn, pipeline.getId(), QueryOptions.empty(), token).first();
        assertEquals(4, pipeline2.getJobs().size());
        for (Pipeline.PipelineJob value : pipeline2.getJobs().values()) {
            assertEquals(pipeline.getId(), value.getName());
            assertEquals(pipeline.getDescription(), value.getDescription());
        }
        assertEquals(2, pipeline2.getJobs().get("alignment-stats").getParams().size());
        assertEquals(pipeline.getParams().get("file1"), pipeline2.getJobs().get("alignment-stats").getParams().get("file2"));
        assertEquals(pipeline.getJobs().get("alignment-stats").getParams().get("file"), pipeline2.getJobs().get("alignment-stats").getParams().get("file"));

        assertEquals(pipeline.getParams().get("method") + "-END", pipeline2.getJobs().get("alignment-flagstats").getParams().get("method"));
    }

    @Test
    public void updatePipelineTest() throws IOException, CatalogException {
        Pipeline pipeline = Pipeline.load(getClass().getResource("/pipelines/pipeline-correct.yml").openStream());
        catalogManager.getPipelineManager().create(studyFqn, pipeline, QueryOptions.empty(), token);

        LinkedHashMap<String, Pipeline.PipelineJob> jobMap = new LinkedHashMap<>();
        jobMap.put("a", new Pipeline.PipelineJob("files-delete", null, "a", "${PIPELINE.description}", null, null, null));
        PipelineUpdateParams updateParams = new PipelineUpdateParams()
                .setDescription("hello")
                .setJobs(jobMap);

        pipeline = catalogManager.getPipelineManager().update(studyFqn, pipeline.getId(), updateParams,
                new QueryOptions(INCLUDE_RESULT_PARAM, true), token).first();
        assertEquals("hello", pipeline.getDescription());
        assertEquals(2, pipeline.getVersion());
        assertEquals(1, pipeline.getJobs().size());
        assertEquals("hello", pipeline.getJobs().get("a").getDescription());
    }


    @Test
    public void createPipelineErrorTest() throws IOException, CatalogException {
        Pipeline pipeline = Pipeline.load(getClass().getResource("/pipelines/pipeline-incorrect.yml").openStream());
        thrown.expect(CatalogException.class);
        thrown.expectMessage("depends on");
        catalogManager.getPipelineManager().create(studyFqn, pipeline, QueryOptions.empty(), token);
    }

}
