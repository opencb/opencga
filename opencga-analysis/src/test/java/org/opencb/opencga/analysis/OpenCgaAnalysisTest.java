package org.opencb.opencga.analysis;

import org.junit.Test;
import org.opencb.opencga.core.analysis.OpenCgaAnalysisExecutor;
import org.opencb.opencga.core.annotations.Analysis;
import org.opencb.opencga.core.annotations.AnalysisExecutor;
import org.opencb.opencga.core.exception.AnalysisExecutorException;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.junit.Assert.assertEquals;

public class OpenCgaAnalysisTest {
    @AnalysisExecutor(id="test-executor", analysis = "test-analysis", framework = AnalysisExecutor.Framework.LOCAL, source= AnalysisExecutor.Source.FILE)
    public static class MyExecutor1 extends OpenCgaAnalysisExecutor { @Override public void run() { } }

    @AnalysisExecutor(id="test-executor-mr", analysis = "test-analysis", framework = AnalysisExecutor.Framework.MAP_REDUCE, source= AnalysisExecutor.Source.HBASE)
    public static class MyExecutor2 extends OpenCgaAnalysisExecutor { @Override public void run() { } }

    @Analysis(id = "test-analysis", type = Analysis.AnalysisType.VARIANT)
    public static class MyAnalysis extends OpenCgaAnalysis {
        @Override
        public void run() {
        }

        public void set(List<AnalysisExecutor.Source> sources, List<AnalysisExecutor.Framework> frameworks) {
            this.sourceTypes = sources;
            this.availableFrameworks = frameworks;
        }

    }


    @Test
    public void testGetExecutorClass() throws AnalysisExecutorException {
        MyAnalysis analysis = new MyAnalysis();

        assertEquals("test-analysis", analysis.getId());
        assertEquals(MyExecutor1.class, analysis.getAnalysisExecutorClass("test-executor"));
        assertEquals(MyExecutor2.class, analysis.getAnalysisExecutorClass("test-executor-mr"));

        analysis.set(Collections.singletonList(AnalysisExecutor.Source.HBASE), Arrays.asList(AnalysisExecutor.Framework.MAP_REDUCE, AnalysisExecutor.Framework.LOCAL));
        assertEquals("test-executor-mr", analysis.getAnalysisExecutor().getId());

        analysis.set(Collections.singletonList(AnalysisExecutor.Source.FILE), Collections.singletonList(AnalysisExecutor.Framework.LOCAL));
        assertEquals("test-executor", analysis.getAnalysisExecutor().getId());

        analysis.set(Collections.singletonList(AnalysisExecutor.Source.FILE), Arrays.asList(AnalysisExecutor.Framework.MAP_REDUCE, AnalysisExecutor.Framework.LOCAL));
        assertEquals("test-executor", analysis.getAnalysisExecutor().getId());

        analysis.set(Arrays.asList(AnalysisExecutor.Source.FILE, AnalysisExecutor.Source.HBASE), Arrays.asList(AnalysisExecutor.Framework.LOCAL, AnalysisExecutor.Framework.MAP_REDUCE));
        assertEquals("test-executor", analysis.getAnalysisExecutor().getId());

        analysis.set(Arrays.asList(AnalysisExecutor.Source.FILE, AnalysisExecutor.Source.HBASE), Arrays.asList(AnalysisExecutor.Framework.MAP_REDUCE, AnalysisExecutor.Framework.LOCAL));
        assertEquals("test-executor-mr", analysis.getAnalysisExecutor().getId());

        analysis.set(Arrays.asList(AnalysisExecutor.Source.HBASE, AnalysisExecutor.Source.FILE), Arrays.asList(AnalysisExecutor.Framework.MAP_REDUCE, AnalysisExecutor.Framework.LOCAL));
        assertEquals("test-executor-mr", analysis.getAnalysisExecutor().getId());
    }
}