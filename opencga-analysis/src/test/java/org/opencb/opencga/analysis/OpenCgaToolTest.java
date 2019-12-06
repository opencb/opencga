package org.opencb.opencga.analysis;

import org.junit.Test;
import org.opencb.commons.datastore.core.ObjectMap;
import org.opencb.opencga.core.tools.OpenCgaToolExecutor;
import org.opencb.opencga.core.annotations.Tool;
import org.opencb.opencga.core.annotations.ToolExecutor;
import org.opencb.opencga.core.exception.ToolExecutorException;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.junit.Assert.assertEquals;

public class OpenCgaToolTest {
    @ToolExecutor(id="test-executor", tool = "test-analysis", framework = ToolExecutor.Framework.LOCAL, source= ToolExecutor.Source.FILE)
    public static final class MyExecutor1 extends OpenCgaToolExecutor { @Override public void run() { } }

    @ToolExecutor(id="test-executor-mr", tool = "test-analysis", framework = ToolExecutor.Framework.MAP_REDUCE, source= ToolExecutor.Source.HBASE)
    public static final class MyExecutor2 extends OpenCgaToolExecutor { @Override public void run() { } }

    @Tool(id = "test-analysis", type = Tool.ToolType.VARIANT)
    public static class MyAnalysis extends OpenCgaTool {

        public MyAnalysis() {
            params = new ObjectMap();
            executorParams = new ObjectMap();
        }

        @Override
        public void run() {
        }

        public void set(List<ToolExecutor.Source> sources, List<ToolExecutor.Framework> frameworks) {
            this.sourceTypes = sources;
            this.availableFrameworks = frameworks;
            params.remove(OpenCgaToolExecutor.EXECUTOR_ID);
            executorParams.remove(OpenCgaToolExecutor.EXECUTOR_ID);
        }

    }
    @Test
    public void testGetExecutorClass() throws ToolExecutorException {
        MyAnalysis analysis = new MyAnalysis();

        assertEquals(MyExecutor2.class, analysis.getToolExecutorClass(MyExecutor2.class, "test-executor-mr"));

        assertEquals("test-analysis", analysis.getId());
        assertEquals(MyExecutor1.class, analysis.getToolExecutorClass("test-executor"));
        assertEquals(MyExecutor2.class, analysis.getToolExecutorClass("test-executor-mr"));

        analysis.set(Collections.singletonList(ToolExecutor.Source.HBASE), Arrays.asList(ToolExecutor.Framework.MAP_REDUCE, ToolExecutor.Framework.LOCAL));
        assertEquals("test-executor-mr", analysis.getToolExecutor().getId());

        analysis.set(Collections.singletonList(ToolExecutor.Source.FILE), Collections.singletonList(ToolExecutor.Framework.LOCAL));
        assertEquals("test-executor", analysis.getToolExecutor().getId());

        analysis.set(Collections.singletonList(ToolExecutor.Source.FILE), Arrays.asList(ToolExecutor.Framework.MAP_REDUCE, ToolExecutor.Framework.LOCAL));
        assertEquals("test-executor", analysis.getToolExecutor().getId());

        analysis.set(Arrays.asList(ToolExecutor.Source.FILE, ToolExecutor.Source.HBASE), Arrays.asList(ToolExecutor.Framework.LOCAL, ToolExecutor.Framework.MAP_REDUCE));
        assertEquals("test-executor", analysis.getToolExecutor().getId());

        analysis.set(Arrays.asList(ToolExecutor.Source.FILE, ToolExecutor.Source.HBASE), Arrays.asList(ToolExecutor.Framework.MAP_REDUCE, ToolExecutor.Framework.LOCAL));
        assertEquals("test-executor-mr", analysis.getToolExecutor().getId());

        analysis.set(Arrays.asList(ToolExecutor.Source.HBASE, ToolExecutor.Source.FILE), Arrays.asList(ToolExecutor.Framework.MAP_REDUCE, ToolExecutor.Framework.LOCAL));
        assertEquals("test-executor-mr", analysis.getToolExecutor().getId());
    }
}