package org.opencb.opencga.analysis.tools;

import org.junit.Test;
import org.opencb.opencga.core.tools.annotations.ToolExecutor;
import org.opencb.opencga.core.exceptions.ToolException;
import org.opencb.opencga.core.tools.OpenCgaToolExecutor;

import java.util.Arrays;
import java.util.Collections;

import static org.junit.Assert.assertEquals;

public class ToolExecutorFactoryTest {
    @ToolExecutor(id="test-executor", tool = "test-analysis", framework = ToolExecutor.Framework.LOCAL, source= ToolExecutor.Source.FILE)
    public static final class MyExecutor1 extends OpenCgaToolExecutor { @Override public void run() { } }

    @ToolExecutor(id="test-executor-mr", tool = "test-analysis", framework = ToolExecutor.Framework.MAP_REDUCE, source= ToolExecutor.Source.HBASE)
    public static final class MyExecutor2 extends OpenCgaToolExecutor { @Override public void run() { } }

    @Test
    public void testGetExecutorClass() throws ToolException {
        ToolExecutorFactory toolExecutorFactory = new ToolExecutorFactory();
        String toolId = "test-analysis";

        assertEquals(MyExecutor2.class, toolExecutorFactory.getToolExecutorClass(toolId, "test-executor-mr", MyExecutor2.class));

        assertEquals("test-analysis", toolId);
        assertEquals(MyExecutor1.class, toolExecutorFactory.getToolExecutorClass(toolId, "test-executor"));
        assertEquals(MyExecutor2.class, toolExecutorFactory.getToolExecutorClass(toolId, "test-executor-mr"));

        assertEquals("test-executor-mr", toolExecutorFactory.getToolExecutor(toolId, null,
                OpenCgaToolExecutor.class,
                Collections.singletonList(ToolExecutor.Source.HBASE),
                Arrays.asList(ToolExecutor.Framework.MAP_REDUCE, ToolExecutor.Framework.LOCAL)).getId());

        assertEquals("test-executor", toolExecutorFactory.getToolExecutor(toolId, null,
                OpenCgaToolExecutor.class,
                Collections.singletonList(ToolExecutor.Source.FILE),
                Collections.singletonList(ToolExecutor.Framework.LOCAL)).getId());

        assertEquals("test-executor", toolExecutorFactory.getToolExecutor(toolId, null,
                OpenCgaToolExecutor.class,
                Collections.singletonList(ToolExecutor.Source.FILE),
                Arrays.asList(ToolExecutor.Framework.MAP_REDUCE, ToolExecutor.Framework.LOCAL)).getId());

        assertEquals("test-executor", toolExecutorFactory.getToolExecutor(toolId, null,
                OpenCgaToolExecutor.class,
                Arrays.asList(ToolExecutor.Source.FILE, ToolExecutor.Source.HBASE),
                Arrays.asList(ToolExecutor.Framework.LOCAL, ToolExecutor.Framework.MAP_REDUCE)).getId());

        assertEquals("test-executor-mr", toolExecutorFactory.getToolExecutor(toolId, null,
                OpenCgaToolExecutor.class,
                Arrays.asList(ToolExecutor.Source.FILE, ToolExecutor.Source.HBASE),
                Arrays.asList(ToolExecutor.Framework.MAP_REDUCE, ToolExecutor.Framework.LOCAL)).getId());

        assertEquals("test-executor-mr", toolExecutorFactory.getToolExecutor(toolId, null,
                OpenCgaToolExecutor.class,
                Arrays.asList(ToolExecutor.Source.HBASE, ToolExecutor.Source.FILE),
                Arrays.asList(ToolExecutor.Framework.MAP_REDUCE, ToolExecutor.Framework.LOCAL)).getId());
    }
}