package org.opencb.opencga.analysis.tools;

import org.junit.Test;
import org.opencb.opencga.core.annotations.Tool;
import org.opencb.opencga.core.exception.ToolException;
import org.opencb.opencga.core.models.common.Enums;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;

import static org.junit.Assert.*;

public class ToolFactoryTest {
    public static final String ID = "test-tool-factory";
    public static final String ID_DUP = "test-tool-factory-duplicated";

    @Tool(id = ID, resource = Enums.Resource.FUNCTIONAL)
    public static class MyTest extends OpenCgaTool {
        @Override
        protected void run() throws Exception {
        }
    }

    @Tool(id = ID_DUP, resource = Enums.Resource.FUNCTIONAL)
    public static class MyTest1 extends OpenCgaTool {
        @Override
        protected void run() throws Exception {
        }
    }

    @Tool(id = ID_DUP, resource = Enums.Resource.FUNCTIONAL)
    public static class MyTest2 extends OpenCgaTool {
        @Override
        protected void run() throws Exception {
        }
    }

    @Test
    public void testFactoryById() throws ToolException {
        ToolFactory toolFactory = new ToolFactory();

        assertEquals(MyTest.class, toolFactory.createTool(ID).getClass());
        assertEquals(MyTest.class, toolFactory.createTool(MyTest.class.getName()).getClass());
    }

    @Test
    public void testFactoryByClass() throws ToolException {
        ToolFactory toolFactory = new ToolFactory();

        assertEquals(MyTest.class, toolFactory.createTool(MyTest.class).getClass());
    }

    @Test
    public void testDuplicatedIds() throws ToolException {
        ToolFactory toolFactory = new ToolFactory();

        assertEquals(Collections.singletonMap(ID_DUP, new HashSet<>(Arrays.asList(MyTest1.class, MyTest2.class))),
                toolFactory.getDuplicatedTools());
    }
}