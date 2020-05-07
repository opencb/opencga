/*
 * Copyright 2015-2020 OpenCB
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.opencb.opencga.analysis.tools;

import org.junit.Test;
import org.opencb.opencga.core.tools.annotations.Tool;
import org.opencb.opencga.core.exceptions.ToolException;
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