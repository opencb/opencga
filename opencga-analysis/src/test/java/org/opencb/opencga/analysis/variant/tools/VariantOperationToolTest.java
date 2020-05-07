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

package org.opencb.opencga.analysis.variant.tools;

import org.junit.Test;
import org.opencb.opencga.analysis.variant.operations.OperationTool;
import org.opencb.opencga.core.tools.annotations.Tool;
import org.reflections.Reflections;

import static org.junit.Assert.*;

public class VariantOperationToolTest {

    @Test
    public void checkStorageOperations() {

        Reflections reflections = new Reflections(this.getClass().getPackage().getName());
        for (Class<? extends OperationTool> aClass : reflections.getSubTypesOf(OperationTool.class)) {
            Tool annotation = aClass.getAnnotation(Tool.class);
            assertNotNull(aClass.toString(), annotation);
        }
    }
}