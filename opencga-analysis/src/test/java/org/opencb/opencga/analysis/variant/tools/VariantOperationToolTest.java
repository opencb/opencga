package org.opencb.opencga.analysis.variant.tools;

import org.junit.Test;
import org.opencb.opencga.analysis.variant.operations.OperationTool;
import org.opencb.opencga.core.annotations.Tool;
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