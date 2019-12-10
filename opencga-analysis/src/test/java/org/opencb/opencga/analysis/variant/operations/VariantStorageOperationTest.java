package org.opencb.opencga.analysis.variant.operations;

import org.junit.Test;
import org.opencb.opencga.core.annotations.Tool;
import org.reflections.Reflections;

import static org.junit.Assert.*;

public class VariantStorageOperationTest {

    @Test
    public void checkStorageOperations() {

        Reflections reflections = new Reflections(this.getClass().getPackage().getName());
        for (Class<? extends StorageOperation> aClass : reflections.getSubTypesOf(StorageOperation.class)) {
            Tool annotation = aClass.getAnnotation(Tool.class);
            assertNotNull(aClass.toString(), annotation);
        }
    }
}