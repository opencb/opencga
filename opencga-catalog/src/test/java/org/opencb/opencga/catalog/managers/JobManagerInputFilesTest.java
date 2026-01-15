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

package org.opencb.opencga.catalog.managers;

import com.fasterxml.jackson.databind.BeanDescription;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.introspect.BeanPropertyDefinition;
import com.fasterxml.jackson.databind.type.TypeBindings;
import org.junit.Assert;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.opencb.opencga.catalog.exceptions.CatalogException;
import org.opencb.opencga.core.common.JacksonUtils;
import org.opencb.opencga.core.testclassification.duration.ShortTests;
import org.opencb.opencga.core.tools.ToolParams;
import org.reflections.Reflections;
import org.reflections.scanners.SubTypesScanner;
import org.reflections.util.ConfigurationBuilder;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.*;

/**
 * Test suite for Job Input Files Detection functionality given the map of Job params.
 * Tests both suffix-based and pattern-based file detection with deep nesting support.
 */
@Category(ShortTests.class)
public class JobManagerInputFilesTest  {

    private final String testFile1 = "/studyA/sample1/file1.vcf";
    private final String testFile2 = "/studyA/sample2/file2.bam";
    private final String data_d1 = "/studyA/data/dir1";

    public static class MyToolParams extends ToolParams {
        public String inputFile;
        public String otherInputFiles; // Comma separated
        public List<String> inputFiles;
        public NestedParams nested;
        public List<NestedParams> nestedList;

        public static class NestedParams extends ToolParams {
            public String dataFile;
            public List<String> dataFiles;

            public NestedParams setDataFile(String dataFile) {
                this.dataFile = dataFile;
                return this;
            }

            public NestedParams setDataFiles(List<String> dataFiles) {
                this.dataFiles = dataFiles;
                return this;
            }
        }

        public static MyToolParams defaultParams() {
            MyToolParams params = new MyToolParams();
            params.inputFile = "file.txt";
            params.otherInputFiles = "other1.txt,other2.txt";
            params.inputFiles = Arrays.asList("file1.txt", "file2.txt");
            params.nested = new NestedParams();
            params.nested.dataFile = "data.txt";
            params.nested.dataFiles = Arrays.asList("data1.txt", "data2.txt");
            params.nestedList = Arrays.asList(new NestedParams(), new NestedParams());
            params.nestedList.get(0).dataFile = "listData1.txt";
            params.nestedList.get(0).dataFiles = Arrays.asList("listData1a.txt", "listData1b.txt");
            params.nestedList.get(1).dataFile = "listData2.txt";
            params.nestedList.get(1).dataFiles = Arrays.asList("listData2a.txt", "listData2b.txt");
            return params;
        }

        public MyToolParams setInputFile(String inputFile) {
            this.inputFile = inputFile;
            return this;
        }

        public MyToolParams setOtherInputFiles(String otherInputFiles) {
            this.otherInputFiles = otherInputFiles;
            return this;
        }

        public MyToolParams setInputFiles(List<String> inputFiles) {
            this.inputFiles = inputFiles;
            return this;
        }

        public MyToolParams setNested(NestedParams nested) {
            this.nested = nested;
            return this;
        }

        public MyToolParams setNestedList(List<NestedParams> nestedList) {
            this.nestedList = nestedList;
            return this;
        }
    }

    @Test
    public void testAllToolParams() throws Exception {
        // Using reflection , find all classes that extend ToolParams
        List<Class<?>> toolParamsClasses;
        Reflections reflections = new Reflections(new ConfigurationBuilder()
                .forPackages("org.opencb.opencga") // Adjust package as needed
                .setScanners(new SubTypesScanner()));
        toolParamsClasses = Arrays.asList(reflections.getSubTypesOf(ToolParams.class).toArray(new Class[0]));
        System.out.println("Found " + toolParamsClasses.size() + " ToolParams classes:");
        toolParamsClasses.forEach(cls -> System.out.println(" - " + cls.getName()));
        assertFalse(toolParamsClasses.isEmpty());

        for (Class<?> aClass : toolParamsClasses) {
            ToolParams toolParam;
            try {
                toolParam = (ToolParams) randomInitializeClass(aClass);
            } catch (Exception e) {
                System.out.println("XXXX - Failed to initialize ToolParams class: " + aClass.getName() + " : " + e.getMessage());
                continue;
            }
            Assert.assertNotNull("Failed to initialize ToolParams class: " + aClass.getName(), toolParam);
            System.out.println("toolParam.toJson() = " + toolParam.toJson());
            JobManager.extractFileParametersBySuffix(toolParam.toParams());
        }

    }

    public static <T> T randomInitializeClass(Class<T> clazz) throws Exception {
        T instance = clazz.getDeclaredConstructor().newInstance();
        // USe jackson instrospector to set random values to all fields
        // For simplicity, we will just set some dummy values here
        // In a real scenario, you would use reflection to set all fields appropriately
        ObjectMapper objectMapper = JacksonUtils.getDefaultObjectMapper();
        System.out.println("Processing tool params class = " + clazz.getName());
        BeanDescription beanDescription = objectMapper.getSerializationConfig().introspect(objectMapper.constructType(clazz));

        for (BeanPropertyDefinition property : beanDescription.findProperties()) {
            Class<?> rawPrimaryType = property.getRawPrimaryType();
            // Set dummy values based on type
            if (rawPrimaryType == String.class) {
                property.getSetter().callOnWith(instance, "dummyValue");
            } else if (rawPrimaryType == int.class || rawPrimaryType == Integer.class) {
                property.getSetter().callOnWith(instance, 42);
            } else if (rawPrimaryType == float.class || rawPrimaryType == Float.class) {
                property.getSetter().callOnWith(instance, 42f);
            } else if (rawPrimaryType == boolean.class || rawPrimaryType == Boolean.class) {
                property.getSetter().callOnWith(instance, true);
            } else if (List.class.isAssignableFrom(rawPrimaryType)) {
                TypeBindings bindings = property.getPrimaryType().getBindings();
                if (bindings.size() == 1) {
                    Class<?> subClass = bindings.getBoundType(0).getRawClass();
                    if (subClass == String.class) {
                        property.getSetter().callOnWith(instance, Arrays.asList("listValue1", "listValue2"));
                    } else {
                        property.getSetter().callOnWith(instance, Arrays.asList(
                                randomInitializeClass(subClass),
                                randomInitializeClass(subClass)
                        ));
                    }
                } else {
                    // For other list types, just set an empty list
                    property.getSetter().callOnWith(instance, Arrays.asList());
                }
            } else if (Map.class.isAssignableFrom(rawPrimaryType)) {
                Map<String, String> map = new HashMap<>();
                map.put("key1", null);
                map.put("key2", null);
                property.getSetter().callOnWith(instance, map);
            } else if (rawPrimaryType.isEnum()) {
                // Do nothing for enums for now
            } else {
                property.getSetter().callOnWith(instance, randomInitializeClass(rawPrimaryType));
            }

        }
        return instance;
    }

    @Test
    public void testToolParamsConversion() throws CatalogException {
        MyToolParams toolParams = MyToolParams.defaultParams();
        Map<String, Object> params = toolParams.toParams();

        Map<String, String> result = JobManager.extractFileParametersBySuffix(params);

        System.out.println("result = " + result);
        result.forEach((key, value) -> System.out.println(" - " + key + " : " + value));
        assertEquals(13, result.size());
        assertTrue(result.containsKey("inputFile"));
        assertTrue(result.containsKey("inputFiles[0]"));
        assertTrue(result.containsKey("inputFiles[1]"));
        assertTrue(result.containsKey("otherInputFiles"));
        assertTrue(result.containsKey("nested.dataFile"));
        assertTrue(result.containsKey("nested.dataFiles[0]"));
        assertTrue(result.containsKey("nested.dataFiles[1]"));
        assertTrue(result.containsKey("nestedList[0].dataFile"));
        assertTrue(result.containsKey("nestedList[0].dataFiles[0]"));
        assertTrue(result.containsKey("nestedList[0].dataFiles[1]"));
        assertTrue(result.containsKey("nestedList[1].dataFile"));
        assertTrue(result.containsKey("nestedList[1].dataFiles[0]"));
        assertTrue(result.containsKey("nestedList[1].dataFiles[1]"));

    }

    // ========================================
    // C. Unit Tests for Static Methods
    // ========================================

    @Test
    public void testExtractFileParametersBySuffixSimple() throws CatalogException {
        Map<String, Object> params = new HashMap<>();
        params.put("inputFile", testFile1);
        params.put("outputFiles", testFile2);
        params.put("otherParam", "not_a_file");

        Map<String, String> result = JobManager.extractFileParametersBySuffix(params);

        assertEquals(2, result.size());
        assertTrue(result.containsKey("inputFile"));
        assertTrue(result.containsKey("outputFiles"));
        assertFalse(result.containsKey("otherParam"));
    }

    @Test
    public void testExtractFileParametersWithNumberedIndices() throws CatalogException {
        Map<String, Object> item1 = new HashMap<>();
        item1.put("file", testFile1);

        Map<String, Object> item2 = new HashMap<>();
        item2.put("file", testFile2);

        Map<String, Object> params = new HashMap<>();
        params.put("items", Arrays.asList(item1, item2));

        Map<String, String> result = JobManager.extractFileParametersBySuffix(params);

        System.out.println("result = " + result);
        // Should have paths with numbered indices
        assertTrue(result.containsKey("items[0].file"));
        assertTrue(result.containsKey("items[1].file"));
        assertEquals(testFile1, result.get("items[0].file"));
        assertEquals(testFile2, result.get("items[1].file"));
    }

    @Test
    public void testExtractFileParametersNestedPath() throws CatalogException {
        Map<String, Object> level3 = new HashMap<>();
        level3.put("dataFile", testFile1);

        Map<String, Object> level2 = new HashMap<>();
        level2.put("config", level3);

        Map<String, Object> params = new HashMap<>();
        params.put("workflow", level2);

        Map<String, String> result = JobManager.extractFileParametersBySuffix(params);

        assertTrue(result.containsKey("workflow.config.dataFile"));
        assertEquals(testFile1, result.get("workflow.config.dataFile"));
    }

    @Test
    public void testExtractFileParametersByPatternSimple() throws CatalogException {
        Map<String, Object> params = new HashMap<>();
        params.put("input1", "ocga://" + testFile1);
        params.put("input2", testFile2); // No pattern, should be excluded
        params.put("input3", "file://" + data_d1);

        Map<String, String> result = JobManager.extractFileParametersByPattern(params);

        assertEquals(2, result.size());
        assertTrue(result.containsKey("input1"));
        assertTrue(result.containsKey("input3"));
        assertFalse(result.containsKey("input2"));
    }

    @Test
    public void testExtractParametersDepthLimiting() throws CatalogException {
        // Build a structure deeper than 5 levels
        Map<String, Object> level11 = new HashMap<>();
        level11.put("level11File", testFile1);

        Map<String, Object> level10 = new HashMap<>();
        level10.put("level11", level11);
        level10.put("myFile", testFile1);

        Map<String, Object> level9 = new HashMap<>();
        level9.put("level10", level10);
        level9.put("myFile", testFile1);

        Map<String, Object> level8 = new HashMap<>();
        level8.put("level9", level9);
        level8.put("myFile", testFile1);

        Map<String, Object> level7 = new HashMap<>();
        level7.put("level8", level8);
        level7.put("myFile", testFile1);

        Map<String, Object> level6 = new HashMap<>();
        level6.put("level7", level7);

        Map<String, Object> level5 = new HashMap<>();
        level5.put("level6", level6);
        level5.put("foundFile", testFile2); // This should be found

        Map<String, Object> level4 = new HashMap<>();
        level4.put("level5", level5);

        Map<String, Object> level3 = new HashMap<>();
        level3.put("level4", level4);

        Map<String, Object> level2 = new HashMap<>();
        level2.put("level3", level3);

        Map<String, Object> level1 = new HashMap<>();
        level1.put("level2", level2);

        Map<String, Object> params = new HashMap<>();
        params.put("level1", level1);

        try {
            JobManager.extractFileParametersBySuffix(params);
            Assert.fail("Expected CatalogException due to depth limit exceeded");
        } catch (Exception e) {
            Assert.assertTrue(e.getMessage().contains("Maximum depth"));
        }
    }

    @Test
    public void testNullValuesShouldBeSkipped() throws CatalogException {
        Map<String, Object> params = new HashMap<>();
        params.put("inputFile", testFile1);
        params.put("nullFile", null);
        params.put("outputFiles", testFile2);

        Map<String, String> result = JobManager.extractFileParametersBySuffix(params);

        assertEquals(2, result.size());
        assertFalse(result.containsKey("nullFile"));
    }
}

