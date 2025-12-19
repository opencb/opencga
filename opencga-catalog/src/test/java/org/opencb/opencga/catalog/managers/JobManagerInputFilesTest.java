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

import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.opencb.opencga.catalog.exceptions.CatalogException;
import org.opencb.opencga.core.models.file.File;
import org.opencb.opencga.core.models.job.Job;
import org.opencb.opencga.core.testclassification.duration.MediumTests;

import java.util.*;

import static org.junit.Assert.*;

/**
 * Test suite for Job Input Files Detection functionality given the map of Job params.
 * Tests both suffix-based and pattern-based file detection with deep nesting support.
 */
@Category(MediumTests.class)
public class JobManagerInputFilesTest extends AbstractManagerTest {

    // ========================================
    // A. Suffix-Based Tests (getJobInputFilesFromParams)
    // ========================================

    @Test
    public void testSimpleFileSuffix() throws CatalogException {
        Map<String, Object> params = new HashMap<>();
        params.put("inputFile", testFile1);

        Job job = new Job().setParams(params);
        List<File> result = catalogManager.getJobManager().getJobInputFilesFromParams(studyFqn, job, ownerToken);

        assertEquals(1, result.size());
        assertEquals(testFile1, result.get(0).getPath());
    }

    @Test
    public void testSimpleFilesSuffix() throws CatalogException {
        Map<String, Object> params = new HashMap<>();
        params.put("inputFiles", testFile1);

        Job job = new Job().setParams(params);
        List<File> result = catalogManager.getJobManager().getJobInputFilesFromParams(studyFqn, job, ownerToken);

        assertEquals(1, result.size());
        assertEquals(testFile1, result.get(0).getPath());
    }

    @Test
    public void testSimpleDirSuffix() throws CatalogException {
        Map<String, Object> params = new HashMap<>();
        params.put("inputDir", data_d1);

        Job job = new Job().setParams(params);
        List<File> result = catalogManager.getJobManager().getJobInputFilesFromParams(studyFqn, job, ownerToken);

        assertEquals(1, result.size());
        assertEquals(data_d1, result.get(0).getPath());
    }

    @Test
    public void testCaseInsensitiveSuffix() throws CatalogException {
        Map<String, Object> params = new HashMap<>();
        params.put("inputFILE", testFile1);
        params.put("outputFiles", testFile2);
        params.put("workingDIR", data_d1);

        Job job = new Job().setParams(params);
        List<File> result = catalogManager.getJobManager().getJobInputFilesFromParams(studyFqn, job, ownerToken);

        assertEquals(3, result.size());
    }

    @Test
    public void testNestedMapLevel1() throws CatalogException {
        Map<String, Object> nested = new HashMap<>();
        nested.put("inputFile", testFile1);

        Map<String, Object> params = new HashMap<>();
        params.put("params", nested);

        Job job = new Job().setParams(params);
        List<File> result = catalogManager.getJobManager().getJobInputFilesFromParams(studyFqn, job, ownerToken);

        assertEquals(1, result.size());
        assertEquals(testFile1, result.get(0).getPath());
    }

    @Test
    public void testNestedMapLevel2() throws CatalogException {
        Map<String, Object> level2 = new HashMap<>();
        level2.put("inputFile", testFile1);

        Map<String, Object> level1 = new HashMap<>();
        level1.put("nested", level2);

        Map<String, Object> params = new HashMap<>();
        params.put("config", level1);

        Job job = new Job().setParams(params);
        List<File> result = catalogManager.getJobManager().getJobInputFilesFromParams(studyFqn, job, ownerToken);

        assertEquals(1, result.size());
        assertEquals(testFile1, result.get(0).getPath());
    }

    @Test
    public void testNestedMapLevel5() throws CatalogException {
        Map<String, Object> level5 = new HashMap<>();
        level5.put("dataFile", testFile1);

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

        Job job = new Job().setParams(params);
        List<File> result = catalogManager.getJobManager().getJobInputFilesFromParams(studyFqn, job, ownerToken);

        assertEquals(1, result.size());
        assertEquals(testFile1, result.get(0).getPath());
    }

    @Test
    public void testNestedMapLevel6ShouldStopAt5() throws CatalogException {
        // Level 6 should not be traversed (max depth is 5)
        Map<String, Object> level6 = new HashMap<>();
        level6.put("deepFile", testFile2);

        Map<String, Object> level5 = new HashMap<>();
        level5.put("level6", level6);
        level5.put("file", testFile1); // This should be found at level 5

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

        Job job = new Job().setParams(params);
        List<File> result = catalogManager.getJobManager().getJobInputFilesFromParams(studyFqn, job, ownerToken);

        // Should only find the file at level 5, not level 6
        assertEquals(1, result.size());
        assertEquals(testFile1, result.get(0).getPath());
    }

    @Test
    public void testListOfStrings() throws CatalogException {
        Map<String, Object> params = new HashMap<>();
        params.put("inputFiles", Arrays.asList(testFile1, testFile2));

        Job job = new Job().setParams(params);
        List<File> result = catalogManager.getJobManager().getJobInputFilesFromParams(studyFqn, job, ownerToken);

        assertEquals(2, result.size());
        assertTrue(result.stream().anyMatch(f -> f.getPath().equals(testFile1)));
        assertTrue(result.stream().anyMatch(f -> f.getPath().equals(testFile2)));
    }

    @Test
    public void testCommaSeparatedStrings() throws CatalogException {
        Map<String, Object> params = new HashMap<>();
        params.put("inputFile", testFile1 + "," + testFile2);

        Job job = new Job().setParams(params);
        List<File> result = catalogManager.getJobManager().getJobInputFilesFromParams(studyFqn, job, ownerToken);

        assertEquals(2, result.size());
        assertTrue(result.stream().anyMatch(f -> f.getPath().equals(testFile1)));
        assertTrue(result.stream().anyMatch(f -> f.getPath().equals(testFile2)));
    }

    @Test
    public void testEmptyListShouldBeSkipped() throws CatalogException {
        Map<String, Object> params = new HashMap<>();
        params.put("inputFiles", Collections.emptyList());
        params.put("otherFile", testFile1);

        Job job = new Job().setParams(params);
        List<File> result = catalogManager.getJobManager().getJobInputFilesFromParams(studyFqn, job, ownerToken);

        // Should only find the non-empty param
        assertEquals(1, result.size());
        assertEquals(testFile1, result.get(0).getPath());
    }

    @Test
    public void testListOfMaps() throws CatalogException {
        Map<String, Object> item1 = new HashMap<>();
        item1.put("file", testFile1);

        Map<String, Object> item2 = new HashMap<>();
        item2.put("file", testFile2);

        List<Map<String, Object>> items = Arrays.asList(item1, item2);

        Map<String, Object> params = new HashMap<>();
        params.put("items", items);

        Job job = new Job().setParams(params);
        List<File> result = catalogManager.getJobManager().getJobInputFilesFromParams(studyFqn, job, ownerToken);

        assertEquals(2, result.size());
        assertTrue(result.stream().anyMatch(f -> f.getPath().equals(testFile1)));
        assertTrue(result.stream().anyMatch(f -> f.getPath().equals(testFile2)));
    }

    @Test
    public void testMapContainingListOfMaps() throws CatalogException {
        Map<String, Object> innerMap1 = new HashMap<>();
        innerMap1.put("dataFile", testFile1);

        Map<String, Object> innerMap2 = new HashMap<>();
        innerMap2.put("dataFile", testFile2);

        List<Map<String, Object>> list = Arrays.asList(innerMap1, innerMap2);

        Map<String, Object> nested = new HashMap<>();
        nested.put("refs", list);

        Map<String, Object> params = new HashMap<>();
        params.put("nested", nested);

        Job job = new Job().setParams(params);
        List<File> result = catalogManager.getJobManager().getJobInputFilesFromParams(studyFqn, job, ownerToken);

        assertEquals(2, result.size());
        assertTrue(result.stream().anyMatch(f -> f.getPath().equals(testFile1)));
        assertTrue(result.stream().anyMatch(f -> f.getPath().equals(testFile2)));
    }

    @Test
    public void testAllSpecialValueShouldBeSkipped() throws CatalogException {
        Map<String, Object> params = new HashMap<>();
        params.put("inputFile", "ALL");

        Job job = new Job().setParams(params);
        List<File> result = catalogManager.getJobManager().getJobInputFilesFromParams(studyFqn, job, ownerToken);

        assertEquals(0, result.size());
    }

    @Test
    public void testNoneSpecialValueShouldBeSkipped() throws CatalogException {
        Map<String, Object> params = new HashMap<>();
        params.put("inputFile", "NONE");

        Job job = new Job().setParams(params);
        List<File> result = catalogManager.getJobManager().getJobInputFilesFromParams(studyFqn, job, ownerToken);

        assertEquals(0, result.size());
    }

    @Test
    public void testCommaSeparatedWithRealFile() throws CatalogException {
        // When there are multiple files, ALL/NONE should not be skipped
        Map<String, Object> params = new HashMap<>();
        params.put("inputFile", testFile1 + ",ALL");

        Job job = new Job().setParams(params);

        // This should try to find "ALL" as a real file and fail
        thrown.expect(CatalogException.class);
        thrown.expectMessage("ALL");
        catalogManager.getJobManager().getJobInputFilesFromParams(studyFqn, job, ownerToken);
    }

    @Test
    public void testNonExistentFileShouldThrowWithFullPath() throws CatalogException {
        Map<String, Object> nested = new HashMap<>();
        nested.put("inputFile", "non_existent_file.txt");

        Map<String, Object> params = new HashMap<>();
        params.put("config", nested);

        Job job = new Job().setParams(params);

        thrown.expect(CatalogException.class);
        thrown.expectMessage("config.inputFile");
        thrown.expectMessage("non_existent_file.txt");
        catalogManager.getJobManager().getJobInputFilesFromParams(studyFqn, job, ownerToken);
    }

    @Test
    public void testNullParamsShouldReturnEmpty() throws CatalogException {
        Job job = new Job().setParams(null);
        List<File> result = catalogManager.getJobManager().getJobInputFilesFromParams(studyFqn, job, ownerToken);

        assertEquals(0, result.size());
    }

    @Test
    public void testEmptyParamsShouldReturnEmpty() throws CatalogException {
        Job job = new Job().setParams(new HashMap<>());
        List<File> result = catalogManager.getJobManager().getJobInputFilesFromParams(studyFqn, job, ownerToken);

        assertEquals(0, result.size());
    }

    // ========================================
    // B. Pattern-Based Tests (getWorkflowJobInputFilesFromParams)
    // ========================================

    @Test
    public void testOcgaProtocol() throws CatalogException {
        Map<String, Object> params = new HashMap<>();
        params.put("anyParam", "ocga://" + testFile1);

        Job job = new Job().setParams(params);
        List<File> result = catalogManager.getJobManager().getWorkflowJobInputFilesFromParams(studyFqn, job, ownerToken);

        assertEquals(1, result.size());
        assertEquals(testFile1, result.get(0).getPath());
    }

    @Test
    public void testOpencgaProtocol() throws CatalogException {
        Map<String, Object> params = new HashMap<>();
        params.put("someInput", "opencga://" + testFile1);

        Job job = new Job().setParams(params);
        List<File> result = catalogManager.getJobManager().getWorkflowJobInputFilesFromParams(studyFqn, job, ownerToken);

        assertEquals(1, result.size());
        assertEquals(testFile1, result.get(0).getPath());
    }

    @Test
    public void testFileProtocol() throws CatalogException {
        Map<String, Object> params = new HashMap<>();
        params.put("dataPath", "file://" + testFile1);

        Job job = new Job().setParams(params);
        List<File> result = catalogManager.getJobManager().getWorkflowJobInputFilesFromParams(studyFqn, job, ownerToken);

        assertEquals(1, result.size());
        assertEquals(testFile1, result.get(0).getPath());
    }

    @Test
    public void testPatternNestedLevel1() throws CatalogException {
        Map<String, Object> nested = new HashMap<>();
        nested.put("input", "ocga://" + testFile1);

        Map<String, Object> params = new HashMap<>();
        params.put("config", nested);

        Job job = new Job().setParams(params);
        List<File> result = catalogManager.getJobManager().getWorkflowJobInputFilesFromParams(studyFqn, job, ownerToken);

        assertEquals(1, result.size());
        assertEquals(testFile1, result.get(0).getPath());
    }

    @Test
    public void testPatternNestedLevel5() throws CatalogException {
        Map<String, Object> level5 = new HashMap<>();
        level5.put("data", "file://" + testFile1);

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

        Job job = new Job().setParams(params);
        List<File> result = catalogManager.getJobManager().getWorkflowJobInputFilesFromParams(studyFqn, job, ownerToken);

        assertEquals(1, result.size());
        assertEquals(testFile1, result.get(0).getPath());
    }

    @Test
    public void testListOfOpenCGAPaths() throws CatalogException {
        Map<String, Object> params = new HashMap<>();
        params.put("inputs", Arrays.asList("ocga://" + testFile1, "opencga://" + testFile2));

        Job job = new Job().setParams(params);
        List<File> result = catalogManager.getJobManager().getWorkflowJobInputFilesFromParams(studyFqn, job, ownerToken);

        assertEquals(2, result.size());
        assertTrue(result.stream().anyMatch(f -> f.getPath().equals(testFile1)));
        assertTrue(result.stream().anyMatch(f -> f.getPath().equals(testFile2)));
    }

    @Test
    public void testPartialListMatchShouldFail() throws CatalogException {
        Map<String, Object> params = new HashMap<>();
        // Mix of OpenCGA paths and regular paths - should fail
        params.put("inputs", Arrays.asList("ocga://" + testFile1, testFile2));

        Job job = new Job().setParams(params);

        thrown.expect(CatalogException.class);
        thrown.expectMessage("inputs");
        thrown.expectMessage("only some elements match");
        thrown.expectMessage(testFile2);
        catalogManager.getJobManager().getWorkflowJobInputFilesFromParams(studyFqn, job, ownerToken);
    }

    @Test
    public void testInvalidOpenCGAPathShouldThrow() throws CatalogException {
        Map<String, Object> nested = new HashMap<>();
        nested.put("data", "ocga://non_existent_file.vcf");

        Map<String, Object> params = new HashMap<>();
        params.put("workflow", nested);

        Job job = new Job().setParams(params);

        thrown.expect(CatalogException.class);
        thrown.expectMessage("workflow.data");
        thrown.expectMessage("non_existent_file.vcf");
        catalogManager.getJobManager().getWorkflowJobInputFilesFromParams(studyFqn, job, ownerToken);
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

        Map<String, List<String>> result = JobManager.extractFileParametersBySuffix("", new LinkedHashMap<>(), params);

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

        Map<String, List<String>> result = JobManager.extractFileParametersBySuffix("", new LinkedHashMap<>(), params);

        // Should have paths with numbered indices
        assertTrue(result.containsKey("items[0].file"));
        assertTrue(result.containsKey("items[1].file"));
        assertEquals(testFile1, result.get("items[0].file").get(0));
        assertEquals(testFile2, result.get("items[1].file").get(0));
    }

    @Test
    public void testExtractFileParametersNestedPath() throws CatalogException {
        Map<String, Object> level3 = new HashMap<>();
        level3.put("dataFile", testFile1);

        Map<String, Object> level2 = new HashMap<>();
        level2.put("config", level3);

        Map<String, Object> params = new HashMap<>();
        params.put("workflow", level2);

        Map<String, List<String>> result = JobManager.extractFileParametersBySuffix("", new LinkedHashMap<>(), params);

        assertTrue(result.containsKey("workflow.config.dataFile"));
        assertEquals(testFile1, result.get("workflow.config.dataFile").get(0));
    }

    @Test
    public void testExtractFileParametersByPatternSimple() throws CatalogException {
        Map<String, Object> params = new HashMap<>();
        params.put("input1", "ocga://" + testFile1);
        params.put("input2", testFile2); // No pattern, should be excluded
        params.put("input3", "file://" + data_d1);

        Map<String, List<String>> result = JobManager.extractFileParametersByPattern("", new LinkedHashMap<>(), params);

        assertEquals(2, result.size());
        assertTrue(result.containsKey("input1"));
        assertTrue(result.containsKey("input3"));
        assertFalse(result.containsKey("input2"));
    }

    @Test
    public void testExtractParametersDepthLimiting() throws CatalogException {
        // Build a structure deeper than 5 levels
        Map<String, Object> level7 = new HashMap<>();
        level7.put("deepFile", testFile1);

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

        Map<String, List<String>> result = JobManager.extractFileParametersBySuffix("", new LinkedHashMap<>(), params);

        // Should find the file at level 5 but not at level 7
        assertTrue(result.containsKey("level1.level2.level3.level4.level5.foundFile"));
        assertFalse(result.containsKey("level1.level2.level3.level4.level5.level6.level7.deepFile"));
    }

    @Test
    public void testNullValuesShouldBeSkipped() throws CatalogException {
        Map<String, Object> params = new HashMap<>();
        params.put("inputFile", testFile1);
        params.put("nullFile", null);
        params.put("outputFiles", testFile2);

        Map<String, List<String>> result = JobManager.extractFileParametersBySuffix("", new LinkedHashMap<>(), params);

        assertEquals(2, result.size());
        assertFalse(result.containsKey("nullFile"));
    }
}

