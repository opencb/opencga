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

package org.opencb.opencga.analysis.variant.manager.operations;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.opencb.biodata.models.variant.metadata.Aggregation;
import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.opencga.catalog.exceptions.CatalogException;
import org.opencb.opencga.core.models.file.File;
import org.opencb.opencga.core.models.file.VariantIndexStatus;
import org.opencb.opencga.core.testclassification.duration.MediumTests;
import org.opencb.opencga.storage.core.exceptions.StoragePipelineException;
import org.opencb.opencga.storage.core.variant.dummy.DummyVariantStoragePipeline;

import java.nio.file.Paths;
import java.util.Arrays;

import static org.hamcrest.CoreMatchers.containsString;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.fail;
import static org.junit.internal.matchers.ThrowableCauseMatcher.hasCause;
import static org.junit.internal.matchers.ThrowableMessageMatcher.hasMessage;
import static org.opencb.opencga.storage.core.variant.VariantStorageBaseTest.getResourceUri;

/**
 * Created on 09/01/18.
 *
 * @author Jacobo Coll &lt;jacobo167@gmail.com&gt;
 */
@Category(MediumTests.class)
public class VariantFileIndexSameNameTest extends AbstractVariantOperationManagerTest {

    private File inputFile1;
    private File inputFile2;

    @Override
    protected Aggregation getAggregation() {
        return Aggregation.NONE;
    }

    @Before
    public void before() throws Exception {
        inputFile1 = create(studyId, getResourceUri("platinum/1K.end.platinum-genomes-vcf-NA12877_S1.genome.vcf.gz", "platinum_1/1K.end.platinum-genomes-vcf-NA12877_S1.genome.vcf.gz"), "data/platinum_1/");
        inputFile2 = create(studyId, getResourceUri("platinum/1K.end.platinum-genomes-vcf-NA12877_S1.genome.vcf.gz", "platinum_2/1K.end.platinum-genomes-vcf-NA12877_S1.genome.vcf.gz"), "data/platinum_2/");

        System.out.println("inputFile1 = " + inputFile1.getUid());
        System.out.println("inputFile2 = " + inputFile2.getUid());
    }

    @Test
    public void testIndex1() throws Exception {
        indexFile(inputFile1, new QueryOptions(), outputId);
        Assert.assertEquals(VariantIndexStatus.READY, catalogManager.getFileManager().get(studyId, inputFile1.getPath(), null, sessionId).first().getInternal().getVariant().getIndex().getStatus().getId());
        Assert.assertEquals(VariantIndexStatus.NONE, catalogManager.getFileManager().get(studyId, inputFile2.getPath(), null, sessionId).first().getInternal().getVariant().getIndex().getStatus().getId());
    }

    @Test
    public void testIndex2() throws Exception {
        indexFile(inputFile2, new QueryOptions(), outputId);
        Assert.assertEquals(VariantIndexStatus.NONE, catalogManager.getFileManager().get(studyId, inputFile1.getPath(), null, sessionId).first().getInternal().getVariant().getIndex().getStatus().getId());
        Assert.assertEquals(VariantIndexStatus.READY, catalogManager.getFileManager().get(studyId, inputFile2.getPath(), null, sessionId).first().getInternal().getVariant().getIndex().getStatus().getId());
    }

    @Test
    public void testTransformTwoFiles() throws Exception {
        File transformFile1 = transformFile(inputFile1, new QueryOptions(), "data/transform_1/");
        File transformFile2 = transformFile(inputFile2, new QueryOptions(), "data/transform_2/");
    }

    @Test
    public void testTransformThreeFiles() throws Exception {
        File inputFile3 = create(studyId, getResourceUri("platinum/1K.end.platinum-genomes-vcf-NA12877_S1.genome.vcf.gz", "platinum_3/1K.end.platinum-genomes-vcf-NA12877_S1.genome.vcf.gz"), "data/platinum_3/");
        File transformFile1 = transformFile(inputFile1, new QueryOptions(), "data/transform_1/");
        File transformFile2 = transformFile(inputFile2, new QueryOptions(), "data/transform_2/");
        File transformFile3 = transformFile(inputFile3, new QueryOptions(), "data/transform_3/");
    }

    @Test
    public void testTransformTwoFilesSameFolder() throws Exception {
        File transformFile1 = transformFile(inputFile1, new QueryOptions(), "data/platinum_1/");
        File transformFile2 = transformFile(inputFile2, new QueryOptions(), "data/platinum_2/");
    }

    @Test
    public void testIndexBoth() throws Exception {
        indexFile(inputFile1, new QueryOptions(), outputId);
        thrown.expect(hasMessage(containsString("Exception executing transform")));
        thrown.expect(hasCause(hasMessage(containsString("Already loaded"))));
        indexFile(inputFile2, new QueryOptions(), outputId);
    }

    @Test
    public void testIndexBothAfterFailure() throws Exception {
        try {
            indexFile(inputFile1, new QueryOptions(DummyVariantStoragePipeline.VARIANTS_LOAD_FAIL, true), outputId);
            fail("Expected exception");
        } catch (StoragePipelineException e) {
            assertThat(e.getCause(), hasMessage(containsString("Error loading file")));
        }

        String outputId2 = catalogManager.getFileManager().createFolder(studyFqn, Paths.get("data", "index2").toString(), true, null,
                QueryOptions.empty(), sessionId).first().getId();

        indexFile(inputFile2, new QueryOptions(DummyVariantStoragePipeline.VARIANTS_LOAD_FAIL, false), outputId2);
    }

    @Test
    public void testIndexBothSameTime() throws Exception {
        thrown.expect(CatalogException.class);
//        thrown.expectCause(isA(StoragePipelineException.class));
        thrown.expectMessage("Unable to INDEX multiple files with the same name");
        indexFiles(Arrays.asList(inputFile1, inputFile2), new QueryOptions(), outputId);
    }

    @Test
    public void testBySteps1() throws Exception {
        File transformFile = transformFile(inputFile1, new QueryOptions());
        Assert.assertEquals(VariantIndexStatus.TRANSFORMED, catalogManager.getFileManager().get(studyId, inputFile1.getPath(), null, sessionId).first().getInternal().getVariant().getIndex().getStatus().getId());
        Assert.assertEquals(VariantIndexStatus.NONE, catalogManager.getFileManager().get(studyId, inputFile2.getPath(), null, sessionId).first().getInternal().getVariant().getIndex().getStatus().getId());
        loadFile(transformFile, new QueryOptions(), outputId);
        Assert.assertEquals(VariantIndexStatus.READY, catalogManager.getFileManager().get(studyId, inputFile1.getPath(), null, sessionId).first().getInternal().getVariant().getIndex().getStatus().getId());
        Assert.assertEquals(VariantIndexStatus.NONE, catalogManager.getFileManager().get(studyId, inputFile2.getPath(), null, sessionId).first().getInternal().getVariant().getIndex().getStatus().getId());
    }

    @Test
    public void testBySteps2() throws Exception {
        File transformFile = transformFile(inputFile2, new QueryOptions());
        Assert.assertEquals(VariantIndexStatus.NONE, catalogManager.getFileManager().get(studyId, inputFile1.getPath(), null, sessionId).first().getInternal().getVariant().getIndex().getStatus().getId());
        Assert.assertEquals(VariantIndexStatus.TRANSFORMED, catalogManager.getFileManager().get(studyId, inputFile2.getPath(), null, sessionId).first().getInternal().getVariant().getIndex().getStatus().getId());
        loadFile(transformFile, new QueryOptions(), outputId);
        Assert.assertEquals(VariantIndexStatus.NONE, catalogManager.getFileManager().get(studyId, inputFile1.getPath(), null, sessionId).first().getInternal().getVariant().getIndex().getStatus().getId());
        Assert.assertEquals(VariantIndexStatus.READY, catalogManager.getFileManager().get(studyId, inputFile2.getPath(), null, sessionId).first().getInternal().getVariant().getIndex().getStatus().getId());
    }
}
