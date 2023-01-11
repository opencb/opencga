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

import org.junit.Test;
import org.opencb.biodata.models.variant.metadata.Aggregation;
import org.opencb.commons.datastore.core.ObjectMap;
import org.opencb.opencga.analysis.tools.ToolRunner;
import org.opencb.opencga.analysis.variant.operations.VariantIndexOperationTool;
import org.opencb.opencga.catalog.managers.FileUtils;
import org.opencb.opencga.core.api.ParamConstants;
import org.opencb.opencga.core.models.file.File;
import org.opencb.opencga.core.models.file.FileLinkParams;
import org.opencb.opencga.core.models.variant.VariantIndexParams;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.file.Paths;

import static org.junit.Assert.*;
import static org.opencb.opencga.storage.core.variant.VariantStorageBaseTest.getResourceUri;

/**
 * Created on 15/07/16
 *
 * @author Jacobo Coll &lt;jacobo167@gmail.com&gt;
 */
public class VariantVirtualFileIndexTest extends AbstractVariantOperationManagerTest {

    private Logger logger = LoggerFactory.getLogger(AbstractVariantOperationManagerTest.class);

    @Override
    protected Aggregation getAggregation() {
        return Aggregation.NONE;
    }

    @Test
    public void testIndexVirtual() throws Exception {
        String path = "data/vcfs/";
        String virtualFile = "variant-test-file.vcf";
        catalogManager.getFileManager().link(studyId, new FileLinkParams()
                .setUri(getResourceUri("by_chr/chr20.variant-test-file.vcf.gz").toString())
                .setVirtualFileName(virtualFile)
                .setPath(path), true, sessionId).first();
        catalogManager.getFileManager().link(studyId, new FileLinkParams()
                .setUri(getResourceUri("by_chr/chr21.variant-test-file.vcf.gz").toString())
                .setVirtualFileName(virtualFile)
                .setPath(path), true, sessionId).first();
        catalogManager.getFileManager().link(studyId, new FileLinkParams()
                .setUri(getResourceUri("by_chr/chr22.variant-test-file.vcf.gz").toString())
                .setVirtualFileName(virtualFile)
                .setPath(path), true, sessionId).first();

        ToolRunner toolRunner = opencga.getToolRunner();
        toolRunner.execute(VariantIndexOperationTool.class, new VariantIndexParams()
                        .setFile("chr20.variant-test-file.vcf.gz")
                        .setLoadSplitData("REGION")
                , new ObjectMap(ParamConstants.STUDY_PARAM, studyId), Paths.get(opencga.createTmpOutdir()), null, sessionId);
        toolRunner.execute(VariantIndexOperationTool.class, new VariantIndexParams()
                        .setFile("chr21.variant-test-file.vcf.gz")
                        .setLoadSplitData("REGION")
                , new ObjectMap(ParamConstants.STUDY_PARAM, studyId), Paths.get(opencga.createTmpOutdir()), null, sessionId);
        toolRunner.execute(VariantIndexOperationTool.class, new VariantIndexParams()
                        .setFile("chr22.variant-test-file.vcf.gz")
                        .setLoadSplitData("REGION")
                , new ObjectMap(ParamConstants.STUDY_PARAM, studyId), Paths.get(opencga.createTmpOutdir()), null, sessionId);

        File file = catalogManager.getFileManager().get(studyId, "chr20.variant-test-file.vcf.gz", null, sessionId).first();
        assertTrue(FileUtils.isPartial(file));
        assertEquals("READY", file.getInternal().getVariant().getIndex().getStatus().getId());

        file = catalogManager.getFileManager().get(studyId, "variant-test-file.vcf", null, sessionId).first();
        assertFalse(FileUtils.isPartial(file));
        assertEquals(File.Type.VIRTUAL, file.getType());
        assertEquals("READY", file.getInternal().getVariant().getIndex().getStatus().getId());
    }

}
