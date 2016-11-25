/*
 * Copyright 2015-2016 OpenCB
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

package org.opencb.opencga.analysis.storage.variant;

import org.junit.Test;
import org.opencb.biodata.models.variant.VariantSource;
import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.opencga.analysis.storage.AbstractAnalysisFileIndexerTest;
import org.opencb.opencga.storage.core.local.variant.operations.VariantFileIndexerStorageOperation;
import org.opencb.opencga.catalog.models.File;
import org.opencb.opencga.catalog.models.FileIndex;

import java.nio.file.Files;
import java.nio.file.Path;

import static org.junit.Assert.*;
import static org.opencb.opencga.storage.core.local.variant.operations.VariantFileIndexerStorageOperation.*;

/**
 * Created on 19/09/16
 *
 * @author Jacobo Coll &lt;jacobo167@gmail.com&gt;
 */
public class VariantFileIndexerTest extends AbstractAnalysisFileIndexerTest {

    @Test
    public void testIndex() throws Exception {

        VariantFileIndexerStorageOperation variantFileIndexer = new VariantFileIndexerStorageOperation(opencga.getCatalogManager().getCatalogConfiguration(),
                opencga.getStorageConfiguration());

        File file = create("variant-test-file.vcf.gz");
        Path outdir1 = opencga.getOpencgaHome().resolve("job1");
        Files.createDirectory(outdir1);
        Path outdir2 = opencga.getOpencgaHome().resolve("job2");
        Files.createDirectory(outdir2);

        variantFileIndexer.index(file.getName(), outdir1.toString(), sessionId, new QueryOptions(TRANSFORM, true).append(CATALOG_PATH, "data/"));
        file = opencga.getCatalogManager().getFile(file.getId(), sessionId).first();
        assertEquals(FileIndex.IndexStatus.TRANSFORMED, file.getIndex().getStatus().getName());

        variantFileIndexer.index(file.getName(), outdir2.toString(), sessionId, new QueryOptions(LOAD, true).append(CATALOG_PATH, "data/"));
        file = opencga.getCatalogManager().getFile(file.getId(), sessionId).first();
        assertEquals(FileIndex.IndexStatus.READY, file.getIndex().getStatus().getName());
    }

    @Override
    protected VariantSource.Aggregation getAggregation() {
        return VariantSource.Aggregation.NONE;
    }
}