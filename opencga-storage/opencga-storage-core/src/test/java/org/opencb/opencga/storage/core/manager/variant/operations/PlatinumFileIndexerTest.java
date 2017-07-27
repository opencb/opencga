/*
 * Copyright 2015-2017 OpenCB
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

package org.opencb.opencga.storage.core.manager.variant.operations;

import org.junit.Before;
import org.junit.Test;
import org.opencb.biodata.models.variant.VariantSource;
import org.opencb.commons.datastore.core.Query;
import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.opencga.catalog.exceptions.CatalogException;
import org.opencb.opencga.catalog.models.File;
import org.opencb.opencga.storage.core.manager.variant.AbstractVariantStorageOperationTest;
import org.opencb.opencga.storage.core.variant.adaptors.VariantQueryParam;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;

/**
 * Created on 15/07/16
 *
 * @author Jacobo Coll &lt;jacobo167@gmail.com&gt;
 */
public class PlatinumFileIndexerTest extends AbstractVariantStorageOperationTest {

    private Logger logger = LoggerFactory.getLogger(AbstractVariantStorageOperationTest.class);

    @Override
    protected VariantSource.Aggregation getAggregation() {
        return VariantSource.Aggregation.NONE;
    }

    @Before
    public void before() throws CatalogException {
    }

    @Test
    public void testBySteps() throws Exception {

        File inputFile;
        File transformFile;
        for (int i = 77; i <= 93; i++) {
            inputFile = create("platinum/1K.end.platinum-genomes-vcf-NA128" + i + "_S1.genome.vcf.gz");
            transformFile = transformFile(inputFile, new QueryOptions());
            loadFile(transformFile, new QueryOptions(), outputId);
        }


        variantManager.iterator(new Query(VariantQueryParam.STUDIES.key(), studyId), new QueryOptions(), sessionId).forEachRemaining(variant -> {
            System.out.println("variant = " + variant);
        });
    }

    @Test
    public void testBatch() throws Exception {

        List<File> files = new ArrayList<>();
        for (int i = 77; i <= 93; i++) {
            files.add(create("platinum/1K.end.platinum-genomes-vcf-NA128" + i + "_S1.genome.vcf.gz"));
        }
        indexFiles(files, new QueryOptions(), outputId);

        variantManager.iterator(new Query(VariantQueryParam.STUDIES.key(), studyId), new QueryOptions(), sessionId).forEachRemaining(variant -> {
            System.out.println("variant = " + variant);
        });
    }
    @Test
    public void testBatchBySteps() throws Exception {

        File inputFile;
        List<File> files = new ArrayList<>();
        for (int i = 77; i <= 93; i++) {
            inputFile = create("platinum/1K.end.platinum-genomes-vcf-NA128" + i + "_S1.genome.vcf.gz");
            files.add(transformFile(inputFile, new QueryOptions()));
        }
        loadFiles(files, new QueryOptions(), outputId);

        variantManager.iterator(new Query(VariantQueryParam.STUDIES.key(), studyId), new QueryOptions(), sessionId).forEachRemaining(variant -> {
            System.out.println("variant = " + variant);
        });
    }

}
