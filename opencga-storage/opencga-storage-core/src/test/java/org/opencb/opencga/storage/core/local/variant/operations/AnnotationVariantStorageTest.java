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

package org.opencb.opencga.storage.core.local.variant.operations;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.opencb.biodata.models.variant.Variant;
import org.opencb.biodata.models.variant.VariantSource;
import org.opencb.commons.datastore.core.Query;
import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.opencga.catalog.db.api.FileDBAdaptor;
import org.opencb.opencga.catalog.models.File;
import org.opencb.opencga.catalog.models.Job;
import org.opencb.opencga.catalog.monitor.executors.old.ExecutorManager;
import org.opencb.opencga.storage.core.config.StorageConfiguration;
import org.opencb.opencga.storage.core.local.OpenCGATestExternalResource;
import org.opencb.opencga.storage.core.local.variant.AbstractVariantStorageOperationTest;
import org.opencb.opencga.storage.core.variant.adaptors.VariantDBAdaptor;
import org.opencb.opencga.storage.core.variant.adaptors.VariantDBIterator;
import org.opencb.opencga.storage.core.variant.annotation.VariantAnnotationManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.function.Function;

/**
 * Created on 18/05/16
 *
 * @author Jacobo Coll &lt;jacobo167@gmail.com&gt;
 */
public class AnnotationVariantStorageTest extends AbstractVariantStorageOperationTest {

    @Rule
    public OpenCGATestExternalResource opencga = new OpenCGATestExternalResource();

    protected StorageConfiguration storageConfiguration;
    protected Logger logger = LoggerFactory.getLogger(StatsVariantStorageTest.class);

    private final String userId = "user";
    private final String dbName = "opencga_variants_test";

    @Before
    public void setUp() throws Exception {
        catalogManager.createUser(userId, "User", "user@email.org", "user", "ACME", null, null).first();


        File file = opencga.createFile(studyId, "variant-test-file.vcf.gz", sessionId);

        variantManager.index(String.valueOf(file.getId()), opencga.createTmpOutdir(studyId, "index", sessionId),
                String.valueOf(outputId), new QueryOptions(), sessionId);
    }

    @Test
    public void testAnnotateDefault() throws Exception {

        VariantStatsStorageOperation variantStorage = new VariantStatsStorageOperation(catalogManager, storageConfiguration);

        VariantAnnotationStorageOperation annotOp = new VariantAnnotationStorageOperation(catalogManager, storageConfiguration);
        annotOp.annotateVariants(studyId, new Query(), null, String.valueOf(outputId), sessionId, new QueryOptions(ExecutorManager.EXECUTE, true));

        checkAnnotation(v -> true);
    }

    @Test
    public void testAnnotateRegion() throws Exception {

        VariantStatsStorageOperation variantStorage = new VariantStatsStorageOperation(catalogManager, storageConfiguration);

        VariantAnnotationStorageOperation annotOp1 = new VariantAnnotationStorageOperation(catalogManager, storageConfiguration);
        annotOp1.annotateVariants(studyId, new Query(), null, String.valueOf(outputId), sessionId, new QueryOptions(VariantDBAdaptor.VariantQueryParams.CHROMOSOME.key(), "22")
                .append(ExecutorManager.EXECUTE, true));

        checkAnnotation(v -> v.getChromosome().equals("22"));

        VariantAnnotationStorageOperation annotOp = new VariantAnnotationStorageOperation(catalogManager, storageConfiguration);
        annotOp.annotateVariants(studyId, new Query(), null, String.valueOf(outputId), sessionId, new QueryOptions(VariantDBAdaptor.VariantQueryParams.CHROMOSOME.key(), "1")
                .append(ExecutorManager.EXECUTE, true));

        checkAnnotation(v -> v.getChromosome().equals("22") || v.getChromosome().equals("1"));
    }

    @Test
    public void testAnnotateCreateAndLoad() throws Exception {

        VariantAnnotationStorageOperation annotOp = new VariantAnnotationStorageOperation(catalogManager, storageConfiguration);
        annotOp.annotateVariants(studyId, new Query(), null, String.valueOf(outputId), sessionId,
                new QueryOptions()
//                        .append(VariantAnnotationManager.CREATE, true)
        );
        Job job = null;

        System.out.println("job = " + job);
        File annotFile = catalogManager.searchFile(studyId,
                new Query(FileDBAdaptor.QueryParams.ID.key(), job.getOutput())
                        .append(FileDBAdaptor.QueryParams.FORMAT.key(), File.Format.JSON), null, sessionId).first();

        checkAnnotation(v -> false);

        annotOp.annotateVariants(studyId, new Query(), null, String.valueOf(outputId), sessionId,
                new QueryOptions(VariantAnnotationManager.LOAD_FILE, annotFile.getId()));
        job = null;
        System.out.println("job = " + job);

        checkAnnotation(v -> true);
    }

    @Test
    public void testCustomAnnotation() throws Exception {

        variantManager.annotate(String.valueOf(studyId), new Query(), opencga.createTmpOutdir(studyId, "annot", sessionId),
                String.valueOf(outputId), new QueryOptions(), sessionId);

        checkAnnotation(v -> true);

        File file = opencga.createFile(studyId, "custom_annotation/myannot.gff", sessionId);
        QueryOptions options = new QueryOptions()
                .append(VariantAnnotationManager.LOAD_FILE, file.getId())
                .append(VariantAnnotationManager.CUSTOM_ANNOTATION_KEY, "myAnnot");
        variantManager.annotate(String.valueOf(studyId), new Query(), opencga.createTmpOutdir(studyId, "annot", sessionId),
                String.valueOf(outputId), options, sessionId);

        Assert.assertEquals(Collections.singleton("myAnnot"), checkAnnotation(v -> true));

        file = opencga.createFile(studyId, "custom_annotation/myannot.bed", sessionId);
        options = new QueryOptions()
                .append(VariantAnnotationManager.LOAD_FILE, file.getId())
                .append(VariantAnnotationManager.CUSTOM_ANNOTATION_KEY, "myAnnot2");
        variantManager.annotate(String.valueOf(studyId), new Query(), opencga.createTmpOutdir(studyId, "annot", sessionId),
                String.valueOf(outputId), options, sessionId);

        Assert.assertEquals(new HashSet<>(Arrays.asList("myAnnot", "myAnnot2")), checkAnnotation(v -> true));
    }

    public Set<String> checkAnnotation(Function<Variant, Boolean> contains) throws Exception {

        try (VariantDBIterator iterator = variantManager.iterator(new Query(VariantDBAdaptor.VariantQueryParams.STUDIES.key(), studyId),
                new QueryOptions(QueryOptions.SORT, true), sessionId)) {

            Set<String> customAnnotationKeySet = new LinkedHashSet<>();
            int c = 0;
            while (iterator.hasNext()) {
                c++;
                Variant next = iterator.next();
                if (contains.apply(next)) {
                    Assert.assertNotNull(next.getAnnotation());
                    if (next.getAnnotation().getAdditionalAttributes() != null) {
                        customAnnotationKeySet.addAll(next.getAnnotation().getAdditionalAttributes().keySet());
                    }
                } else {
                    Assert.assertNull(next.getAnnotation());
                }
            }
            Assert.assertTrue(c > 0);
            return customAnnotationKeySet;
        }
    }

    @Override
    protected VariantSource.Aggregation getAggregation() {
        return VariantSource.Aggregation.NONE;
    }
}
