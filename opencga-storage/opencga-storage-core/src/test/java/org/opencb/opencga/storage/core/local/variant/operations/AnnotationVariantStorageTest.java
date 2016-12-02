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
import org.junit.Test;
import org.opencb.biodata.models.variant.Variant;
import org.opencb.biodata.models.variant.VariantSource;
import org.opencb.commons.datastore.core.Query;
import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.opencga.catalog.exceptions.CatalogException;
import org.opencb.opencga.catalog.models.File;
import org.opencb.opencga.storage.core.exceptions.StorageManagerException;
import org.opencb.opencga.storage.core.local.variant.AbstractVariantStorageOperationTest;
import org.opencb.opencga.storage.core.variant.adaptors.VariantDBAdaptor;
import org.opencb.opencga.storage.core.variant.annotation.VariantAnnotationManager;
import org.opencb.opencga.storage.core.variant.dummy.DummyVariantDBAdaptor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.URISyntaxException;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.function.Function;

import static org.mockito.Mockito.*;

/**
 * Created on 18/05/16
 *
 * @author Jacobo Coll &lt;jacobo167@gmail.com&gt;
 */
public class AnnotationVariantStorageTest extends AbstractVariantStorageOperationTest {

    protected Logger logger = LoggerFactory.getLogger(StatsVariantStorageTest.class);

    @Before
    public void setUp() throws Exception {
        File file = opencga.createFile(studyId, "variant-test-file.vcf.gz", sessionId);

        indexFile(file, new QueryOptions(), outputId);
    }

    @Test
    public void testAnnotateDefault() throws Exception {
        annotate(new Query(), new QueryOptions());

        checkAnnotation(v -> true);
    }

    @Test
    public void testAnnotateRegion() throws Exception {

        annotate(new Query(VariantDBAdaptor.VariantQueryParams.CHROMOSOME.key(), "22"), new QueryOptions());

        checkAnnotation(v -> v.getChromosome().equals("22"));

        annotate(new Query(VariantDBAdaptor.VariantQueryParams.CHROMOSOME.key(), "1"), new QueryOptions());

        checkAnnotation(v -> v.getChromosome().equals("22") || v.getChromosome().equals("1"));
    }

    @Test
    public void testAnnotateCreateAndLoad() throws Exception {
        DummyVariantDBAdaptor dbAdaptor = mockVariantDBAdaptor();

        List<File> files = annotate(new Query(), new QueryOptions(VariantAnnotationManager.CREATE, true));
        verify(dbAdaptor, atLeastOnce()).iterator(any(Query.class), any());
        verify(dbAdaptor, never()).updateAnnotations(any(), any());
        verify(dbAdaptor, never()).updateCustomAnnotations(any(), any(), any(), any());

        Assert.assertEquals(1, files.size());

        checkAnnotation(v -> false);

        QueryOptions config = new QueryOptions(VariantAnnotationManager.LOAD_FILE, files.get(0).getId());

        dbAdaptor = mockVariantDBAdaptor();
        annotate(new Query(), config);
        verify(dbAdaptor, atLeastOnce()).updateAnnotations(any(), any());
        verify(dbAdaptor, never()).updateCustomAnnotations(any(), any(), any(), any());
        verify(dbAdaptor, never()).iterator(any(Query.class), any());

        checkAnnotation(v -> true);
    }

    List<File> annotate(Query query, QueryOptions config) throws CatalogException, StorageManagerException, IOException, URISyntaxException {
        return variantManager.annotate(studyStr, query,
                opencga.createTmpOutdir(studyId, "_ANNOT_", sessionId), outputStr, config, sessionId);
    }

    @Test
    public void testCustomAnnotation() throws Exception {

        annotate(new Query(), new QueryOptions());

        checkAnnotation(v -> true);

        DummyVariantDBAdaptor dbAdaptor = mockVariantDBAdaptor();

        File file = opencga.createFile(studyId, "custom_annotation/myannot.gff", sessionId);
        QueryOptions options = new QueryOptions()
                .append(VariantAnnotationManager.LOAD_FILE, file.getId())
                .append(VariantAnnotationManager.CUSTOM_ANNOTATION_KEY, "myAnnot");
        variantManager.annotate(String.valueOf(studyId), new Query(), opencga.createTmpOutdir(studyId, "annot", sessionId),
                String.valueOf(outputId), options, sessionId);

        verify(dbAdaptor, atLeastOnce()).updateCustomAnnotations(any(), matches("myAnnot"), any(), any());

        file = opencga.createFile(studyId, "custom_annotation/myannot.bed", sessionId);
        options = new QueryOptions()
                .append(VariantAnnotationManager.LOAD_FILE, file.getId())
                .append(VariantAnnotationManager.CUSTOM_ANNOTATION_KEY, "myAnnot2");
        variantManager.annotate(String.valueOf(studyId), new Query(), opencga.createTmpOutdir(studyId, "annot", sessionId),
                String.valueOf(outputId), options, sessionId);

        verify(dbAdaptor, atLeastOnce()).updateCustomAnnotations(any(), matches("myAnnot2"), any(), any());
    }

    public Set<String> checkAnnotation(Function<Variant, Boolean> contains) throws Exception {
        return Collections.emptySet();
    }

//    public Set<String> checkAnnotation(Function<Variant, Boolean> contains) throws Exception {
//        try (VariantDBIterator iterator = variantManager.iterator(new Query(VariantDBAdaptor.VariantQueryParams.STUDIES.key(), studyId),
//                new QueryOptions(QueryOptions.SORT, true), sessionId)) {
//
//            Set<String> customAnnotationKeySet = new LinkedHashSet<>();
//            int c = 0;
//            while (iterator.hasNext()) {
//                c++;
//                Variant next = iterator.next();
//                if (contains.apply(next)) {
//                    Assert.assertNotNull(next.getAnnotation());
//                    if (next.getAnnotation().getAdditionalAttributes() != null) {
//                        customAnnotationKeySet.addAll(next.getAnnotation().getAdditionalAttributes().keySet());
//                    }
//                } else {
//                    Assert.assertNull(next.getAnnotation());
//                }
//            }
//            Assert.assertTrue(c > 0);
//            return customAnnotationKeySet;
//        }
//    }

    @Override
    protected VariantSource.Aggregation getAggregation() {
        return VariantSource.Aggregation.NONE;
    }
}
