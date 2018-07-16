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

package org.opencb.opencga.storage.core.variant.annotation;

import org.junit.Before;
import org.junit.Test;
import org.opencb.biodata.models.variant.Variant;
import org.opencb.biodata.models.variant.avro.AdditionalAttribute;
import org.opencb.biodata.models.variant.avro.VariantAnnotation;
import org.opencb.commons.datastore.core.ObjectMap;
import org.opencb.commons.datastore.core.Query;
import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.opencga.storage.core.metadata.ProjectMetadata;
import org.opencb.opencga.storage.core.variant.VariantStorageEngine;
import org.opencb.opencga.storage.core.variant.VariantStorageBaseTest;
import org.opencb.opencga.storage.core.variant.adaptors.VariantDBAdaptor;
import org.opencb.opencga.storage.core.variant.annotation.annotators.VariantAnnotator;
import org.opencb.opencga.storage.core.variant.annotation.annotators.VariantAnnotatorFactory;

import java.util.Collections;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.opencb.opencga.storage.core.variant.adaptors.VariantQueryParam.CUSTOM_ANNOTATION;
import static org.opencb.opencga.storage.core.variant.annotation.VariantAnnotationManager.CUSTOM_ANNOTATION_KEY;

/**
 * Created on 16/05/16
 *
 * @author Jacobo Coll &lt;jacobo167@gmail.com&gt;
 */
public abstract class CustomVariantAnnotationManagerTest extends VariantStorageBaseTest {


    private ProjectMetadata projectMetadata;

    @Before
    public void setUp() throws Exception {
        clearDB(DB_NAME);
        ObjectMap params = new ObjectMap(VariantStorageEngine.Options.CALCULATE_STATS.key(), false)
                .append(VariantStorageEngine.Options.ANNOTATE.key(), false);
        runDefaultETL(inputUri, variantStorageEngine, newStudyConfiguration(), params);
        projectMetadata = new ProjectMetadata("hsapiens", "grch37", 1);
    }


    @Test
    public void testBedAnnotation() throws Exception {
        VariantDBAdaptor dbAdaptor = variantStorageEngine.getDBAdaptor();

        VariantAnnotator annotator = VariantAnnotatorFactory.buildVariantAnnotator(variantStorageEngine.getConfiguration(), variantStorageEngine.getStorageEngineId(), projectMetadata);
        DefaultVariantAnnotationManager annotationManager = new DefaultVariantAnnotationManager(annotator, dbAdaptor);


        String annotKey = "BEDAnnotation";
        annotationManager.loadCustomAnnotation(getResourceUri("custom_annotation/myannot.bed"), new QueryOptions(CUSTOM_ANNOTATION_KEY, annotKey));

        int pos1 = 0, pos2 = 0, pos3 = 0;
        for (Variant variant : dbAdaptor) {
            VariantAnnotation annotation = variant.getAnnotation();
            Map<String, AdditionalAttribute> additionalAttributes;
            if (annotation != null) {
                additionalAttributes = annotation.getAdditionalAttributes();
            } else {
                additionalAttributes = Collections.emptyMap();
            }

            if (variant.getChromosome().equals("22") && variant.getStart() >= 16053659 && variant.getStart() < 16063659) {
                assertTrue(additionalAttributes.containsKey(annotKey));
                AdditionalAttribute annot = additionalAttributes.get(annotKey);
                assertEquals("Pos1", annot.getAttribute().get("name"));
                pos1++;
            } else if (variant.getChromosome().equals("22") && variant.getStart() >= 16073659 && variant.getStart() < 16083659) {
                assertTrue(additionalAttributes.containsKey(annotKey));
                AdditionalAttribute annot = additionalAttributes.get(annotKey);
                assertEquals("Pos2", annot.getAttribute().get("name"));
                pos2++;
            } else if (variant.getChromosome().equals("22") && variant.getStart() >= 16083659 && variant.getStart() < 16093659) {
                assertTrue(additionalAttributes.containsKey(annotKey));
                AdditionalAttribute annot = additionalAttributes.get(annotKey);
                assertEquals("Pos3", annot.getAttribute().get("name"));
                pos3++;
            }
        }

        assertEquals(pos1, dbAdaptor.count(new Query(CUSTOM_ANNOTATION.key(), annotKey + ".name=Pos1")).first().intValue());
        assertEquals(pos2, dbAdaptor.count(new Query(CUSTOM_ANNOTATION.key(), annotKey + ".name=Pos2")).first().intValue());
        assertEquals(pos3, dbAdaptor.count(new Query(CUSTOM_ANNOTATION.key(), annotKey + ".name=Pos3")).first().intValue());

    }

    @Test
    public void testGffAnnotation() throws Exception {
        VariantDBAdaptor dbAdaptor = variantStorageEngine.getDBAdaptor();

        VariantAnnotator annotator = VariantAnnotatorFactory.buildVariantAnnotator(variantStorageEngine.getConfiguration(), variantStorageEngine.getStorageEngineId(), projectMetadata);
        DefaultVariantAnnotationManager annotationManager = new DefaultVariantAnnotationManager(annotator, dbAdaptor);


        String annotKey = "GFFAnnotation";
        annotationManager.loadCustomAnnotation(getResourceUri("custom_annotation/myannot.gff"), new QueryOptions(CUSTOM_ANNOTATION_KEY, annotKey));

        int feat1 = 0, feat2 = 0, feat3 = 0;
        for (Variant variant : dbAdaptor) {
            VariantAnnotation annotation = variant.getAnnotation();
            Map<String, AdditionalAttribute> additionalAttributes;
            if (annotation != null) {
                additionalAttributes = annotation.getAdditionalAttributes();
            } else {
                additionalAttributes = Collections.emptyMap();
            }

            if (variant.getChromosome().equals("22") && variant.getStart() >= 16053659 && variant.getStart() < 16063659) {
                assertTrue(additionalAttributes.containsKey(annotKey));
                AdditionalAttribute annot = additionalAttributes.get(annotKey);
                assertEquals("enhancer", annot.getAttribute().get("feature"));
                feat1++;
            } else if (variant.getChromosome().equals("22") && variant.getStart() >= 16073659 && variant.getStart() < 16083659) {
                assertTrue(additionalAttributes.containsKey(annotKey));
                AdditionalAttribute annot = additionalAttributes.get(annotKey);
                assertEquals("promoter", annot.getAttribute().get("feature"));
                feat2++;
            } else if (variant.getChromosome().equals("22") && variant.getStart() >= 16083659 && variant.getStart() < 16093659) {
                assertTrue(additionalAttributes.containsKey(annotKey));
                AdditionalAttribute annot = additionalAttributes.get(annotKey);
                assertEquals("other", annot.getAttribute().get("feature"));
                feat3++;
            }
        }

        assertEquals(feat1, dbAdaptor.count(new Query(CUSTOM_ANNOTATION.key(), annotKey + ".feature=enhancer")).first().intValue());
        assertEquals(feat2, dbAdaptor.count(new Query(CUSTOM_ANNOTATION.key(), annotKey + ".feature=promoter")).first().intValue());
        assertEquals(feat3, dbAdaptor.count(new Query(CUSTOM_ANNOTATION.key(), annotKey + ".feature=other")).first().intValue());


    }

    @Test
    public void testVcfAnnotation() throws Exception {
        VariantDBAdaptor dbAdaptor = variantStorageEngine.getDBAdaptor();

        VariantAnnotator annotator = VariantAnnotatorFactory.buildVariantAnnotator(variantStorageEngine.getConfiguration(), variantStorageEngine.getStorageEngineId(), projectMetadata);
        DefaultVariantAnnotationManager annotationManager = new DefaultVariantAnnotationManager(annotator, dbAdaptor);

        String annotKey = "VCFAnnotation";
        annotationManager.loadCustomAnnotation(getResourceUri("custom_annotation/myannot.vcf"), new QueryOptions(CUSTOM_ANNOTATION_KEY, annotKey));

        int feat1 = 0, feat2 = 0, feat3 = 0;
        for (Variant variant : dbAdaptor) {
            VariantAnnotation annotation = variant.getAnnotation();
            Map<String, AdditionalAttribute> additionalAttributes;
            if (annotation != null) {
                additionalAttributes = annotation.getAdditionalAttributes();
            } else {
                additionalAttributes = Collections.emptyMap();
            }

            if (variant.getChromosome().equals("22") && variant.getStart() >= 16053659 && variant.getStart() < 16063659) {
                assertTrue(additionalAttributes.containsKey(annotKey));
                AdditionalAttribute annot = additionalAttributes.get(annotKey);
                assertEquals("enhancer", annot.getAttribute().get("FEATURE"));
                feat1++;
            } else if (variant.getChromosome().equals("22") && variant.getStart() >= 16073659 && variant.getStart() < 16083659) {
                assertTrue(additionalAttributes.containsKey(annotKey));
                AdditionalAttribute annot = additionalAttributes.get(annotKey);
                assertEquals("promoter", annot.getAttribute().get("FEATURE"));
                feat2++;
            } else if (variant.getChromosome().equals("22") && variant.getStart() >= 16083659 && variant.getStart() < 16093659) {
                assertTrue(additionalAttributes.containsKey(annotKey));
                AdditionalAttribute annot = additionalAttributes.get(annotKey);
                assertEquals("other", annot.getAttribute().get("FEATURE"));
                feat3++;
            } else if (variant.getChromosome().equals("22") && variant.getStart() == 16050075) {
                assertTrue(additionalAttributes.containsKey(annotKey));
                AdditionalAttribute annot = additionalAttributes.get(annotKey);
                assertEquals("specific", annot.getAttribute().get("FEATURE"));
            }
        }

        assertEquals(feat1, dbAdaptor.count(new Query(CUSTOM_ANNOTATION.key(), annotKey + ".FEATURE=enhancer")).first().intValue());
        assertEquals(feat2, dbAdaptor.count(new Query(CUSTOM_ANNOTATION.key(), annotKey + ".FEATURE=promoter")).first().intValue());
        assertEquals(feat3, dbAdaptor.count(new Query(CUSTOM_ANNOTATION.key(), annotKey + ".FEATURE=other")).first().intValue());
        assertEquals(1, dbAdaptor.count(new Query(CUSTOM_ANNOTATION.key(), annotKey + ".FEATURE=specific")).first().intValue());


    }
}
