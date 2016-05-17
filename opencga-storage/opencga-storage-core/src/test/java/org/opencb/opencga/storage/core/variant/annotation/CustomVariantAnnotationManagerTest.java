package org.opencb.opencga.storage.core.variant.annotation;

import org.junit.Before;
import org.junit.Test;
import org.opencb.biodata.models.variant.Variant;
import org.opencb.commons.datastore.core.ObjectMap;
import org.opencb.commons.datastore.core.Query;
import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.opencga.storage.core.variant.VariantStorageManager;
import org.opencb.opencga.storage.core.variant.VariantStorageManagerTestUtils;
import org.opencb.opencga.storage.core.variant.adaptors.VariantDBAdaptor;
import org.opencb.opencga.storage.core.variant.adaptors.VariantDBAdaptor.VariantQueryParams;
import org.opencb.opencga.storage.core.variant.adaptors.VariantDBIterator;

import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.opencb.opencga.storage.core.variant.adaptors.VariantDBAdaptor.VariantQueryParams.*;
import static org.opencb.opencga.storage.core.variant.annotation.VariantAnnotationManager.CUSTOM_ANNOTATION_KEY;

/**
 * Created on 16/05/16
 *
 * @author Jacobo Coll &lt;jacobo167@gmail.com&gt;
 */
public abstract class CustomVariantAnnotationManagerTest extends VariantStorageManagerTestUtils {


    @Before
    public void setUp() throws Exception {
        clearDB(DB_NAME);
        ObjectMap params = new ObjectMap(VariantStorageManager.Options.CALCULATE_STATS.key(), false);
        runDefaultETL(inputUri, variantStorageManager, newStudyConfiguration(), params);
    }


    @Test
    public void testBedAnnotation() throws Exception {
        VariantDBAdaptor dbAdaptor = variantStorageManager.getDBAdaptor(DB_NAME);

        VariantAnnotator annotator = VariantAnnotationManager.buildVariantAnnotator(variantStorageManager.getConfiguration(), variantStorageManager.getStorageEngineId());
        VariantAnnotationManager annotationManager = new VariantAnnotationManager(annotator, dbAdaptor);


        String annotKey = "BEDAnnotation";
        annotationManager.loadCustomAnnotation(getResourceUri("custom_annotation/myannot.bed"), new QueryOptions(CUSTOM_ANNOTATION_KEY, annotKey));

        int pos1 = 0, pos2 = 0, pos3 = 0;
        for (Variant variant : dbAdaptor) {
            Map<String, Object> additionalAttributes = variant.getAnnotation().getAdditionalAttributes();


            if (variant.getChromosome().equals("22") && variant.getStart() >= 16053659 && variant.getStart() < 16063659) {
                assertTrue(additionalAttributes.containsKey(annotKey));
                Map<String, Object> annot = (Map<String, Object>) additionalAttributes.get(annotKey);
                assertEquals("Pos1", annot.get("name"));
                pos1++;
            } else if (variant.getChromosome().equals("22") && variant.getStart() >= 16073659 && variant.getStart() < 16083659) {
                assertTrue(additionalAttributes.containsKey(annotKey));
                Map<String, Object> annot = (Map<String, Object>) additionalAttributes.get(annotKey);
                assertEquals("Pos2", annot.get("name"));
                pos2++;
            } else if (variant.getChromosome().equals("22") && variant.getStart() >= 16083659 && variant.getStart() < 16093659) {
                assertTrue(additionalAttributes.containsKey(annotKey));
                Map<String, Object> annot = (Map<String, Object>) additionalAttributes.get(annotKey);
                assertEquals("Pos3", annot.get("name"));
                pos3++;
            }
        }

        assertEquals(pos1, dbAdaptor.count(new Query(ANNOT_CUSTOM.key(), annotKey + ".name=Pos1")).first().intValue());
        assertEquals(pos2, dbAdaptor.count(new Query(ANNOT_CUSTOM.key(), annotKey + ".name=Pos2")).first().intValue());
        assertEquals(pos3, dbAdaptor.count(new Query(ANNOT_CUSTOM.key(), annotKey + ".name=Pos3")).first().intValue());

    }

    @Test
    public void testGffAnnotation() throws Exception {
        VariantDBAdaptor dbAdaptor = variantStorageManager.getDBAdaptor(DB_NAME);

        VariantAnnotator annotator = VariantAnnotationManager.buildVariantAnnotator(variantStorageManager.getConfiguration(), variantStorageManager.getStorageEngineId());
        VariantAnnotationManager annotationManager = new VariantAnnotationManager(annotator, dbAdaptor);


        String annotKey = "GFFAnnotation";
        annotationManager.loadCustomAnnotation(getResourceUri("custom_annotation/myannot.gff"), new QueryOptions(CUSTOM_ANNOTATION_KEY, annotKey));

        int feat1 = 0, feat2 = 0, feat3 = 0;
        for (Variant variant : dbAdaptor) {
            Map<String, Object> additionalAttributes = variant.getAnnotation().getAdditionalAttributes();

            if (variant.getChromosome().equals("22") && variant.getStart() >= 16053659 && variant.getStart() < 16063659) {
                assertTrue(additionalAttributes.containsKey(annotKey));
                Map<String, Object> annot = (Map<String, Object>) additionalAttributes.get(annotKey);
                assertEquals("enhancer", annot.get("feature"));
                feat1++;
            } else if (variant.getChromosome().equals("22") && variant.getStart() >= 16073659 && variant.getStart() < 16083659) {
                assertTrue(additionalAttributes.containsKey(annotKey));
                Map<String, Object> annot = (Map<String, Object>) additionalAttributes.get(annotKey);
                assertEquals("promoter", annot.get("feature"));
                feat2++;
            } else if (variant.getChromosome().equals("22") && variant.getStart() >= 16083659 && variant.getStart() < 16093659) {
                assertTrue(additionalAttributes.containsKey(annotKey));
                Map<String, Object> annot = (Map<String, Object>) additionalAttributes.get(annotKey);
                assertEquals("other", annot.get("feature"));
                feat3++;
            }
        }

        assertEquals(feat1, dbAdaptor.count(new Query(ANNOT_CUSTOM.key(), annotKey + ".feature=enhancer")).first().intValue());
        assertEquals(feat2, dbAdaptor.count(new Query(ANNOT_CUSTOM.key(), annotKey + ".feature=promoter")).first().intValue());
        assertEquals(feat3, dbAdaptor.count(new Query(ANNOT_CUSTOM.key(), annotKey + ".feature=other")).first().intValue());


    }
}
