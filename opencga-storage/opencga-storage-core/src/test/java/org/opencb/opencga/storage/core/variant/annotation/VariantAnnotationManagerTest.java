package org.opencb.opencga.storage.core.variant.annotation;

import org.junit.Test;
import org.opencb.biodata.models.variant.avro.VariantAnnotation;
import org.opencb.commons.datastore.core.ObjectMap;
import org.opencb.commons.datastore.core.Query;
import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.opencga.storage.core.exceptions.StorageEngineException;
import org.opencb.opencga.storage.core.variant.VariantStorageBaseTest;
import org.opencb.opencga.storage.core.variant.VariantStorageEngine;
import org.opencb.opencga.storage.core.variant.VariantStorageOptions;
import org.opencb.opencga.storage.core.variant.adaptors.VariantField;
import org.opencb.opencga.storage.core.variant.adaptors.VariantMatchers;
import org.opencb.opencga.storage.core.variant.adaptors.VariantQueryParam;
import org.opencb.opencga.storage.core.variant.annotation.annotators.VariantAnnotatorFactory;

import static org.junit.Assert.*;
import static org.opencb.opencga.storage.core.variant.adaptors.VariantField.AdditionalAttributes.GROUP_NAME;

/**
 * Created on 24/04/18.
 *
 * @author Jacobo Coll &lt;jacobo167@gmail.com&gt;
 */
public abstract class VariantAnnotationManagerTest extends VariantStorageBaseTest {

    @Test
    public void testChangeAnnotator() throws Exception {
        VariantStorageEngine variantStorageEngine = getVariantStorageEngine();
        runDefaultETL(smallInputUri, variantStorageEngine, newStudyMetadata(),
                new ObjectMap(VariantStorageOptions.ANNOTATE.key(), false));

        variantStorageEngine.getOptions()
                .append(VariantStorageOptions.ANNOTATOR_CLASS.key(), DummyTestAnnotator.class.getName())
                .append(VariantStorageOptions.ANNOTATOR.key(), VariantAnnotatorFactory.AnnotationEngine.OTHER);

        // First annotation. Should run ok.
        variantStorageEngine.annotate(new Query(), new ObjectMap(DummyTestAnnotator.ANNOT_KEY, "v1"));
        assertEquals("v1", variantStorageEngine.getMetadataManager().getProjectMetadata().getAnnotation().getCurrent().getAnnotator().getVersion());

        // Second annotation. New annotator. Overwrite. Should run ok.
        variantStorageEngine.annotate(new Query(), new ObjectMap(DummyTestAnnotator.ANNOT_KEY, "v2").append(VariantStorageOptions.ANNOTATION_OVERWEITE.key(), true));
        assertEquals("v2", variantStorageEngine.getMetadataManager().getProjectMetadata().getAnnotation().getCurrent().getAnnotator().getVersion());

        // Third annotation. New annotator. Do not overwrite. Should fail.
        thrown.expect(VariantAnnotatorException.class);
        variantStorageEngine.annotate(new Query(), new ObjectMap(DummyTestAnnotator.ANNOT_KEY, "v3").append(VariantStorageOptions.ANNOTATION_OVERWEITE.key(), false));
    }

    @Test
    public void testChangeAnnotatorFail() throws Exception {
        VariantStorageEngine variantStorageEngine = getVariantStorageEngine();
        runDefaultETL(smallInputUri, variantStorageEngine, newStudyMetadata(),
                new ObjectMap(VariantStorageOptions.ANNOTATE.key(), false));

        variantStorageEngine.getOptions()
                .append(VariantStorageOptions.ANNOTATOR_CLASS.key(), DummyTestAnnotator.class.getName())
                .append(VariantStorageOptions.ANNOTATOR.key(), VariantAnnotatorFactory.AnnotationEngine.OTHER);

        // First annotation. Should run ok.
        variantStorageEngine.annotate(new Query(), new ObjectMap(DummyTestAnnotator.ANNOT_KEY, "v1"));

        try {
            // Second annotation. New annotator. Overwrite. Fail annotation
            variantStorageEngine.annotate(new Query(), new ObjectMap(DummyTestAnnotator.ANNOT_KEY, "v2")
                    .append(DummyTestAnnotator.FAIL, true)
                    .append(VariantStorageOptions.ANNOTATION_OVERWEITE.key(), true));
            fail("Expected to fail!");
        } catch (VariantAnnotatorException e) {
            e.printStackTrace();
            // Annotator information does not change
            assertEquals("v1", variantStorageEngine.getMetadataManager().getProjectMetadata().getAnnotation().getCurrent().getAnnotator().getVersion());
        }


        // Second annotation bis. New annotator. Overwrite.
        variantStorageEngine.annotate(new Query(), new ObjectMap(DummyTestAnnotator.ANNOT_KEY, "v2")
                .append(DummyTestAnnotator.FAIL, false)
                .append(VariantStorageOptions.ANNOTATION_OVERWEITE.key(), true));
        assertEquals("v2", variantStorageEngine.getMetadataManager().getProjectMetadata().getAnnotation().getCurrent().getAnnotator().getVersion());
    }

    @Test
    public void testMultiAnnotations() throws Exception {

        VariantStorageEngine variantStorageEngine = getVariantStorageEngine();
        runDefaultETL(smallInputUri, variantStorageEngine, newStudyMetadata(),
                new ObjectMap(VariantStorageOptions.ANNOTATE.key(), false));

        variantStorageEngine.getOptions()
                .append(VariantStorageOptions.ANNOTATOR_CLASS.key(), DummyTestAnnotator.class.getName())
                .append(VariantStorageOptions.ANNOTATOR.key(), VariantAnnotatorFactory.AnnotationEngine.OTHER);

        variantStorageEngine.saveAnnotation("v0", new ObjectMap());
        variantStorageEngine.annotate(new Query(), new ObjectMap(DummyTestAnnotator.ANNOT_KEY, "v1").append(VariantStorageOptions.ANNOTATION_OVERWEITE.key(), true));
        variantStorageEngine.saveAnnotation("v1", new ObjectMap());
        variantStorageEngine.annotate(new Query(), new ObjectMap(DummyTestAnnotator.ANNOT_KEY, "v2").append(VariantStorageOptions.ANNOTATION_OVERWEITE.key(), true));
        variantStorageEngine.saveAnnotation("v2", new ObjectMap());
        variantStorageEngine.annotate(new Query(VariantQueryParam.REGION.key(), "1"), new ObjectMap(DummyTestAnnotator.ANNOT_KEY, "v3").append(VariantStorageOptions.ANNOTATION_OVERWEITE.key(), true));

        assertEquals(0, variantStorageEngine.getAnnotation("v0", null, null).getResults().size());
        checkAnnotationSnapshot(variantStorageEngine, "v1", "v1");
        checkAnnotationSnapshot(variantStorageEngine, "v2", "v2");
        checkAnnotationSnapshot(variantStorageEngine, VariantAnnotationManager.CURRENT, VariantAnnotationManager.CURRENT, "v3", new Query(VariantQueryParam.REGION.key(), "1"));
        checkAnnotationSnapshot(variantStorageEngine, VariantAnnotationManager.CURRENT, "v2", "v2", new Query(VariantQueryParam.REGION.key(), "2"));

        variantStorageEngine.deleteAnnotation("v1", new ObjectMap());

        testQueries(variantStorageEngine);

    }

    public void testQueries(VariantStorageEngine variantStorageEngine) throws StorageEngineException {
        long count = variantStorageEngine.count(new Query()).first();
        long partialCount = 0;
        int batchSize = (int) Math.ceil(count / 10.0);
        for (int i = 0; i < 10; i++) {
            partialCount += variantStorageEngine.getAnnotation("v2", null, new QueryOptions(QueryOptions.LIMIT, batchSize)
                    .append(QueryOptions.SKIP, batchSize * i)).getResults().size();
        }
        assertEquals(count, partialCount);

        for (int chr = 1; chr < 22; chr += 2) {
            Query query = new Query(VariantQueryParam.REGION.key(), chr + "," + (chr + 1));
            count = variantStorageEngine.count(query).first();
            partialCount = variantStorageEngine.getAnnotation("v2", query, new QueryOptions()).getResults().size();
            assertEquals(count, partialCount);
        }

        String consequenceTypes = VariantField.ANNOTATION_CONSEQUENCE_TYPES.fieldName().replace(VariantField.ANNOTATION.fieldName() + ".", "");
        for (VariantAnnotation annotation : variantStorageEngine.getAnnotation("v2", null, new QueryOptions(QueryOptions.INCLUDE, consequenceTypes)).getResults()) {
            assertEquals(1, annotation.getConsequenceTypes().size());
        }
        for (VariantAnnotation annotation : variantStorageEngine.getAnnotation("v2", null, new QueryOptions(QueryOptions.EXCLUDE, consequenceTypes)).getResults()) {
            assertThat(annotation.getConsequenceTypes(), VariantMatchers.isEmpty());
        }


        // Get annotations from a deleted snapshot
        thrown.expectMessage("Variant Annotation snapshot \"v1\" not found!");
        assertEquals(0, variantStorageEngine.getAnnotation("v1", null, null).getResults().size());
    }

    public void checkAnnotationSnapshot(VariantStorageEngine variantStorageEngine, String annotationName, String expectedId) throws Exception {
        checkAnnotationSnapshot(variantStorageEngine, annotationName, annotationName, expectedId, null);
    }

    public void checkAnnotationSnapshot(VariantStorageEngine variantStorageEngine, String annotationName, String expectedAnnotationName, String expectedId, Query query) throws Exception {
        int count = 0;
        for (VariantAnnotation annotation: variantStorageEngine.getAnnotation(annotationName, query, null).getResults()) {
            assertEquals("an id -- " + expectedId, annotation.getId());
//            assertEquals("1", annotation.getAdditionalAttributes().get("opencga").getAttribute().get("release"));
            assertEquals(expectedAnnotationName, annotation.getAdditionalAttributes().get(GROUP_NAME.key())
                    .getAttribute().get(VariantField.AdditionalAttributes.ANNOTATION_ID.key()));
            count++;
        }
        assertEquals(count, variantStorageEngine.count(query).first().intValue());
    }

}
