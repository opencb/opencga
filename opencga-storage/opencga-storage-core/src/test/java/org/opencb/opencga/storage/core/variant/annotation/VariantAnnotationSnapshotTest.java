package org.opencb.opencga.storage.core.variant.annotation;

import org.junit.Test;
import org.opencb.biodata.models.variant.Variant;
import org.opencb.biodata.models.variant.avro.ConsequenceType;
import org.opencb.biodata.models.variant.avro.VariantAnnotation;
import org.opencb.commons.datastore.core.ObjectMap;
import org.opencb.commons.datastore.core.Query;
import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.opencga.storage.core.config.StorageConfiguration;
import org.opencb.opencga.storage.core.exceptions.StorageEngineException;
import org.opencb.opencga.storage.core.metadata.ProjectMetadata;
import org.opencb.opencga.storage.core.variant.VariantStorageBaseTest;
import org.opencb.opencga.storage.core.variant.VariantStorageEngine;
import org.opencb.opencga.storage.core.variant.adaptors.VariantField;
import org.opencb.opencga.storage.core.variant.adaptors.VariantQueryParam;
import org.opencb.opencga.storage.core.variant.annotation.annotators.VariantAnnotator;
import org.opencb.opencga.storage.core.variant.annotation.annotators.VariantAnnotatorFactory;

import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.opencb.opencga.storage.core.variant.annotation.VariantAnnotationManager.ANNOTATOR;
import static org.opencb.opencga.storage.core.variant.annotation.VariantAnnotationManager.VARIANT_ANNOTATOR_CLASSNAME;

/**
 * Created on 24/04/18.
 *
 * @author Jacobo Coll &lt;jacobo167@gmail.com&gt;
 */
public abstract class VariantAnnotationSnapshotTest extends VariantStorageBaseTest {

    @Test
    public void testMultiAnnotations() throws Exception {

        VariantStorageEngine variantStorageEngine = getVariantStorageEngine();
        runDefaultETL(smallInputUri, variantStorageEngine, newStudyConfiguration(),
                new ObjectMap(VariantStorageEngine.Options.ANNOTATE.key(), false));

        variantStorageEngine.getOptions()
                .append(VARIANT_ANNOTATOR_CLASSNAME, TestAnnotator.class.getName())
                .append(ANNOTATOR, VariantAnnotatorFactory.AnnotationSource.OTHER);

        variantStorageEngine.createAnnotationSnapshot("v0", new ObjectMap());
        variantStorageEngine.annotate(new Query(), new ObjectMap(TestAnnotator.ANNOT_KEY, "v1"));
        variantStorageEngine.createAnnotationSnapshot("v1", new ObjectMap());
        variantStorageEngine.annotate(new Query(), new ObjectMap(TestAnnotator.ANNOT_KEY, "v2"));
        variantStorageEngine.createAnnotationSnapshot("v2", new ObjectMap());
        variantStorageEngine.annotate(new Query(), new ObjectMap(TestAnnotator.ANNOT_KEY, "v3"));

        assertEquals(0, variantStorageEngine.getAnnotation("v0", null, null).getResult().size());
        checkAnnotationSnapshot(variantStorageEngine, "v1", "v1");
        checkAnnotationSnapshot(variantStorageEngine, "v2", "v2");
        checkAnnotationSnapshot(variantStorageEngine, "LATEST", "v3");

        variantStorageEngine.deleteAnnotationSnapshot("v1", new ObjectMap());

        testQueries(variantStorageEngine);

    }

    public void testQueries(VariantStorageEngine variantStorageEngine) throws StorageEngineException {
        long count = variantStorageEngine.count(new Query()).first();
        long partialCount = 0;
        int batchSize = (int) Math.ceil(count / 10.0);
        for (int i = 0; i < 10; i++) {
            partialCount += variantStorageEngine.getAnnotation("v2", null, new QueryOptions(QueryOptions.LIMIT, batchSize)
                    .append(QueryOptions.SKIP, batchSize * i)).getResult().size();
        }
        assertEquals(count, partialCount);

        for (int chr = 1; chr < 22; chr += 2) {
            Query query = new Query(VariantQueryParam.REGION.key(), chr + "," + (chr + 1));
            count = variantStorageEngine.count(query).first();
            partialCount = variantStorageEngine.getAnnotation("v2", query, new QueryOptions()).getResult().size();
            assertEquals(count, partialCount);
        }

        String consequenceTypes = VariantField.ANNOTATION_CONSEQUENCE_TYPES.fieldName().replace(VariantField.ANNOTATION.fieldName() + ".", "");
        for (VariantAnnotation annotation : variantStorageEngine.getAnnotation("v2", null, new QueryOptions(QueryOptions.INCLUDE, consequenceTypes)).getResult()) {
            assertEquals(1, annotation.getConsequenceTypes().size());
        }
        for (VariantAnnotation annotation : variantStorageEngine.getAnnotation("v2", null, new QueryOptions(QueryOptions.EXCLUDE, consequenceTypes)).getResult()) {
            assertTrue(annotation.getConsequenceTypes() == null || annotation.getConsequenceTypes().isEmpty());
        }


        // Get annotations from a deleted snapshot
        // FIXME: Should throw an exception?
        assertEquals(0, variantStorageEngine.getAnnotation("v1", null, null).getResult().size());
    }

    public void checkAnnotationSnapshot(VariantStorageEngine variantStorageEngine, String name, String expectedId) throws Exception {
        int count = 0;
        for (VariantAnnotation annotation: variantStorageEngine.getAnnotation(name, null, null).getResult()) {
            assertEquals(expectedId, annotation.getId());
            count++;
        }
        assertEquals(count, variantStorageEngine.count(new Query()).first().intValue());
    }

    public static class TestAnnotator extends VariantAnnotator {

        public static final String ANNOT_KEY = "ANNOT_KEY";
        private String key;

        public TestAnnotator(StorageConfiguration configuration, ProjectMetadata projectMetadata, ObjectMap options) throws VariantAnnotatorException {
            super(configuration, projectMetadata, options);
            key = options.getString(ANNOT_KEY);
        }

        @Override
        public List<VariantAnnotation> annotate(List<Variant> variants) throws VariantAnnotatorException {
            return variants.stream().map(v -> {
                VariantAnnotation a = new VariantAnnotation();
                a.setChromosome(v.getChromosome());
                a.setStart(v.getStart());
                a.setEnd(v.getEnd());
                a.setReference(v.getReference());
                a.setAlternate(v.getAlternate());
                a.setId(key);
                ConsequenceType ct = new ConsequenceType();
                ct.setGeneName("a gene");
                ct.setSequenceOntologyTerms(Collections.emptyList());
                ct.setExonOverlap(Collections.emptyList());
                ct.setTranscriptAnnotationFlags(Collections.emptyList());
                a.setConsequenceTypes(Collections.singletonList(ct));
                return a;
            }).collect(Collectors.toList());
        }
    }

}
