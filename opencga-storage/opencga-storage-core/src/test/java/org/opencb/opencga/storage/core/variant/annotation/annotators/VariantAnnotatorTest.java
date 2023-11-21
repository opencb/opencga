package org.opencb.opencga.storage.core.variant.annotation.annotators;

import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang.StringUtils;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.ExpectedException;
import org.opencb.biodata.models.variant.Variant;
import org.opencb.biodata.models.variant.avro.EvidenceEntry;
import org.opencb.biodata.models.variant.avro.VariantAnnotation;
import org.opencb.cellbase.core.result.CellBaseDataResult;
import org.opencb.commons.datastore.core.ObjectMap;
import org.opencb.opencga.core.testclassification.duration.ShortTests;
import org.opencb.opencga.storage.core.StorageEngine;
import org.opencb.opencga.core.config.storage.StorageConfiguration;
import org.opencb.opencga.storage.core.metadata.models.ProjectMetadata;
import org.opencb.opencga.storage.core.variant.VariantStorageOptions;
import org.opencb.opencga.storage.core.variant.annotation.VariantAnnotatorException;

import java.util.*;

import static org.hamcrest.CoreMatchers.instanceOf;
import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.*;
import static org.junit.Assume.assumeTrue;

/**
 * Created by jacobo on 27/11/17.
 */
@Category(ShortTests.class)
public class VariantAnnotatorTest {

    private StorageConfiguration storageConfiguration;

    @Rule
    public ExpectedException thrown = ExpectedException.none();
    private ProjectMetadata projectMetadata;

    @Before
    public void setUp() throws Exception {
        storageConfiguration = StorageConfiguration.load(StorageEngine.class.getClassLoader().getResourceAsStream("storage-configuration.yml"), "yml");
        projectMetadata = new ProjectMetadata("hsapiens", "grch38", 1);
    }

    @Test
    public void testVariantFactory() throws Exception {
        ObjectMap options;
        VariantAnnotator variantAnnotator;

        options = new ObjectMap(VariantStorageOptions.ANNOTATOR.key(), "cellbase");
        variantAnnotator = VariantAnnotatorFactory.buildVariantAnnotator(storageConfiguration, projectMetadata, options);
        assertThat(variantAnnotator, is(instanceOf(CellBaseRestVariantAnnotator.class)));

        options = new ObjectMap(VariantStorageOptions.ANNOTATOR.key(), "cellbase_rest");
        variantAnnotator = VariantAnnotatorFactory.buildVariantAnnotator(storageConfiguration, projectMetadata, options);
        assertThat(variantAnnotator, is(instanceOf(CellBaseRestVariantAnnotator.class)));

//        storageConfiguration.getVariantEngine().getOptions().put(VariantAnnotationManager.ANNOTATOR, VariantAnnotatorFactory.AnnotationSource.CELLBASE_DB_ADAPTOR.toString());
//        options = new ObjectMap();
//        variantAnnotator = VariantAnnotatorFactory.buildVariantAnnotator(storageConfiguration, projectMetadata, options);
//        assertThat(variantAnnotator, is(instanceOf(CellBaseDirectVariantAnnotator.class)));

//        options = new ObjectMap(VariantAnnotationManager.ANNOTATOR, null);
//        variantAnnotator = VariantAnnotatorFactory.buildVariantAnnotator(storageConfiguration, projectMetadata, options);
//        assertThat(variantAnnotator, is(instanceOf(CellBaseDirectVariantAnnotator.class)));

//        storageConfiguration.getVariantEngine().getOptions().put(VariantAnnotationManager.ANNOTATOR, VariantAnnotatorFactory.AnnotationSource.CELLBASE_REST);
//        options = new ObjectMap(VariantAnnotationManager.ANNOTATOR, null);
//        variantAnnotator = VariantAnnotatorFactory.buildVariantAnnotator(storageConfiguration, projectMetadata, options);
//        assertThat(variantAnnotator, is(instanceOf(CellBaseRestVariantAnnotator.class)));

        options = new ObjectMap();
        variantAnnotator = VariantAnnotatorFactory.buildVariantAnnotator(storageConfiguration, projectMetadata, options);
        assertThat(variantAnnotator, is(instanceOf(CellBaseRestVariantAnnotator.class)));

        options = new ObjectMap(VariantStorageOptions.ANNOTATOR.key(), VariantAnnotatorFactory.AnnotationEngine.OTHER.toString())
                .append(VariantStorageOptions.ANNOTATOR_CLASS.key(), TestCellBaseRestVariantAnnotator.class.getName());
        variantAnnotator = VariantAnnotatorFactory.buildVariantAnnotator(storageConfiguration, projectMetadata, options);
        assertThat(variantAnnotator, is(instanceOf(TestCellBaseRestVariantAnnotator.class)));
    }


    @Test
    public void testSkipVariant() throws VariantAnnotatorException {
        ObjectMap options = new ObjectMap(VariantStorageOptions.ANNOTATOR.key(), VariantAnnotatorFactory.AnnotationEngine.OTHER.toString())
                .append(VariantStorageOptions.ANNOTATOR_CLASS.key(), TestCellBaseRestVariantAnnotator.class.getName());
        VariantAnnotator variantAnnotator = VariantAnnotatorFactory.buildVariantAnnotator(storageConfiguration, projectMetadata, options);
        assertThat(variantAnnotator, is(instanceOf(TestCellBaseRestVariantAnnotator.class)));

        TestCellBaseRestVariantAnnotator testAnnotator = (TestCellBaseRestVariantAnnotator) variantAnnotator;
        testAnnotator.skip("10:1000:A:C");
        List<VariantAnnotation> annotate = testAnnotator.annotate(Arrays.asList(new Variant("10:999:A:C"), new Variant("10:1000:A:C"), new Variant("10:1001:A:C")));
        assertEquals(2, annotate.size());

    }

    @Test
    public void testErrorVariant() throws VariantAnnotatorException {
        ObjectMap options = new ObjectMap(VariantStorageOptions.ANNOTATOR.key(), VariantAnnotatorFactory.AnnotationEngine.OTHER.toString())
                .append(VariantStorageOptions.ANNOTATOR_CLASS.key(), TestCellBaseRestVariantAnnotator.class.getName());
        VariantAnnotator variantAnnotator = VariantAnnotatorFactory.buildVariantAnnotator(storageConfiguration, projectMetadata, options);
        assertThat(variantAnnotator, is(instanceOf(TestCellBaseRestVariantAnnotator.class)));

        TestCellBaseRestVariantAnnotator testAnnotator = (TestCellBaseRestVariantAnnotator) variantAnnotator;
        testAnnotator.remove("10:1000:A:C");
        Exception exception = AbstractCellBaseVariantAnnotator.unexpectedVariantOrderException("10:1000:A:C", "10:1001:A:C");
        thrown.expect(exception.getClass());
        thrown.expectMessage(exception.getMessage());
        testAnnotator.annotate(Arrays.asList(new Variant("10:999:A:C"), new Variant("10:1000:A:C"), new Variant("10:1001:A:C")));
    }

    @Test
    public void useCellBaseApiKeys() throws VariantAnnotatorException {
        storageConfiguration.getCellbase().setUrl("https://uk.ws.zettagenomics.com/cellbase/");
        storageConfiguration.getCellbase().setVersion("v5.4");
        storageConfiguration.getCellbase().setDataRelease("3");

        VariantAnnotator variantAnnotator = null;
        ObjectMap options = new ObjectMap(VariantStorageOptions.ANNOTATOR.key(), "cellbase");

        try {
            variantAnnotator = VariantAnnotatorFactory.buildVariantAnnotator(storageConfiguration, projectMetadata, options);
        } catch (Exception e) {
            // Nothing to do
        }
        assumeTrue(variantAnnotator != null);

        String apiKey;

        // No token
        List<VariantAnnotation> results = variantAnnotator.annotate(Collections.singletonList(new Variant("10:113588287:G:A")));
        assertEquals(1, results.size());
        assertTrue(results.get(0).getTraitAssociation().stream().anyMatch(e -> e.getSource().getName().equals("clinvar")));
        assertFalse(results.get(0).getTraitAssociation().stream().anyMatch(e -> e.getSource().getName().equals("cosmic")));
        assertFalse(results.get(0).getTraitAssociation().stream().anyMatch(e -> e.getSource().getName().equals("hgmd")));

        // Using COSMIC token
        apiKey = System.getenv("CELLBASE_COSMIC_APIKEY");
        if (StringUtils.isNotEmpty(apiKey)) {
            storageConfiguration.getCellbase().setApiKey(apiKey);
            variantAnnotator = VariantAnnotatorFactory.buildVariantAnnotator(storageConfiguration, projectMetadata, options);
            results = variantAnnotator.annotate(Collections.singletonList(new Variant("10:113588287:G:A")));
            assertEquals(1, results.size());
            assertTrue(results.get(0).getTraitAssociation().stream().anyMatch(e -> e.getSource().getName().equals("clinvar")));
            assertTrue(results.get(0).getTraitAssociation().stream().anyMatch(e -> e.getSource().getName().equals("cosmic")));
            assertFalse(results.get(0).getTraitAssociation().stream().anyMatch(e -> e.getSource().getName().equals("hgmd")));
        }

        // Using HGMD token
        apiKey = System.getenv("CELLBASE_HGMD_APIKEY");
        if (StringUtils.isNotEmpty(apiKey)) {
            storageConfiguration.getCellbase().setApiKey(apiKey);
            variantAnnotator = VariantAnnotatorFactory.buildVariantAnnotator(storageConfiguration, projectMetadata, options);
            results = variantAnnotator.annotate(Collections.singletonList(new Variant("10:113588287:G:A")));
            assertEquals(1, results.size());
            assertTrue(results.get(0).getTraitAssociation().stream().anyMatch(e -> e.getSource().getName().equals("clinvar")));
            assertFalse(results.get(0).getTraitAssociation().stream().anyMatch(e -> e.getSource().getName().equals("cosmic")));
            assertTrue(results.get(0).getTraitAssociation().stream().anyMatch(e -> e.getSource().getName().equals("hgmd")));
        }

        // Using COSMIC + HGMD token
        apiKey = System.getenv("CELLBASE_COSMIC_HGMD_APIKEY");
        if (StringUtils.isNotEmpty(apiKey)) {
            storageConfiguration.getCellbase().setApiKey(apiKey);
            variantAnnotator = VariantAnnotatorFactory.buildVariantAnnotator(storageConfiguration, projectMetadata, options);
            results = variantAnnotator.annotate(Collections.singletonList(new Variant("10:113588287:G:A")));
            assertEquals(1, results.size());
            assertTrue(results.get(0).getTraitAssociation().stream().anyMatch(e -> e.getSource().getName().equals("clinvar")));
            assertTrue(results.get(0).getTraitAssociation().stream().anyMatch(e -> e.getSource().getName().equals("cosmic")));
            assertTrue(results.get(0).getTraitAssociation().stream().anyMatch(e -> e.getSource().getName().equals("hgmd")));
        }
    }

    public static class TestCellBaseRestVariantAnnotator extends CellBaseRestVariantAnnotator {

        private final Set<String> skipvariants;
        private final Set<String> removevariants;

        public TestCellBaseRestVariantAnnotator(StorageConfiguration storageConfiguration, ProjectMetadata projectMetadata, ObjectMap options) throws VariantAnnotatorException {
            super(storageConfiguration, projectMetadata, options);
            skipvariants = new HashSet<>();
            removevariants = new HashSet<>();
            System.out.println("Create " + getClass());
        }

        public TestCellBaseRestVariantAnnotator skip(String variant) {
            skipvariants.add(variant);
            return this;
        }

        public TestCellBaseRestVariantAnnotator remove(String variant) {
            removevariants.add(variant);
            return this;
        }

        @Override
        protected List<CellBaseDataResult<VariantAnnotation>> annotateFiltered(List<Variant> variants) throws VariantAnnotatorException {
            List<CellBaseDataResult<VariantAnnotation>> queryResults = super.annotateFiltered(variants);
            for (Iterator<CellBaseDataResult<VariantAnnotation>> iterator = queryResults.iterator(); iterator.hasNext(); ) {
                CellBaseDataResult<VariantAnnotation> queryResult = iterator.next();
                assertEquals(1, queryResult.getResults().size());
                if (skipvariants.contains(queryResult.getId())) {
                    queryResult.setResults(Collections.emptyList());
                    queryResult.setNumResults(0);
                } else if (removevariants.contains(queryResult.getId())) {
                    iterator.remove();
                }
            }
            return queryResults;
        }
    }


}