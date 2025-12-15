package org.opencb.opencga.storage.core.variant.annotation.annotators;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.commons.lang3.StringUtils;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.ExpectedException;
import org.opencb.biodata.models.variant.Variant;
import org.opencb.biodata.models.variant.avro.VariantAnnotation;
import org.opencb.cellbase.core.result.CellBaseDataResult;
import org.opencb.commons.datastore.core.ObjectMap;
import org.opencb.commons.utils.FileUtils;
import org.opencb.opencga.core.common.JacksonUtils;
import org.opencb.opencga.core.config.storage.StorageConfiguration;
import org.opencb.opencga.core.testclassification.duration.ShortTests;
import org.opencb.opencga.storage.core.StorageEngine;
import org.opencb.opencga.storage.core.metadata.models.ProjectMetadata;
import org.opencb.opencga.storage.core.variant.VariantStorageOptions;
import org.opencb.opencga.storage.core.variant.annotation.VariantAnnotatorException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

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
    public void testErrorVariant() throws Exception {
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
    public void useCellBaseApiKeys() throws Exception {
        storageConfiguration.getCellbase().setUrl("https://uk.ws.zettagenomics.com/cellbase/");
        storageConfiguration.getCellbase().setVersion("v5.8");
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

    public static class TestCachedCellBaseRestVariantAnnotator extends CellBaseRestVariantAnnotator {
        public static Map<String, VariantAnnotation> ANNOTATION_CACHE = new ConcurrentHashMap<>();
        protected static Logger logger = LoggerFactory.getLogger(TestCachedCellBaseRestVariantAnnotator.class);

        private final Path cacheDir;
        private final ObjectMapper mapper = JacksonUtils.getDefaultObjectMapper();
        private final Path metadataFile;
        private volatile ProjectMetadata.VariantAnnotationMetadata cachedMetadata;
        private int cacheHits = 0;
        private int cacheMisses = 0;

        public TestCachedCellBaseRestVariantAnnotator(StorageConfiguration storageConfiguration, ProjectMetadata projectMetadata, ObjectMap options)
                throws VariantAnnotatorException {
            super(storageConfiguration, projectMetadata, options);
            this.cacheDir = Paths.get("target", "test-data", "variant-annotation-cache");
            this.metadataFile = cacheDir.resolve("variant-annotation-metadata.json");
        }

        @Override
        public ProjectMetadata.VariantAnnotationMetadata getVariantAnnotationMetadata() throws VariantAnnotatorException {
            if (cachedMetadata != null) {
                return cachedMetadata;
            }
            if (Files.exists(metadataFile)) {
                try (BufferedReader br = FileUtils.newBufferedReader(metadataFile)) {
                    cachedMetadata = mapper.readValue(br, ProjectMetadata.VariantAnnotationMetadata.class);
                    return cachedMetadata;
                } catch (IOException e) {
                    logger.warn("Failed reading cached variant annotation metadata from {}. Recomputing.", metadataFile, e);
                }
            }
            cachedMetadata = super.getVariantAnnotationMetadata();
            saveVariantAnnotationMetadata(cachedMetadata);
            return cachedMetadata;
        }

        @Override
        public void pre() throws Exception {
            super.pre();
            // Load cache
            if (cacheDir.toFile().exists()) {
                ANNOTATION_CACHE.putAll(loadAnnotationCache(cacheDir, mapper));
            }
        }

        @Override
        protected List<CellBaseDataResult<VariantAnnotation>> annotateFiltered(List<Variant> variants) throws VariantAnnotatorException {
            List<CellBaseDataResult<VariantAnnotation>> results = Arrays.asList(new CellBaseDataResult[variants.size()]);
            List<Variant> variantsToAnnotate = new ArrayList<>();
            Map<String, Integer> variantIndexMap = new HashMap<>();
            for (int i = 0; i < variants.size(); i++) {
                Variant variant = variants.get(i);
                String variantId = variant.toString();
                if (ANNOTATION_CACHE.containsKey(variantId)) {
                    cacheHits++;
                    CellBaseDataResult<VariantAnnotation> result = new CellBaseDataResult<>();
                    result.setId(variantId);
                    result.setResults(Collections.singletonList(ANNOTATION_CACHE.get(variantId)));
                    result.setNumResults(1);
                    results.set(i, result);
                } else {
                    cacheMisses++;
                    variantIndexMap.put(variantId, i);
                    variantsToAnnotate.add(variant);
                }
            }
            if (!variantsToAnnotate.isEmpty()) {
                List<CellBaseDataResult<VariantAnnotation>> queryResults = super.annotateFiltered(variantsToAnnotate);
                for (CellBaseDataResult<VariantAnnotation> queryResult : queryResults) {
                    if (!queryResult.getResults().isEmpty()) {
                        ANNOTATION_CACHE.put(queryResult.getId(), queryResult.getResults().get(0));
                    }
                    results.set(variantIndexMap.get(queryResult.getId()), queryResult);
                }
            }
            return results;
        }

        @Override
        public void post() throws Exception {
            super.post();
            // Save cache
            saveAnnotationCache(cacheDir, mapper);
            if (cachedMetadata != null) {
                saveVariantAnnotationMetadata(cachedMetadata);
            }
            logger.info("Annotation cache stats: {} hits, {} misses", cacheHits, cacheMisses);
        }

        private static synchronized Map<String, VariantAnnotation> loadAnnotationCache(Path dir, ObjectMapper mapper) {
            Path cacheFile = dir.resolve("cache.json.gz");
            if (!Files.exists(cacheFile)) {
                return new HashMap<>();
            }
            try (BufferedReader br = FileUtils.newBufferedReader(cacheFile)) {
                return mapper.readValue(br, new TypeReference<Map<String, VariantAnnotation>>() {});
            } catch (IOException e) {
                logger.warn("Error loading annotation cache from " + cacheFile + ". Starting with empty cache.", e);
                // Delete corrupted cache file
                try {
                    Files.delete(cacheFile);
                } catch (IOException ex) {
                    logger.warn("Failed deleting corrupted cache file " + cacheFile, ex);
                }
                return new HashMap<>();
            }
        }

        private static synchronized void saveAnnotationCache(Path dir, ObjectMapper mapper) {
            try {
                Path cacheFile = dir.resolve("cache.json.gz");
                Files.createDirectories(dir);
                try (BufferedWriter bw = FileUtils.newBufferedWriter(cacheFile)) {
                    mapper.writerWithDefaultPrettyPrinter().writeValue(bw, ANNOTATION_CACHE);
                }
            } catch (IOException e) {
                throw new UncheckedIOException("Error saving annotation cache into " + dir, e);
            }
        }

        private synchronized void saveVariantAnnotationMetadata(ProjectMetadata.VariantAnnotationMetadata metadata)
                throws VariantAnnotatorException {
            try {
                Files.createDirectories(cacheDir);
                try (BufferedWriter bw = FileUtils.newBufferedWriter(metadataFile)) {
                    mapper.writerWithDefaultPrettyPrinter().writeValue(bw, metadata);
                }
            } catch (IOException e) {
                throw new VariantAnnotatorException("Error saving variant annotation metadata into " + metadataFile, e);
            }
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

