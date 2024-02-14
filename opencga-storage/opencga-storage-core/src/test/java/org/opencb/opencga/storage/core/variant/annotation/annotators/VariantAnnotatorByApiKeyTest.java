package org.opencb.opencga.storage.core.variant.annotation.annotators;

import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.junit.Assume;
import org.junit.Before;
import org.junit.Test;
import org.opencb.biodata.models.variant.Variant;
import org.opencb.biodata.models.variant.avro.EvidenceEntry;
import org.opencb.biodata.models.variant.avro.VariantAnnotation;
import org.opencb.cellbase.client.rest.CellBaseClient;
import org.opencb.commons.datastore.core.ObjectMap;
import org.opencb.opencga.core.config.storage.StorageConfiguration;
import org.opencb.opencga.storage.core.StorageEngine;
import org.opencb.opencga.storage.core.metadata.models.ProjectMetadata;
import org.opencb.opencga.storage.core.utils.CellBaseUtils;
import org.opencb.opencga.storage.core.variant.VariantStorageOptions;

import java.util.*;

import static org.junit.Assert.assertEquals;
import static org.opencb.opencga.storage.core.variant.VariantStorageOptions.ANNOTATOR_CELLBASE_INCLUDE;

public class VariantAnnotatorByApiKeyTest {

    private StorageConfiguration storageConfiguration;

    private Variant variant = new Variant("10:113588287:G:A");

    private ProjectMetadata projectMetadata;

    @Before
    public void setUp() throws Exception {
        storageConfiguration = StorageConfiguration.load(StorageEngine.class.getClassLoader().getResourceAsStream("storage-configuration.yml"), "yml");
        String url = "https://uk.ws.zettagenomics.com/cellbase/";
        storageConfiguration.getCellbase().setUrl(url);
        storageConfiguration.getCellbase().setDataRelease("3");
        storageConfiguration.getCellbase().setVersion("v5.4");
        storageConfiguration.getCellbase().setApiKey(null);

        CellBaseUtils cellBaseUtils = new CellBaseUtils(new CellBaseClient(storageConfiguration.getCellbase().toClientConfiguration()));
        Assume.assumeTrue(cellBaseUtils.isMinVersion("v5.4"));

        projectMetadata = new ProjectMetadata("hsapiens", "grch38", "3", 1, null, null, null);
    }

    @Test
    public void testNoApiKey() throws Exception {
        ObjectMap options = new ObjectMap(VariantStorageOptions.ANNOTATOR.key(), "cellbase");
        CellBaseRestVariantAnnotator annotator = new CellBaseRestVariantAnnotator(storageConfiguration, projectMetadata, options);

        List<VariantAnnotation> results = annotator.annotate(Collections.singletonList(variant));
        assertEquals(Collections.emptyList(), annotator.getVariantAnnotationMetadata().getPrivateSources());
        assertEquals(1, results.size());
        Set<String> sourcesNames = getClinicalSourceNames(results.get(0));
        assertEquals(1, sourcesNames.size());
        assertEquals(true, sourcesNames.contains("clinvar"));
    }

    @Test
    public void testCOSMICApiKey() throws Exception {
        String apiKey = System.getenv("CELLBASE_COSMIC_APIKEY");
        Assume.assumeTrue(StringUtils.isNotEmpty(apiKey));

        storageConfiguration.getCellbase().setApiKey(apiKey);

        ObjectMap options = new ObjectMap(VariantStorageOptions.ANNOTATOR.key(), "cellbase");
        CellBaseRestVariantAnnotator annotator = new CellBaseRestVariantAnnotator(storageConfiguration, projectMetadata, options);
        assertEquals(Collections.singletonList("cosmic"), annotator.getVariantAnnotationMetadata().getPrivateSources());

        List<VariantAnnotation> results = annotator.annotate(Collections.singletonList(variant));
        assertEquals(1, results.size());
        Set<String> sourcesNames = getClinicalSourceNames(results.get(0));
        assertEquals(2, sourcesNames.size());
        assertEquals(true, sourcesNames.contains("clinvar"));
        assertEquals(true, sourcesNames.contains("cosmic"));
    }

    @Test
    public void testHGMDApiKey() throws Exception {
        String apiKey = System.getenv("CELLBASE_HGMD_APIKEY");
        Assume.assumeTrue(StringUtils.isNotEmpty(apiKey));

        storageConfiguration.getCellbase().setApiKey(apiKey);

        ObjectMap options = new ObjectMap(VariantStorageOptions.ANNOTATOR.key(), "cellbase");
        CellBaseRestVariantAnnotator annotator = new CellBaseRestVariantAnnotator(storageConfiguration, projectMetadata, options);
        assertEquals(Collections.singletonList("hgmd"), annotator.getVariantAnnotationMetadata().getPrivateSources());

        List<VariantAnnotation> results = annotator.annotate(Collections.singletonList(variant));
        assertEquals(1, results.size());
        Set<String> sourcesNames = getClinicalSourceNames(results.get(0));
        assertEquals(2, sourcesNames.size());
        assertEquals(true, sourcesNames.contains("clinvar"));
        assertEquals(true, sourcesNames.contains("hgmd"));
    }

    @Test
    public void testCOSMICandHGMDApiKey() throws Exception {
        String apiKey = System.getenv("CELLBASE_COSMIC_HGMD_APIKEY");
        Assume.assumeTrue(StringUtils.isNotEmpty(apiKey));

        storageConfiguration.getCellbase().setApiKey(apiKey);

        ObjectMap options = new ObjectMap(VariantStorageOptions.ANNOTATOR.key(), "cellbase");
        options.put(ANNOTATOR_CELLBASE_INCLUDE.key(), "clinical");
        CellBaseRestVariantAnnotator annotator = new CellBaseRestVariantAnnotator(storageConfiguration, projectMetadata, options);
        assertEquals(new HashSet<>(Arrays.asList("hgmd", "cosmic")),
                new HashSet<>(annotator.getVariantAnnotationMetadata().getPrivateSources()));

        List<VariantAnnotation> results = annotator.annotate(Collections.singletonList(variant));
        assertEquals(1, results.size());
        Set<String> sourcesNames = getClinicalSourceNames(results.get(0));
        assertEquals(3, sourcesNames.size());
        assertEquals(true, sourcesNames.contains("clinvar"));
        assertEquals(true, sourcesNames.contains("cosmic"));
        assertEquals(true, sourcesNames.contains("hgmd"));
    }

    private Set<String> getClinicalSourceNames(VariantAnnotation variantAnnotation) {
        Set<String> sourceNames = new HashSet<>();
        if (variantAnnotation != null && CollectionUtils.isNotEmpty(variantAnnotation.getTraitAssociation())) {
            for (EvidenceEntry entry : variantAnnotation.getTraitAssociation()) {
                if (entry.getSource() != null && StringUtils.isNotEmpty(entry.getSource().getName())) {
                    sourceNames.add(entry.getSource().getName());
                }
            }
        }
        return sourceNames;
    }
}