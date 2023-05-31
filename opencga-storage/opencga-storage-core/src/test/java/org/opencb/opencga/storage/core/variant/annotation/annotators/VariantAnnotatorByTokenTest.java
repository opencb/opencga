package org.opencb.opencga.storage.core.variant.annotation.annotators;

import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang.StringUtils;
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

public class VariantAnnotatorByTokenTest {

    private StorageConfiguration storageConfiguration;

    private Variant variant = new Variant("10:113588287:G:A");

    private ProjectMetadata projectMetadata;

    @Before
    public void setUp() throws Exception {
        storageConfiguration = StorageConfiguration.load(StorageEngine.class.getClassLoader().getResourceAsStream("storage-configuration.yml"), "yml");
        String url = "https://uk.ws.zettagenomics.com/cellbase/";
        storageConfiguration.getCellbase().setUrl(url);
        storageConfiguration.getCellbase().setDataRelease("1");
        storageConfiguration.getCellbase().setVersion("v5.3");
        storageConfiguration.getCellbase().setToken(null);

        CellBaseUtils cellBaseUtils = new CellBaseUtils(new CellBaseClient(storageConfiguration.getCellbase().toClientConfiguration()));
        try {
            Assume.assumeTrue(cellBaseUtils.isMinVersion("5.3.0"));
        } catch (RuntimeException e) {
            Assume.assumeNoException("Cellbase '" + url + "' not available", e);
        }
    }

    @Test
    public void testNoToken() throws Exception {
        projectMetadata = new ProjectMetadata("hsapiens", "grch37", "1", 1, null, null, null);
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
    public void testCOSMICToken() throws Exception {
        String cosmicToken = System.getenv("CELLBASE_COSMIC_TOKEN");
        Assume.assumeTrue(StringUtils.isNotEmpty(cosmicToken));

        storageConfiguration.getCellbase().setToken(cosmicToken);
        projectMetadata = new ProjectMetadata("hsapiens", "grch37", "1", 1, null, null, null);
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
    public void testHGMDToken() throws Exception {
        String hgmdToken = System.getenv("CELLBASE_HGMD_TOKEN");
        Assume.assumeTrue(StringUtils.isNotEmpty(hgmdToken));

        storageConfiguration.getCellbase().setToken(hgmdToken);
        projectMetadata = new ProjectMetadata("hsapiens", "grch37", "1", 1, null, null, null);
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
    public void testCOSMICandHGMDToken() throws Exception {
        String token = System.getenv("CELLBASE_TOKEN");
        Assume.assumeTrue(StringUtils.isNotEmpty(token));

        storageConfiguration.getCellbase().setToken(token);

        projectMetadata = new ProjectMetadata("hsapiens", "grch37", "1", 1, null, null, null);
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