package org.opencb.opencga.storage.core.variant.annotation.annotators;

import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang.StringUtils;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.opencb.biodata.models.variant.Variant;
import org.opencb.biodata.models.variant.avro.EvidenceEntry;
import org.opencb.biodata.models.variant.avro.VariantAnnotation;
import org.opencb.cellbase.core.result.CellBaseDataResult;
import org.opencb.commons.datastore.core.ObjectMap;
import org.opencb.opencga.core.config.storage.StorageConfiguration;
import org.opencb.opencga.storage.core.StorageEngine;
import org.opencb.opencga.storage.core.metadata.models.ProjectMetadata;
import org.opencb.opencga.storage.core.variant.VariantStorageOptions;
import org.opencb.opencga.storage.core.variant.annotation.VariantAnnotatorException;

import java.util.*;

import static org.hamcrest.CoreMatchers.instanceOf;
import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;
import static org.opencb.opencga.storage.core.variant.VariantStorageOptions.ANNOTATOR_CELLBASE_INCLUDE;

/**
 * Created by jacobo on 27/11/17.
 */
public class VariantAnnotatorByTokenTest {

    private StorageConfiguration storageConfiguration;

    private Variant variant = new Variant("10:113588287:G:A");
    // Secret key = xPacig89igHSieEnveJEi4KCfdEslhmssC3vui1JJQGgDQ0y8v

    @Rule
    public ExpectedException thrown = ExpectedException.none();
    private ProjectMetadata projectMetadata;

    @Before
    public void setUp() throws Exception {
        storageConfiguration = StorageConfiguration.load(StorageEngine.class.getClassLoader().getResourceAsStream("storage-configuration.yml"), "yml");

        storageConfiguration.getCellbase().setUrl("http://127.0.0.1:8080/cellbase-5.3.0-SNAPSHOT/");
        storageConfiguration.getCellbase().setDataRelease("1");
        storageConfiguration.getCellbase().setVersion("v5");

    }

    @Test
    public void testNoToken() throws Exception {
        VariantAnnotator variantAnnotator;

        projectMetadata = new ProjectMetadata("hsapiens", "grch37", "1", "", 1, null, null, null);
        ObjectMap options = new ObjectMap(VariantStorageOptions.ANNOTATOR.key(), "cellbase");
        CellBaseRestVariantAnnotator annotator = new CellBaseRestVariantAnnotator(storageConfiguration, projectMetadata, options);

        List<VariantAnnotation> results = annotator.annotate(Collections.singletonList(variant));
        assertEquals(1, results.size());
        Set<String> sourcesNames = getClinicalSourceNames(results.get(0));
        assertEquals(1, sourcesNames.size());
        assertEquals(true, sourcesNames.contains("clinvar"));
    }

    @Test
    public void testCOSMICToken() throws Exception {
        String cosmicToken = "eyJhbGciOiJIUzI1NiJ9.eyJzb3VyY2VzIjp7ImNvc21pYyI6NDg4Mjg4ODgwMDAwMH0sInZlcnNpb24iOiIxLjAiLCJzdWIiOiJ1Y2FtIiwiaWF0IjoxNjc1MDkyMDc1fQ.EWeH1KH9AtJsOQym5FurkIzsWW2ncP_ZsmlXa9_fGnY";
        storageConfiguration.getCellbase().setToken(cosmicToken);
        projectMetadata = new ProjectMetadata("hsapiens", "grch37", "1", cosmicToken, 1, null, null, null);
        ObjectMap options = new ObjectMap(VariantStorageOptions.ANNOTATOR.key(), "cellbase");
        CellBaseRestVariantAnnotator annotator = new CellBaseRestVariantAnnotator(storageConfiguration, projectMetadata, options);

        List<VariantAnnotation> results = annotator.annotate(Collections.singletonList(variant));
        assertEquals(1, results.size());
        Set<String> sourcesNames = getClinicalSourceNames(results.get(0));
        assertEquals(2, sourcesNames.size());
        assertEquals(true, sourcesNames.contains("clinvar"));
        assertEquals(true, sourcesNames.contains("cosmic"));
    }

    @Test
    public void testHGMDToken() throws Exception {
        String hgmdToken = "eyJhbGciOiJIUzI1NiJ9.eyJzb3VyY2VzIjp7ImhnbWQiOjQ5MTQ0MjQ4MDAwMDB9LCJ2ZXJzaW9uIjoiMS4wIiwic3ViIjoidWNhbSIsImlhdCI6MTY3NTA5MjE2MX0.QQedB2Jr8WdVpPdyG2-FRuv-nvJOhLQMSIdwZMk7bmM";
        storageConfiguration.getCellbase().setToken(hgmdToken);
        projectMetadata = new ProjectMetadata("hsapiens", "grch37", "1", hgmdToken, 1, null, null, null);
        ObjectMap options = new ObjectMap(VariantStorageOptions.ANNOTATOR.key(), "cellbase");
        CellBaseRestVariantAnnotator annotator = new CellBaseRestVariantAnnotator(storageConfiguration, projectMetadata, options);

        List<VariantAnnotation> results = annotator.annotate(Collections.singletonList(variant));
        assertEquals(1, results.size());
        Set<String> sourcesNames = getClinicalSourceNames(results.get(0));
        assertEquals(2, sourcesNames.size());
        assertEquals(true, sourcesNames.contains("clinvar"));
        assertEquals(true, sourcesNames.contains("hgmd"));
    }

    @Test
    public void testCOSMICandHGMDToken() throws Exception {
        String cosmicHgmdToken = "eyJhbGciOiJIUzI1NiJ9.eyJzb3VyY2VzIjp7ImNvc21pYyI6NDg4Mjg4ODgwMDAwMCwiaGdtZCI6NDkxNDQyNDgwMDAwMH0sInZlcnNpb24iOiIxLjAiLCJzdWIiOiJ1Y2FtIiwiaWF0IjoxNjc1MDkyMjQ1fQ.Zu30C9tp5E4kAzmYretQRxmTXoAmE-HkSLJjznkyQNU";
        storageConfiguration.getCellbase().setToken(cosmicHgmdToken);
        projectMetadata = new ProjectMetadata("hsapiens", "grch37", "1", cosmicHgmdToken, 1, null, null, null);
        ObjectMap options = new ObjectMap(VariantStorageOptions.ANNOTATOR.key(), "cellbase");
        options.put(ANNOTATOR_CELLBASE_INCLUDE.key(), "clinical");
        CellBaseRestVariantAnnotator annotator = new CellBaseRestVariantAnnotator(storageConfiguration, projectMetadata, options);

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