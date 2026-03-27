/*
 * Copyright 2015-2020 OpenCB
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

package org.opencb.opencga.catalog.managers.clinical;

import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.opencb.biodata.models.clinical.ClinicalAcmg;
import org.opencb.biodata.models.clinical.ClinicalProperty;
import org.opencb.biodata.models.clinical.Disorder;
import org.opencb.biodata.models.clinical.Phenotype;
import org.opencb.biodata.models.clinical.interpretation.ClinicalVariant;
import org.opencb.biodata.models.clinical.interpretation.ClinicalVariantEvidence;
import org.opencb.biodata.models.clinical.interpretation.MiniPubmed;
import org.opencb.biodata.models.clinical.interpretation.VariantClassification;
import org.opencb.opencga.core.models.clinical.ClinicalAnalysis;
import org.opencb.opencga.core.models.clinical.Interpretation;
import org.opencb.opencga.core.models.individual.Individual;
import org.opencb.opencga.core.models.sample.Sample;
import org.opencb.opencga.core.testclassification.duration.ShortTests;

import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.*;

@Category(ShortTests.class)
public class EmedgeneParserTest {

    private EmedgeneParser parser;
    private Path exampleFile;

    @Before
    public void setUp() {
        parser = new EmedgeneParser();
        exampleFile = Paths.get(getClass().getClassLoader().getResource("emedgene/emedgene_example.json").getPath());
    }

    @Test
    public void testParseExampleFile() throws Exception {
        EmedgeneParser.EmedgeneParseResult result = parser.parse(exampleFile);

        assertNotNull(result);
        assertNotNull(result.getClinicalAnalysis());
        assertNotNull(result.getProband());
        assertNotNull(result.getSample());
        assertNotNull(result.getInterpretation());
        assertNotNull(result.getRawJson());
    }

    @Test
    public void testClinicalAnalysisFields() throws Exception {
        EmedgeneParser.EmedgeneParseResult result = parser.parse(exampleFile);
        ClinicalAnalysis ca = result.getClinicalAnalysis();

        // Check ID from emg_id
        assertEquals("EMG101786000", ca.getId());

        // Check type defaults to SINGLE
        assertEquals(ClinicalAnalysis.Type.SINGLE, ca.getType());

        // Check description
        assertTrue(ca.getDescription().contains("Emedgene"));
        assertTrue(ca.getDescription().contains("EMG101786000"));

        // Check attributes contain raw JSON and metadata
        assertNotNull(ca.getAttributes());
        assertTrue(ca.getAttributes().containsKey("EMEDGENE_RAW"));
        assertEquals("GRCh38", ca.getAttributes().get("genomeBuild"));
        assertEquals("emedgene", ca.getAttributes().get("source"));
    }

    @Test
    public void testProbandAndSample() throws Exception {
        EmedgeneParser.EmedgeneParseResult result = parser.parse(exampleFile);

        // Sample ID from samples.proband
        Sample sample = result.getSample();
        assertEquals("25GM2975", sample.getId());

        // Individual uses sample ID as individual ID
        Individual proband = result.getProband();
        assertEquals("25GM2975", proband.getId());

        // Individual has the sample
        assertNotNull(proband.getSamples());
        assertEquals(1, proband.getSamples().size());
        assertEquals("25GM2975", proband.getSamples().get(0).getId());
    }

    @Test
    public void testPhenotypes() throws Exception {
        EmedgeneParser.EmedgeneParseResult result = parser.parse(exampleFile);
        Individual proband = result.getProband();

        // Proband should have 3 HPO phenotypes
        assertNotNull(proband.getPhenotypes());
        assertEquals(3, proband.getPhenotypes().size());

        Phenotype p1 = proband.getPhenotypes().get(0);
        assertEquals("HP:0001166", p1.getId());
        assertEquals("Arachnodactyly", p1.getName());
        assertEquals("HPO", p1.getSource());
        assertEquals(Phenotype.Status.OBSERVED, p1.getStatus());

        Phenotype p2 = proband.getPhenotypes().get(1);
        assertEquals("HP:0000308", p2.getId());
        assertEquals("Microretrognathia", p2.getName());

        Phenotype p3 = proband.getPhenotypes().get(2);
        assertEquals("HP:0000175", p3.getId());
        assertEquals("Cleft palate", p3.getName());
    }

    @Test
    public void testDisorder() throws Exception {
        EmedgeneParser.EmedgeneParseResult result = parser.parse(exampleFile);
        ClinicalAnalysis ca = result.getClinicalAnalysis();

        // Disorder derived from first variant's evidence.disease
        Disorder disorder = ca.getDisorder();
        assertNotNull(disorder);
        assertEquals("OMIM:251260", disorder.getId());
        assertEquals("Nijmegen breakage syndrome", disorder.getName());
        assertEquals("OMIM", disorder.getSource());

        // Proband should also have the disorder
        Individual proband = result.getProband();
        assertNotNull(proband.getDisorders());
        assertEquals(1, proband.getDisorders().size());
        assertEquals("OMIM:251260", proband.getDisorders().get(0).getId());
    }

    @Test
    public void testInterpretation() throws Exception {
        EmedgeneParser.EmedgeneParseResult result = parser.parse(exampleFile);
        Interpretation interp = result.getInterpretation();

        assertEquals("EMG101786000.1", interp.getId());
        assertTrue(interp.getDescription().contains("EMG101786000"));
        assertNotNull(interp.getMethod());
        assertEquals("emedgene", interp.getMethod().getName());

        // Primary findings: 4 variants
        assertNotNull(interp.getPrimaryFindings());
        assertEquals(4, interp.getPrimaryFindings().size());

        // Secondary findings should be empty
        assertNotNull(interp.getSecondaryFindings());
        assertTrue(interp.getSecondaryFindings().isEmpty());
    }

    @Test
    public void testStructuralVariantDeletion() throws Exception {
        EmedgeneParser.EmedgeneParseResult result = parser.parse(exampleFile);
        List<ClinicalVariant> findings = result.getInterpretation().getPrimaryFindings();

        // First variant: NBN DEL chr8:89978210-89978332
        ClinicalVariant v1 = findings.get(0);
        assertEquals("8", v1.getChromosome());
        assertEquals(89978210, v1.getStart().intValue());
        assertEquals(89978332, v1.getEnd().intValue());

        // Should have "most_likely" tag
        assertNotNull(v1.getTags());
        assertTrue(v1.getTags().contains("most_likely"));
        assertEquals(ClinicalVariant.Status.REPORTED, v1.getStatus());

        // Check evidence
        assertNotNull(v1.getEvidences());
        assertEquals(1, v1.getEvidences().size());
        ClinicalVariantEvidence evidence = v1.getEvidences().get(0);

        // Gene info
        assertNotNull(evidence.getGenomicFeature());
        assertEquals("NBN", evidence.getGenomicFeature().getGeneName());
        assertEquals("GENE", evidence.getGenomicFeature().getType());
        assertEquals("NM_002485", evidence.getGenomicFeature().getTranscriptId());

        // Mode of inheritance
        assertNotNull(evidence.getModeOfInheritances());
        assertEquals(1, evidence.getModeOfInheritances().size());
        assertEquals(ClinicalProperty.ModeOfInheritance.AUTOSOMAL_RECESSIVE, evidence.getModeOfInheritances().get(0));

        // ACMG classification
        VariantClassification classification = evidence.getClassification();
        assertNotNull(classification);
        assertEquals(ClinicalProperty.ClinicalSignificance.UNCERTAIN_SIGNIFICANCE, classification.getClinicalSignificance());

        // CNV ACMG tags with non-zero scores
        List<ClinicalAcmg> acmg = classification.getAcmg();
        assertNotNull(acmg);
        // Only tags with non-zero score should be included (5G has score 0.1)
        assertTrue(acmg.stream().anyMatch(a -> "5G".equals(a.getClassification())));
    }

    @Test
    public void testStructuralVariantDuplication() throws Exception {
        EmedgeneParser.EmedgeneParseResult result = parser.parse(exampleFile);
        List<ClinicalVariant> findings = result.getInterpretation().getPrimaryFindings();

        // Second variant: CNGB3 DUP chr8:86654011-86654063
        ClinicalVariant v2 = findings.get(1);
        assertEquals("8", v2.getChromosome());
        assertEquals(86654011, v2.getStart().intValue());
        assertEquals(86654063, v2.getEnd().intValue());

        ClinicalVariantEvidence evidence = v2.getEvidences().get(0);
        assertEquals("CNGB3", evidence.getGenomicFeature().getGeneName());
        assertEquals(ClinicalProperty.ModeOfInheritance.AUTOSOMAL_RECESSIVE, evidence.getModeOfInheritances().get(0));
    }

    @Test
    public void testSmallVariant() throws Exception {
        EmedgeneParser.EmedgeneParseResult result = parser.parse(exampleFile);
        List<ClinicalVariant> findings = result.getInterpretation().getPrimaryFindings();

        // Third variant: NAIP small_del chr5:70985792 CAT>C
        ClinicalVariant v3 = findings.get(2);
        assertEquals("5", v3.getChromosome());
        assertEquals(70985792, v3.getStart().intValue());

        ClinicalVariantEvidence evidence = v3.getEvidences().get(0);
        assertEquals("NAIP", evidence.getGenomicFeature().getGeneName());
        assertEquals("NM_004536", evidence.getGenomicFeature().getTranscriptId());

        // ACMG tags: PM2 (supporting) and BP4 (supporting)
        VariantClassification classification = evidence.getClassification();
        assertNotNull(classification);
        assertEquals(ClinicalProperty.ClinicalSignificance.UNCERTAIN_SIGNIFICANCE, classification.getClinicalSignificance());
        List<ClinicalAcmg> acmg = classification.getAcmg();
        assertTrue(acmg.stream().anyMatch(a -> "PM2".equals(a.getClassification())));
        assertTrue(acmg.stream().anyMatch(a -> "BP4".equals(a.getClassification())));
    }

    @Test
    public void testLargeStructuralDeletion() throws Exception {
        EmedgeneParser.EmedgeneParseResult result = parser.parse(exampleFile);
        List<ClinicalVariant> findings = result.getInterpretation().getPrimaryFindings();

        // Fourth variant: SMN1,NAIP DEL chr5:70944657-71012915 (68kb)
        ClinicalVariant v4 = findings.get(3);
        assertEquals("5", v4.getChromosome());
        assertEquals(70944657, v4.getStart().intValue());
        assertEquals(71012915, v4.getEnd().intValue());

        // Evidence: first gene is SMN1
        ClinicalVariantEvidence evidence = v4.getEvidences().get(0);
        assertEquals("SMN1", evidence.getGenomicFeature().getGeneName());
    }

    @Test
    public void testVariantReferences() throws Exception {
        EmedgeneParser.EmedgeneParseResult result = parser.parse(exampleFile);
        ClinicalVariant v1 = result.getInterpretation().getPrimaryFindings().get(0);

        // First variant (NBN DEL) has 5 articles
        List<MiniPubmed> refs = v1.getReferences();
        assertNotNull(refs);
        assertEquals(5, refs.size());

        // Check first article
        assertEquals("NBN is known to cause Nijmegen breakage syndrome (OMIM)", refs.get(0).getTitle());
        assertTrue(refs.get(0).getUrl().contains("omim.org"));
    }

    @Test
    public void testVariantConfidence() throws Exception {
        EmedgeneParser.EmedgeneParseResult result = parser.parse(exampleFile);

        // First variant: quality=HIGH
        ClinicalVariant v1 = result.getInterpretation().getPrimaryFindings().get(0);
        assertNotNull(v1.getConfidence());
        assertEquals(org.opencb.biodata.models.clinical.interpretation.ClinicalVariantConfidence.Confidence.HIGH,
                v1.getConfidence().getValue());

        // Third variant: quality=MODERATE → MEDIUM
        ClinicalVariant v3 = result.getInterpretation().getPrimaryFindings().get(2);
        assertNotNull(v3.getConfidence());
        assertEquals(org.opencb.biodata.models.clinical.interpretation.ClinicalVariantConfidence.Confidence.MEDIUM,
                v3.getConfidence().getValue());
    }

    @Test
    public void testVariantAttributes() throws Exception {
        EmedgeneParser.EmedgeneParseResult result = parser.parse(exampleFile);
        ClinicalVariant v1 = result.getInterpretation().getPrimaryFindings().get(0);

        Map<String, Object> attrs = v1.getAttributes();
        assertNotNull(attrs);
        assertEquals("DEL", attrs.get("emedgene_vartype"));
        assertEquals("HIGH", attrs.get("emedgene_quality"));
        assertEquals("Hom", attrs.get("emedgene_proband_zygosity"));
        assertTrue(attrs.containsKey("emedgene_phenomeld_score"));
    }

    @Test
    public void testRawJsonPreserved() throws Exception {
        EmedgeneParser.EmedgeneParseResult result = parser.parse(exampleFile);
        Map<String, Object> rawJson = result.getRawJson();

        assertNotNull(rawJson);
        assertEquals("EMG101786000", rawJson.get("emg_id"));
        assertEquals("GRCh38", rawJson.get("genome_build"));
        assertTrue(rawJson.containsKey("variants"));
        assertTrue(rawJson.containsKey("proband_phenotypes"));
    }

    @Test(expected = org.opencb.opencga.catalog.exceptions.CatalogException.class)
    public void testMissingEmgId() throws Exception {
        parser.parse("{\"samples\": {\"proband\": \"S1\"}}");
    }

    @Test(expected = org.opencb.opencga.catalog.exceptions.CatalogException.class)
    public void testMissingSamplesProband() throws Exception {
        parser.parse("{\"emg_id\": \"EMG1\"}");
    }

    @Test
    public void testMinimalValidJson() throws Exception {
        String json = "{\"emg_id\": \"EMG_MINIMAL\", \"samples\": {\"proband\": \"SAMPLE1\"}}";
        EmedgeneParser.EmedgeneParseResult result = parser.parse(json);

        assertEquals("EMG_MINIMAL", result.getClinicalAnalysis().getId());
        assertEquals("SAMPLE1", result.getSample().getId());
        assertNull(result.getClinicalAnalysis().getDisorder());
        assertEquals(0, result.getInterpretation().getPrimaryFindings().size());
    }
}
