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

package org.opencb.opencga.storage.mongodb.variant.converters;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.MapperFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.mongodb.util.JSON;
import org.bson.Document;
import org.junit.Before;
import org.junit.Test;
import org.opencb.biodata.models.variant.avro.VariantAnnotation;

import java.util.Arrays;
import java.util.List;
import java.util.Map;

import static java.util.Arrays.asList;
import static java.util.Collections.singletonList;
import static org.junit.Assert.assertEquals;
import static org.opencb.opencga.storage.mongodb.variant.converters.DocumentToVariantAnnotationConverter.*;

/**
 * Created by fjlopez on 23/09/15.
 */
public class DocumentToVariantAnnotationConverterTest {

    private VariantAnnotation variantAnnotation;
    private Document dbObject;
    public static final Document ANY = new Document();
    public static final List ANY_LIST = Arrays.asList();


    @Before
    public void setUp() throws JsonProcessingException {
        //Setup variant
        // 19:45411941:T:C
        // curl 'http://${CELLBASE_HOST}/cellbase/webservices/rest/v3/hsapiens/genomic/variant/19:45411941:T:C/full_annotation?exclude'
        // =expression'
        String variantJson = "{\"chromosome\":\"19\",\"start\":45411941,\"reference\":\"T\",\"alternate\":\"C\"," +
                "\"id\":\"rs429358\"," +
                "\"displayConsequenceType\": \"missense_variant\"," +
                "\"consequenceTypes\":[{\"geneName\":\"TOMM40\",\"ensemblGeneId\":\"ENSG00000130204\"," +
                "\"ensemblTranscriptId\":\"ENST00000252487\",\"strand\":\"+\",\"biotype\":\"protein_coding\"," +
                "\"sequenceOntologyTerms\":[{\"accession\":\"SO:0001632\",\"name\":\"downstream_gene_variant\"}]}," +
                "{\"geneName\":\"TOMM40\",\"ensemblGeneId\":\"ENSG00000130204\",\"ensemblTranscriptId\":\"ENST00000592434\"," +
                "\"strand\":\"+\",\"biotype\":\"protein_coding\",\"sequenceOntologyTerms\":[{\"accession\":\"SO:0001632\"," +
                "\"name\":\"downstream_gene_variant\"}]},{\"geneName\":\"APOE\",\"ensemblGeneId\":\"ENSG00000130203\"," +
                "\"ensemblTranscriptId\":\"ENST00000252486\",\"strand\":\"+\",\"biotype\":\"protein_coding\",\"cdsPosition\":388," +
                "\"codon\":\"Tgc/Cgc\",\"proteinVariantAnnotation\":{\"uniprotAccession\":\"P02649\",\"position\":130," +
                "\"reference\":\"CYS\",\"alternate\":\"ARG\",\"uniprotVariantId\":\"VAR_000652\",\"functionalDescription\":\"In HLPP3; " +
                "form E3**, form E4, form E4/3 and some forms E5-type; only form E3** is disease-linked; dbSNP:rs429358.\"," +
                "\"substitutionScores\":[{\"score\":1.0,\"source\":\"sift\"},{\"score\":0.0,\"source\":\"polyphen\"}]," +
                "\"keywords\":[\"3D-structure\",\"Alzheimer disease\",\"Amyloidosis\",\"Cholesterol metabolism\",\"Chylomicron\"," +
                "\"Complete proteome\",\"Direct protein sequencing\",\"Disease mutation\",\"Glycation\",\"Glycoprotein\",\"HDL\"," +
                "\"Heparin-binding\",\"Hyperlipidemia\",\"Lipid metabolism\",\"Lipid transport\",\"Neurodegeneration\",\"Oxidation\"," +
                "\"Phosphoprotein\",\"Polymorphism\",\"Reference proteome\",\"Repeat\",\"Secreted\",\"Signal\",\"Steroid metabolism\"," +
                "\"Sterol metabolism\",\"Transport\",\"VLDL\"],\"features\":[{\"start\":106,\"end\":141,\"type\":\"helix\"}," +
                "{\"start\":80,\"end\":255,\"type\":\"region of interest\",\"description\":\"8 X 22 AA approximate tandem repeats\"}," +
                "{\"start\":124,\"end\":145,\"type\":\"repeat\",\"description\":\"3\"},{\"id\":\"PRO_0000001987\",\"start\":19," +
                "\"end\":317,\"type\":\"chain\",\"description\":\"Apolipoprotein E\"}]}," +
                "\"sequenceOntologyTerms\":[{\"accession\":\"SO:0001583\",\"name\":\"missense_variant\"}],\"cdnaPosition\":499}," +
                "{\"geneName\":\"APOE\",\"ensemblGeneId\":\"ENSG00000130203\",\"ensemblTranscriptId\":\"ENST00000446996\"," +
                "\"strand\":\"+\",\"biotype\":\"protein_coding\",\"cdsPosition\":388,\"codon\":\"Tgc/Cgc\"," +
                "\"proteinVariantAnnotation\":{\"position\":130,\"reference\":\"CYS\",\"alternate\":\"ARG\"," +
                "\"substitutionScores\":[{\"score\":1.0,\"source\":\"sift\"},{\"score\":0.0,\"source\":\"polyphen\"}]}," +
                "\"sequenceOntologyTerms\":[{\"accession\":\"SO:0001583\",\"name\":\"missense_variant\"}],\"cdnaPosition\":477}," +
                "{\"geneName\":\"APOE\",\"ensemblGeneId\":\"ENSG00000130203\",\"ensemblTranscriptId\":\"ENST00000485628\"," +
                "\"strand\":\"+\",\"biotype\":\"retained_intron\",\"sequenceOntologyTerms\":[{\"accession\":\"SO:0001632\"," +
                "\"name\":\"2KB_downstream_gene_variant\"}]},{\"geneName\":\"APOE\",\"ensemblGeneId\":\"ENSG00000130203\"," +
                "\"ensemblTranscriptId\":\"ENST00000434152\",\"strand\":\"+\",\"biotype\":\"protein_coding\",\"cdsPosition\":466," +
                "\"codon\":\"Tgc/Cgc\",\"proteinVariantAnnotation\":{\"position\":156,\"reference\":\"CYS\",\"alternate\":\"ARG\"," +
                "\"substitutionScores\":[{\"score\":0.91,\"source\":\"sift\"},{\"score\":0.0,\"source\":\"polyphen\"}]}," +
                "\"sequenceOntologyTerms\":[{\"accession\":\"SO:0001583\",\"name\":\"missense_variant\"}],\"cdnaPosition\":523}," +
                "{\"geneName\":\"APOE\",\"ensemblGeneId\":\"ENSG00000130203\",\"ensemblTranscriptId\":\"ENST00000425718\"," +
                "\"strand\":\"+\",\"biotype\":\"protein_coding\",\"cdsPosition\":388,\"codon\":\"Tgc/Cgc\"," +
                "\"proteinVariantAnnotation\":{\"position\":130,\"reference\":\"CYS\",\"alternate\":\"ARG\"," +
                "\"substitutionScores\":[{\"score\":1.0,\"source\":\"sift\"},{\"score\":0.0,\"source\":\"polyphen\"}]}," +
                "\"sequenceOntologyTerms\":[{\"accession\":\"SO:0001583\",\"name\":\"missense_variant\"}],\"cdnaPosition\":653}," +
                "{\"sequenceOntologyTerms\":[{\"accession\":\"\",\"name\":\"regulatory_region_variant\"}]}]," +
                "\"populationFrequencies\":[{\"study\":\"ESP_6500\",\"population\":\"European_American\"," +
                "\"refAllele\":\"T\",\"altAllele\":\"C\",\"refAlleleFreq\":0.88312," +
                "\"altAlleleFreq\":0.11688,\"refHomGenotypeFreq\":0.0,\"hetGenotypeFreq\":0.0,\"altHomGenotypeFreq\":0.0}," +
                "{\"study\":\"ESP_6500\",\"population\":\"African_American\"," +
                "\"refAllele\":\"T\",\"altAllele\":\"C\",\"refAlleleFreq\":0.81068,\"altAlleleFreq\":0.18932,\"refHomGenotypeFreq\":0.0," +
                "\"hetGenotypeFreq\":0.0,\"altHomGenotypeFreq\":0.0},{\"study\":\"1000GENOMES_phase_1\",\"population\":\"AFR\"," +
                "\"refAllele\":\"T\",\"altAllele\":\"C\",\"refAlleleFreq\":0.74,\"altAlleleFreq\":0.26," +
                "\"refHomGenotypeFreq\":0.0,\"hetGenotypeFreq\":0.0,\"altHomGenotypeFreq\":0.0},{\"study\":\"1000GENOMES_phase_1\"," +
                "\"population\":\"AMR\",\"refAllele\":\"T\",\"altAllele\":\"C\",\"refAlleleFreq\":0.89," +
                "\"altAlleleFreq\":0.11,\"refHomGenotypeFreq\":0.0,\"hetGenotypeFreq\":0.0,\"altHomGenotypeFreq\":0.0}," +
                "{\"study\":\"1000GENOMES_phase_1\",\"population\":\"ASN\",\"refAllele\":\"T\"," +
                "\"altAllele\":\"C\",\"refAlleleFreq\":0.91,\"altAlleleFreq\":0.09,\"refHomGenotypeFreq\":0.0,\"hetGenotypeFreq\":0.0," +
                "\"altHomGenotypeFreq\":0.0},{\"study\":\"1000GENOMES_phase_1\",\"population\":\"EUR\"," +
                "\"refAllele\":\"T\",\"altAllele\":\"C\",\"refAlleleFreq\":0.86,\"altAlleleFreq\":0.14,\"refHomGenotypeFreq\":0.0," +
                "\"hetGenotypeFreq\":0.0,\"altHomGenotypeFreq\":0.0},{\"study\":\"1000GENOMES_phase_1\",\"population\":\"ALL\"," +
                "\"refAllele\":\"T\",\"altAllele\":\"C\",\"refAlleleFreq\":0.85,\"altAlleleFreq\":0.15," +
                "\"refHomGenotypeFreq\":0.0,\"hetGenotypeFreq\":0.0,\"altHomGenotypeFreq\":0.0},{\"study\":\"1000G_PHASE_3\"," +
                "\"population\":\"1000G_PHASE_3_ALL\",\"refAllele\":\"T\",\"altAllele\":\"C\"," +
                "\"refAlleleFreq\":0.84944,\"altAlleleFreq\":0.15056,\"refHomGenotypeFreq\":0.0,\"hetGenotypeFreq\":0.0," +
                "\"altHomGenotypeFreq\":0.0},{\"study\":\"1000G_PHASE_3\",\"population\":\"1000G_PHASE_3_SAS\"," +
                "\"refAllele\":\"T\",\"altAllele\":\"C\",\"refAlleleFreq\":0.91309," +
                "\"altAlleleFreq\":0.08691,\"refHomGenotypeFreq\":0.0,\"hetGenotypeFreq\":0.0,\"altHomGenotypeFreq\":0.0}," +
                "{\"study\":\"1000G_PHASE_3\",\"population\":\"1000G_PHASE_3_EAS\"," +
                "\"refAllele\":\"T\",\"altAllele\":\"C\",\"refAlleleFreq\":0.91369,\"altAlleleFreq\":0.08631,\"refHomGenotypeFreq\":0.0," +
                "\"hetGenotypeFreq\":0.0,\"altHomGenotypeFreq\":0.0},{\"study\":\"1000G_PHASE_3\",\"population\":\"1000G_PHASE_3_AMR\"," +
                "\"refAllele\":\"T\",\"altAllele\":\"C\",\"refAlleleFreq\":0.89625," +
                "\"altAlleleFreq\":0.10375,\"refHomGenotypeFreq\":0.0,\"hetGenotypeFreq\":0.0,\"altHomGenotypeFreq\":0.0}," +
                "{\"study\":\"1000G_PHASE_3\",\"population\":\"1000G_PHASE_3_AFR\"," +
                "\"refAllele\":\"T\",\"altAllele\":\"C\",\"refAlleleFreq\":0.73222,\"altAlleleFreq\":0.26778,\"refHomGenotypeFreq\":0.0," +
                "\"hetGenotypeFreq\":0.0,\"altHomGenotypeFreq\":0.0},{\"study\":\"1000G_PHASE_3\",\"population\":\"1000G_PHASE_3_EUR\"," +
                "\"refAllele\":\"T\",\"altAllele\":\"C\",\"refAlleleFreq\":0.84493," +
                "\"altAlleleFreq\":0.15507,\"refHomGenotypeFreq\":0.0,\"hetGenotypeFreq\":0.0,\"altHomGenotypeFreq\":0.0}]," +
                "\"conservation\":[{\"score\":0.11100000143051147,\"source\":\"phastCons\"},{\"score\":0.5609999895095825," +
                "\"source\":\"phylop\"}],\"geneDrugInteraction\":[]," +
                "\"variantTraitAssociation\":{\"clinvar\":[{\"accession\":\"RCV000019456\",\"clinicalSignificance\":\"Pathogenic\"," +
                "\"traits\":[\"APOE4(-)-FREIBURG\"],\"geneNames\":[\"APOE\"],\"reviewStatus\":\"CLASSIFIED_BY_SINGLE_SUBMITTER\"}," +
                "{\"accession\":\"RCV000019455\",\"clinicalSignificance\":\"Pathogenic\",\"traits\":[\"Familial type 3 " +
                "hyperlipoproteinemia\"],\"geneNames\":[\"APOE\"],\"reviewStatus\":\"CLASSIFIED_BY_SINGLE_SUBMITTER\"}," +
                "{\"accession\":\"RCV000019438\",\"clinicalSignificance\":\"Pathogenic\",\"traits\":[\"Familial type 3 " +
                "hyperlipoproteinemia\"],\"geneNames\":[\"APOE\"],\"reviewStatus\":\"CLASSIFIED_BY_SINGLE_SUBMITTER\"}," +
                "{\"accession\":\"RCV000019448\",\"clinicalSignificance\":\"Pathogenic\",\"traits\":[\"ALZHEIMER DISEASE 2, DUE TO APOE4 " +
                "ISOFORM\"],\"geneNames\":[\"APOE\"],\"reviewStatus\":\"CLASSIFIED_BY_SINGLE_SUBMITTER\"}," +
                "{\"accession\":\"RCV000019458\",\"clinicalSignificance\":\"Pathogenic\",\"traits\":[\"APOE4 VARIANT\"]," +
                "\"geneNames\":[\"APOE\"],\"reviewStatus\":\"CLASSIFIED_BY_SINGLE_SUBMITTER\"}],\"gwas\":[{\"snpIdCurrent\":\"429358\"," +
                "\"traits\":[\"Alzheimer's disease biomarkers\"],\"riskAlleleFrequency\":0.28,\"reportedGenes\":\"APOE\"}]," +
                "\"cosmic\":[{\"mutationId\":\"3749517\",\"primarySite\":\"large_intestine\",\"siteSubtype\":\"rectum\"," +
                "\"primaryHistology\":\"carcinoma\",\"histologySubtype\":\"adenocarcinoma\",\"sampleSource\":\"NS\"," +
                "\"tumourOrigin\":\"primary\",\"geneName\":\"APOE\",\"mutationSomaticStatus\":\"Confirmed somatic variant\"}]}}";
        ObjectMapper jsonObjectMapper = new ObjectMapper();
        jsonObjectMapper.configure(MapperFeature.REQUIRE_SETTERS_FOR_GETTERS, true);
        jsonObjectMapper.setSerializationInclusion(JsonInclude.Include.NON_NULL);
        variantAnnotation = jsonObjectMapper.convertValue(JSON.parse(variantJson), VariantAnnotation.class);

//        System.out.println("annotation = " + jsonObjectMapper.writerWithDefaultPrettyPrinter().writeValueAsString(variantAnnotation));
        dbObject = new Document()
                .append(ANNOT_ID_FIELD, null)
                .append(DISPLAY_CONSEQUENCE_TYPE_FIELD, 1583)
                .append(CONSEQUENCE_TYPE_FIELD, asList(
                        new Document(CT_GENE_NAME_FIELD, "TOMM40")
                                .append(CT_ENSEMBL_GENE_ID_FIELD, "ENSG00000130204")
                                .append(CT_ENSEMBL_TRANSCRIPT_ID_FIELD, "ENST00000252487")
                                .append(CT_BIOTYPE_FIELD, "protein_coding")
                                .append(CT_SO_ACCESSION_FIELD, singletonList(1632)),
                        new Document(CT_GENE_NAME_FIELD, "TOMM40")
                                .append(CT_ENSEMBL_GENE_ID_FIELD, "ENSG00000130204")
                                .append(CT_ENSEMBL_TRANSCRIPT_ID_FIELD, "ENST00000592434")
                                .append(CT_BIOTYPE_FIELD, "protein_coding")
                                .append(CT_SO_ACCESSION_FIELD, singletonList(1632)),
                        new Document(CT_GENE_NAME_FIELD, "APOE")
                                .append(CT_ENSEMBL_GENE_ID_FIELD, "ENSG00000130203")
                                .append(CT_ENSEMBL_TRANSCRIPT_ID_FIELD, "ENST00000252486")
                                .append(CT_CODON_FIELD, "Tgc/Cgc")
                                .append(CT_BIOTYPE_FIELD, "protein_coding")
                                .append(CT_C_DNA_POSITION_FIELD, 499)
                                .append(CT_CDS_POSITION_FIELD, 388)
                                .append(CT_SO_ACCESSION_FIELD, singletonList(1583))
                                .append(CT_AA_POSITION_FIELD, 130)
                                .append(CT_AA_REFERENCE_FIELD, "CYS")
                                .append(CT_AA_ALTERNATE_FIELD, "ARG")
                                .append(CT_PROTEIN_UNIPROT_ACCESSION, "P02649")
                                .append(CT_PROTEIN_UNIPROT_VARIANT_ID, "VAR_000652")
                                .append(CT_PROTEIN_FEATURE_DESCRIPTION_FIELD, "In HLPP3; form E3**, form E4, form E4/3 and some forms E5-type; only form E3** is disease-linked; dbSNP:rs429358.")
                                .append(CT_PROTEIN_SIFT_FIELD, new Document()
                                        .append(SCORE_SCORE_FIELD, 1.0)/*.append(SCORE_DESCRIPTION_FIELD, "tolerated")*/)
                                .append(CT_PROTEIN_POLYPHEN_FIELD, new Document()
                                        .append(SCORE_SCORE_FIELD, 0.0)/*.append(SCORE_DESCRIPTION_FIELD, "benign")*/)
                                .append(CT_PROTEIN_KEYWORDS, asList("3D-structure", "Alzheimer disease", "Amyloidosis", "Cholesterol metabolism", "Chylomicron", "Complete proteome", "Direct protein sequencing", "Disease mutation", "Glycation", "Glycoprotein", "HDL", "Heparin-binding", "Hyperlipidemia", "Lipid metabolism", "Lipid transport", "Neurodegeneration", "Oxidation", "Phosphoprotein", "Polymorphism", "Reference proteome", "Repeat", "Secreted", "Signal", "Steroid metabolism", "Sterol metabolism", "Transport", "VLDL"))
                                .append(CT_PROTEIN_FEATURE_FIELD, asList(new Document()
                                                .append(CT_PROTEIN_FEATURE_START_FIELD, 106)
                                                .append(CT_PROTEIN_FEATURE_END_FIELD, 141)
                                                .append(CT_PROTEIN_FEATURE_TYPE_FIELD, "helix"),
                                        new Document()
                                                .append(CT_PROTEIN_FEATURE_START_FIELD, 80)
                                                .append(CT_PROTEIN_FEATURE_END_FIELD, 255)
                                                .append(CT_PROTEIN_FEATURE_TYPE_FIELD, "region of interest")
                                                .append(CT_PROTEIN_FEATURE_DESCRIPTION_FIELD, "8 X 22 AA approximate tandem repeats"),
                                        new Document()
                                                .append(CT_PROTEIN_FEATURE_START_FIELD, 124)
                                                .append(CT_PROTEIN_FEATURE_END_FIELD, 145)
                                                .append(CT_PROTEIN_FEATURE_TYPE_FIELD, "repeat")
                                                .append(CT_PROTEIN_FEATURE_DESCRIPTION_FIELD, "3"),
                                        new Document()
                                                .append(CT_PROTEIN_FEATURE_ID_FIELD, "PRO_0000001987")
                                                .append(CT_PROTEIN_FEATURE_START_FIELD, 19)
                                                .append(CT_PROTEIN_FEATURE_END_FIELD, 317)
                                                .append(CT_PROTEIN_FEATURE_TYPE_FIELD, "chain")
                                                .append(CT_PROTEIN_FEATURE_DESCRIPTION_FIELD, "Apolipoprotein E")

                                )),
                        ANY,
                        ANY,
                        ANY,
                        ANY,
                        new Document(CT_SO_ACCESSION_FIELD, singletonList(1566))
                ))
                .append(CONSERVED_REGION_PHASTCONS_FIELD, new Document(SCORE_SCORE_FIELD, 0.11100000143051147))
                .append(CONSERVED_REGION_PHYLOP_FIELD, new Document(SCORE_SCORE_FIELD, 0.5609999895095825))
//                .append(CLINICAL_DATA_FIELD, new Document()
//                        .append(CLINICAL_COSMIC_FIELD, asList(
//                                new Document()
//                                        .append("mutationId", "3749517")
//                                        .append("primarySite", "large_intestine")
//                                        .append("siteSubtype", "rectum")
//                                        .append("primaryHistology", "carcinoma")
//                                        .append("histologySubtype", "adenocarcinoma")
//                                        .append("sampleSource", "NS")
//                                        .append("tumourOrigin", "primary")
//                                        .append("geneName", "APOE")
//                                        .append("mutationSomaticStatus", "Confirmed somatic variant")))
//                        .append(CLINICAL_GWAS_FIELD, asList(
//                                new Document()
//                                        .append("snpIdCurrent", "429358")
//                                        .append("traits", asList("Alzheimer's disease biomarkers"))
//                                        .append("riskAlleleFrequency", 0.28)
//                                        .append("reportedGenes", "APOE")))
//                        .append(CLINICAL_CLINVAR_FIELD, asList(
//                                new Document()
//                                        .append("accession", "RCV000019456")
//                                        .append("clinicalSignificance", "Pathogenic")
//                                        .append("traits", singletonList("APOE4(-)-FREIBURG"))
//                                        .append("geneNames", singletonList("APOE"))
//                                        .append("reviewStatus", "CLASSIFIED_BY_SINGLE_SUBMITTER"),
//                                new Document()
//                                        .append("accession", "RCV000019455")
//                                        .append("clinicalSignificance", "Pathogenic")
//                                        .append("traits", singletonList("Familial type 3 hyperlipoproteinemia"))
//                                        .append("geneNames", singletonList("APOE"))
//                                        .append("reviewStatus", "CLASSIFIED_BY_SINGLE_SUBMITTER"),
//                                new Document()
//                                        .append("accession", "RCV000019438")
//                                        .append("clinicalSignificance", "Pathogenic")
//                                        .append("traits", singletonList("Familial type 3 hyperlipoproteinemia"))
//                                        .append("geneNames", singletonList("APOE"))
//                                        .append("reviewStatus", "CLASSIFIED_BY_SINGLE_SUBMITTER"),
//                                new Document()
//                                        .append("accession", "RCV000019448")
//                                        .append("clinicalSignificance", "Pathogenic")
//                                        .append("traits", singletonList("ALZHEIMER DISEASE 2, DUE TO APOE4 ISOFORM"))
//                                        .append("geneNames", singletonList("APOE"))
//                                        .append("reviewStatus", "CLASSIFIED_BY_SINGLE_SUBMITTER"),
//                                new Document()
//                                        .append("accession", "RCV000019458")
//                                        .append("clinicalSignificance", "Pathogenic")
//                                        .append("traits", singletonList("APOE4 VARIANT"))
//                                        .append("geneNames", singletonList("APOE"))
//                                        .append("reviewStatus", "CLASSIFIED_BY_SINGLE_SUBMITTER")
//                        )))
                .append(XREFS_FIELD, ANY_LIST)
                .append(POPULATION_FREQUENCIES_FIELD, ANY_LIST)
                .append(GENE_SO_FIELD, ANY_LIST);

//        Document drugDBObject = new Document(DRUG_NAME_FIELD, "TOMM40")
//                .append(DRUG_NAME_FIELD, "PA164712505")
//                .append(DRUG_STUDY_TYPE_FIELD, "PharmGKB");
//        LinkedList drugDBList = new LinkedList();
//        drugDBList.add(drugDBObject);
//        dbObject.append(DRUG_FIELD, drugDBList);


    }

    @Test
    public void testConvertToDataModelType() throws Exception {
        DocumentToVariantAnnotationConverter documentToVariantAnnotationConverter = new DocumentToVariantAnnotationConverter();
        VariantAnnotation convertedVariantAnnotation = documentToVariantAnnotationConverter.convertToDataModelType(dbObject);
        assertEquals(convertedVariantAnnotation.getConsequenceTypes().get(2).getProteinVariantAnnotation().getReference(), "CYS");
//        assertEquals(convertedVariantAnnotation.getVariantTraitAssociation().getCosmic().get(0).getPrimarySite(), "large_intestine");

    }

    @Test
    public void testConvertToStorageType() throws Exception {
        DocumentToVariantAnnotationConverter documentToVariantAnnotationConverter = new DocumentToVariantAnnotationConverter();
        Document convertedDBObject = documentToVariantAnnotationConverter.convertToStorageType(variantAnnotation);
        assertEquals(130, (int) ((Document) ((List) convertedDBObject.get(CONSEQUENCE_TYPE_FIELD)).get(2)).get(CT_AA_POSITION_FIELD));
        assertEquals(0.5609999895095825, ((Document) convertedDBObject.get(CONSERVED_REGION_PHYLOP_FIELD)).getDouble(SCORE_SCORE_FIELD), 0.0001);
//        assertEquals("RCV000019456", ((Document)  convertedDBObject.get(CLINICAL_DATA_FIELD, Document.class)
//                .get(CLINICAL_CLINVAR_FIELD, List.class).get(0)).get("accession", String.class));

//        System.out.println("convertedDBObject = " + convertedDBObject.toJson(new JsonWriterSettings(JsonMode.SHELL, true)));
        checkEqualDocuments(dbObject, convertedDBObject);

    }

    public static void checkEqualDocuments(Document expected, Document actual) {
        try {
            checkEqualObjects(expected, actual, "");
        } catch (AssertionError error) {
            System.out.println("expected = " + expected.toJson());
            System.out.println("actual   = " + actual.toJson());
            throw error;
        }
    }

    private static void checkEqualObjects(Object expected, Object actual, String path) {
        if (expected == ANY || expected == ANY_LIST) {
            // Accept ANY field. Ignore
            return;
        }
        if (expected instanceof Map) {
            expected = new Document((Map) expected);
        }
        if (actual instanceof Map) {
            actual = new Document((Map) actual);
        }
        if (expected instanceof Document && actual instanceof Document) {
            checkEqualObjects((Document) expected, (Document) actual, path);
        } else if (expected instanceof List && actual instanceof List) {
            assertEquals(path + ".size", ((List) expected).size(), ((List) actual).size());
            for (int i = 0; i < ((List) expected).size(); i++) {
                checkEqualObjects(((List) expected).get(i), ((List) actual).get(i), path + '[' + i + ']');
            }
        } else {
            assertEquals("Through " + path, expected, actual);
        }
    }

    private static void checkEqualObjects(Document expected, Document actual, String path) {
        assertEquals("Through " + path, expected.keySet(), actual.keySet());
        for (String key : expected.keySet()) {
            Object e = expected.get(key);
            Object a = actual.get(key);
            checkEqualObjects(e, a, path + '.' + key);
        }
    }

}