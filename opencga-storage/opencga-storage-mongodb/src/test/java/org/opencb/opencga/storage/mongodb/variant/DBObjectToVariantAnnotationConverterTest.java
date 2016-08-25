package org.opencb.opencga.storage.mongodb.variant;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.MapperFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.mongodb.BasicDBList;
import com.mongodb.BasicDBObject;
import com.mongodb.DBObject;
import com.mongodb.util.JSON;
import org.junit.Before;
import org.junit.Test;
import org.opencb.biodata.models.variant.avro.VariantAnnotation;

import java.util.Collections;
import java.util.List;

import static org.junit.Assert.assertEquals;

/**
 * Created by fjlopez on 23/09/15.
 */
public class DBObjectToVariantAnnotationConverterTest {

    private VariantAnnotation variantAnnotation;
    private BasicDBObject dbObject;

    @Before
    public void setUp() {
        //Setup variant
        // 19:45411941:T:C
        // curl 'http://${CELLBASE_HOST}/cellbase/webservices/rest/v3/hsapiens/genomic/variant/19:45411941:T:C/full_annotation?exclude=expression'
        String variantJson = "{\"chromosome\":\"19\",\"start\":45411941,\"reference\":\"T\",\"alternate\":\"C\",\"id\":\"rs429358\",\"consequenceTypes\":[{\"geneName\":\"TOMM40\",\"ensemblGeneId\":\"ENSG00000130204\",\"ensemblTranscriptId\":\"ENST00000252487\",\"strand\":\"+\",\"biotype\":\"protein_coding\",\"sequenceOntologyTerms\":[{\"accession\":\"SO:0001632\",\"name\":\"downstream_gene_variant\"}]},{\"geneName\":\"TOMM40\",\"ensemblGeneId\":\"ENSG00000130204\",\"ensemblTranscriptId\":\"ENST00000592434\",\"strand\":\"+\",\"biotype\":\"protein_coding\",\"sequenceOntologyTerms\":[{\"accession\":\"SO:0001632\",\"name\":\"downstream_gene_variant\"}]},{\"geneName\":\"APOE\",\"ensemblGeneId\":\"ENSG00000130203\",\"ensemblTranscriptId\":\"ENST00000252486\",\"strand\":\"+\",\"biotype\":\"protein_coding\",\"cdsPosition\":388,\"codon\":\"Tgc/Cgc\",\"proteinVariantAnnotation\":{\"uniprotAccession\":\"P02649\",\"position\":130,\"reference\":\"CYS\",\"alternate\":\"ARG\",\"uniprotVariantId\":\"VAR_000652\",\"functionalDescription\":\"In HLPP3; form E3**, form E4, form E4/3 and some forms E5-type; only form E3** is disease-linked; dbSNP:rs429358.\",\"substitutionScores\":[{\"score\":1.0,\"source\":\"sift\"},{\"score\":0.0,\"source\":\"polyphen\"}],\"keywords\":[\"3D-structure\",\"Alzheimer disease\",\"Amyloidosis\",\"Cholesterol metabolism\",\"Chylomicron\",\"Complete proteome\",\"Direct protein sequencing\",\"Disease mutation\",\"Glycation\",\"Glycoprotein\",\"HDL\",\"Heparin-binding\",\"Hyperlipidemia\",\"Lipid metabolism\",\"Lipid transport\",\"Neurodegeneration\",\"Oxidation\",\"Phosphoprotein\",\"Polymorphism\",\"Reference proteome\",\"Repeat\",\"Secreted\",\"Signal\",\"Steroid metabolism\",\"Sterol metabolism\",\"Transport\",\"VLDL\"],\"features\":[{\"start\":106,\"end\":141,\"type\":\"helix\"},{\"start\":80,\"end\":255,\"type\":\"region of interest\",\"description\":\"8 X 22 AA approximate tandem repeats\"},{\"start\":124,\"end\":145,\"type\":\"repeat\",\"description\":\"3\"},{\"id\":\"PRO_0000001987\",\"start\":19,\"end\":317,\"type\":\"chain\",\"description\":\"Apolipoprotein E\"}]},\"sequenceOntologyTerms\":[{\"accession\":\"SO:0001583\",\"name\":\"missense_variant\"}],\"cdnaPosition\":499},{\"geneName\":\"APOE\",\"ensemblGeneId\":\"ENSG00000130203\",\"ensemblTranscriptId\":\"ENST00000446996\",\"strand\":\"+\",\"biotype\":\"protein_coding\",\"cdsPosition\":388,\"codon\":\"Tgc/Cgc\",\"proteinVariantAnnotation\":{\"position\":130,\"reference\":\"CYS\",\"alternate\":\"ARG\",\"substitutionScores\":[{\"score\":1.0,\"source\":\"sift\"},{\"score\":0.0,\"source\":\"polyphen\"}]},\"sequenceOntologyTerms\":[{\"accession\":\"SO:0001583\",\"name\":\"missense_variant\"}],\"cdnaPosition\":477},{\"geneName\":\"APOE\",\"ensemblGeneId\":\"ENSG00000130203\",\"ensemblTranscriptId\":\"ENST00000485628\",\"strand\":\"+\",\"biotype\":\"retained_intron\",\"sequenceOntologyTerms\":[{\"accession\":\"SO:0001632\",\"name\":\"2KB_downstream_gene_variant\"}]},{\"geneName\":\"APOE\",\"ensemblGeneId\":\"ENSG00000130203\",\"ensemblTranscriptId\":\"ENST00000434152\",\"strand\":\"+\",\"biotype\":\"protein_coding\",\"cdsPosition\":466,\"codon\":\"Tgc/Cgc\",\"proteinVariantAnnotation\":{\"position\":156,\"reference\":\"CYS\",\"alternate\":\"ARG\",\"substitutionScores\":[{\"score\":0.91,\"source\":\"sift\"},{\"score\":0.0,\"source\":\"polyphen\"}]},\"sequenceOntologyTerms\":[{\"accession\":\"SO:0001583\",\"name\":\"missense_variant\"}],\"cdnaPosition\":523},{\"geneName\":\"APOE\",\"ensemblGeneId\":\"ENSG00000130203\",\"ensemblTranscriptId\":\"ENST00000425718\",\"strand\":\"+\",\"biotype\":\"protein_coding\",\"cdsPosition\":388,\"codon\":\"Tgc/Cgc\",\"proteinVariantAnnotation\":{\"position\":130,\"reference\":\"CYS\",\"alternate\":\"ARG\",\"substitutionScores\":[{\"score\":1.0,\"source\":\"sift\"},{\"score\":0.0,\"source\":\"polyphen\"}]},\"sequenceOntologyTerms\":[{\"accession\":\"SO:0001583\",\"name\":\"missense_variant\"}],\"cdnaPosition\":653},{\"sequenceOntologyTerms\":[{\"accession\":\"\",\"name\":\"regulatory_region_variant\"}]}],\"populationFrequencies\":[{\"study\":\"ESP_6500\",\"population\":\"European_American\",\"refAllele\":\"T\",\"altAllele\":\"C\",\"refAlleleFreq\":0.88312,\"altAlleleFreq\":0.11688,\"refHomGenotypeFreq\":0.0,\"hetGenotypeFreq\":0.0,\"altHomGenotypeFreq\":0.0},{\"study\":\"ESP_6500\",\"population\":\"African_American\",\"refAllele\":\"T\",\"altAllele\":\"C\",\"refAlleleFreq\":0.81068,\"altAlleleFreq\":0.18932,\"refHomGenotypeFreq\":0.0,\"hetGenotypeFreq\":0.0,\"altHomGenotypeFreq\":0.0},{\"study\":\"1000GENOMES_phase_1\",\"population\":\"AFR\",\"refAllele\":\"T\",\"altAllele\":\"C\",\"refAlleleFreq\":0.74,\"altAlleleFreq\":0.26,\"refHomGenotypeFreq\":0.0,\"hetGenotypeFreq\":0.0,\"altHomGenotypeFreq\":0.0},{\"study\":\"1000GENOMES_phase_1\",\"population\":\"AMR\",\"refAllele\":\"T\",\"altAllele\":\"C\",\"refAlleleFreq\":0.89,\"altAlleleFreq\":0.11,\"refHomGenotypeFreq\":0.0,\"hetGenotypeFreq\":0.0,\"altHomGenotypeFreq\":0.0},{\"study\":\"1000GENOMES_phase_1\",\"population\":\"ASN\",\"refAllele\":\"T\",\"altAllele\":\"C\",\"refAlleleFreq\":0.91,\"altAlleleFreq\":0.09,\"refHomGenotypeFreq\":0.0,\"hetGenotypeFreq\":0.0,\"altHomGenotypeFreq\":0.0},{\"study\":\"1000GENOMES_phase_1\",\"population\":\"EUR\",\"refAllele\":\"T\",\"altAllele\":\"C\",\"refAlleleFreq\":0.86,\"altAlleleFreq\":0.14,\"refHomGenotypeFreq\":0.0,\"hetGenotypeFreq\":0.0,\"altHomGenotypeFreq\":0.0},{\"study\":\"1000GENOMES_phase_1\",\"population\":\"ALL\",\"refAllele\":\"T\",\"altAllele\":\"C\",\"refAlleleFreq\":0.85,\"altAlleleFreq\":0.15,\"refHomGenotypeFreq\":0.0,\"hetGenotypeFreq\":0.0,\"altHomGenotypeFreq\":0.0},{\"study\":\"1000G_PHASE_3\",\"population\":\"1000G_PHASE_3_ALL\",\"refAllele\":\"T\",\"altAllele\":\"C\",\"refAlleleFreq\":0.84944,\"altAlleleFreq\":0.15056,\"refHomGenotypeFreq\":0.0,\"hetGenotypeFreq\":0.0,\"altHomGenotypeFreq\":0.0},{\"study\":\"1000G_PHASE_3\",\"population\":\"1000G_PHASE_3_SAS\",\"refAllele\":\"T\",\"altAllele\":\"C\",\"refAlleleFreq\":0.91309,\"altAlleleFreq\":0.08691,\"refHomGenotypeFreq\":0.0,\"hetGenotypeFreq\":0.0,\"altHomGenotypeFreq\":0.0},{\"study\":\"1000G_PHASE_3\",\"population\":\"1000G_PHASE_3_EAS\",\"refAllele\":\"T\",\"altAllele\":\"C\",\"refAlleleFreq\":0.91369,\"altAlleleFreq\":0.08631,\"refHomGenotypeFreq\":0.0,\"hetGenotypeFreq\":0.0,\"altHomGenotypeFreq\":0.0},{\"study\":\"1000G_PHASE_3\",\"population\":\"1000G_PHASE_3_AMR\",\"refAllele\":\"T\",\"altAllele\":\"C\",\"refAlleleFreq\":0.89625,\"altAlleleFreq\":0.10375,\"refHomGenotypeFreq\":0.0,\"hetGenotypeFreq\":0.0,\"altHomGenotypeFreq\":0.0},{\"study\":\"1000G_PHASE_3\",\"population\":\"1000G_PHASE_3_AFR\",\"refAllele\":\"T\",\"altAllele\":\"C\",\"refAlleleFreq\":0.73222,\"altAlleleFreq\":0.26778,\"refHomGenotypeFreq\":0.0,\"hetGenotypeFreq\":0.0,\"altHomGenotypeFreq\":0.0},{\"study\":\"1000G_PHASE_3\",\"population\":\"1000G_PHASE_3_EUR\",\"refAllele\":\"T\",\"altAllele\":\"C\",\"refAlleleFreq\":0.84493,\"altAlleleFreq\":0.15507,\"refHomGenotypeFreq\":0.0,\"hetGenotypeFreq\":0.0,\"altHomGenotypeFreq\":0.0}],\"conservation\":[{\"score\":0.11100000143051147,\"source\":\"phastCons\"},{\"score\":0.5609999895095825,\"source\":\"phylop\"}],\"geneDrugInteraction\":[],\"variantTraitAssociation\":{\"clinvar\":[{\"accession\":\"RCV000019456\",\"clinicalSignificance\":\"Pathogenic\",\"traits\":[\"APOE4(-)-FREIBURG\"],\"geneNames\":[\"APOE\"],\"reviewStatus\":\"CLASSIFIED_BY_SINGLE_SUBMITTER\"},{\"accession\":\"RCV000019455\",\"clinicalSignificance\":\"Pathogenic\",\"traits\":[\"Familial type 3 hyperlipoproteinemia\"],\"geneNames\":[\"APOE\"],\"reviewStatus\":\"CLASSIFIED_BY_SINGLE_SUBMITTER\"},{\"accession\":\"RCV000019438\",\"clinicalSignificance\":\"Pathogenic\",\"traits\":[\"Familial type 3 hyperlipoproteinemia\"],\"geneNames\":[\"APOE\"],\"reviewStatus\":\"CLASSIFIED_BY_SINGLE_SUBMITTER\"},{\"accession\":\"RCV000019448\",\"clinicalSignificance\":\"Pathogenic\",\"traits\":[\"ALZHEIMER DISEASE 2, DUE TO APOE4 ISOFORM\"],\"geneNames\":[\"APOE\"],\"reviewStatus\":\"CLASSIFIED_BY_SINGLE_SUBMITTER\"},{\"accession\":\"RCV000019458\",\"clinicalSignificance\":\"Pathogenic\",\"traits\":[\"APOE4 VARIANT\"],\"geneNames\":[\"APOE\"],\"reviewStatus\":\"CLASSIFIED_BY_SINGLE_SUBMITTER\"}],\"gwas\":[{\"snpIdCurrent\":\"429358\",\"traits\":[\"Alzheimer's disease biomarkers\"],\"riskAlleleFrequency\":0.28,\"reportedGenes\":\"APOE\"}],\"cosmic\":[{\"mutationId\":\"3749517\",\"primarySite\":\"large_intestine\",\"siteSubtype\":\"rectum\",\"primaryHistology\":\"carcinoma\",\"histologySubtype\":\"adenocarcinoma\",\"sampleSource\":\"NS\",\"tumourOrigin\":\"primary\",\"geneName\":\"APOE\",\"mutationSomaticStatus\":\"Confirmed somatic variant\"}]}}";
        ObjectMapper jsonObjectMapper = new ObjectMapper();
        jsonObjectMapper.configure(MapperFeature.REQUIRE_SETTERS_FOR_GETTERS, true);
        jsonObjectMapper.setSerializationInclusion(JsonInclude.Include.NON_NULL);
        variantAnnotation = jsonObjectMapper.convertValue(JSON.parse(variantJson), VariantAnnotation.class);

        BasicDBList soDBList1 = new BasicDBList();
        soDBList1.add(1632);
        BasicDBObject ctDBObject1 = new BasicDBObject(DBObjectToVariantAnnotationConverter.GENE_NAME_FIELD, "TOMM40")
                .append(DBObjectToVariantAnnotationConverter.ENSEMBL_GENE_ID_FIELD, "ENSG00000130204")
                .append(DBObjectToVariantAnnotationConverter.ENSEMBL_TRANSCRIPT_ID_FIELD, "ENST00000592434")
                .append(DBObjectToVariantAnnotationConverter.STRAND_FIELD, "+")
                .append(DBObjectToVariantAnnotationConverter.BIOTYPE_FIELD, "protein_coding")
                .append(DBObjectToVariantAnnotationConverter.SO_ACCESSION_FIELD, soDBList1);

        BasicDBList soDBList2 = new BasicDBList();
        soDBList2.add(1583);
        BasicDBObject scoreDBobject1 = new BasicDBObject(DBObjectToVariantAnnotationConverter.SCORE_SOURCE_FIELD, 1.0)
                .append(DBObjectToVariantAnnotationConverter.SCORE_DESCRIPTION_FIELD, "tolerated");
        BasicDBObject scoreDBobject2 = new BasicDBObject(DBObjectToVariantAnnotationConverter.SCORE_SOURCE_FIELD, 0.0)
                .append(DBObjectToVariantAnnotationConverter.SCORE_DESCRIPTION_FIELD, "benign");
        BasicDBObject ctDBObject2 = new BasicDBObject(DBObjectToVariantAnnotationConverter.GENE_NAME_FIELD, "APOE")
                .append(DBObjectToVariantAnnotationConverter.ENSEMBL_GENE_ID_FIELD, "ENSG00000130203")
                .append(DBObjectToVariantAnnotationConverter.ENSEMBL_TRANSCRIPT_ID_FIELD, "ENST00000252486")
                .append(DBObjectToVariantAnnotationConverter.CODON_FIELD, "Tgc/Cgc")
                .append(DBObjectToVariantAnnotationConverter.STRAND_FIELD, "+")
                .append(DBObjectToVariantAnnotationConverter.BIOTYPE_FIELD, "protein_coding")
                .append(DBObjectToVariantAnnotationConverter.C_DNA_POSITION_FIELD, 499)
                .append(DBObjectToVariantAnnotationConverter.CDS_POSITION_FIELD, 388)
                .append(DBObjectToVariantAnnotationConverter.SO_ACCESSION_FIELD, soDBList2)
                .append(DBObjectToVariantAnnotationConverter.AA_POSITION_FIELD, 130)
                .append(DBObjectToVariantAnnotationConverter.AA_REFERENCE_FIELD, "CYS")
                .append(DBObjectToVariantAnnotationConverter.AA_ALTERNATE_FIELD, "ARG")
                .append(DBObjectToVariantAnnotationConverter.SIFT_FIELD, scoreDBobject1)
                .append(DBObjectToVariantAnnotationConverter.POLYPHEN_FIELD, scoreDBobject2);

        BasicDBList soDBList3 = new BasicDBList();
        soDBList3.add(1566);
        BasicDBObject ctDBObject3 = new BasicDBObject(DBObjectToVariantAnnotationConverter.SO_ACCESSION_FIELD,
                soDBList3);

        dbObject = new BasicDBObject("id", "?");
        BasicDBList ctDBList = new BasicDBList();
        ctDBList.add(ctDBObject1);
        ctDBList.add(ctDBObject2);
        ctDBList.add(ctDBObject3);
        dbObject.append("ct", ctDBList);

        BasicDBList conservationDBList = new BasicDBList();
        BasicDBObject conservationScore1 =
                new BasicDBObject(DBObjectToVariantAnnotationConverter.SCORE_SCORE_FIELD, 0.11100000143051147)
                        .append(DBObjectToVariantAnnotationConverter.SCORE_SCORE_FIELD, "phastCons");
        BasicDBObject conservationScore2 =
                new BasicDBObject(DBObjectToVariantAnnotationConverter.SCORE_SCORE_FIELD, 0.5609999895095825)
                        .append(DBObjectToVariantAnnotationConverter.SCORE_SCORE_FIELD, "phylop");
        conservationDBList.add(conservationScore1);
        conservationDBList.add(conservationScore2);
        dbObject.append("cr_score", conservationDBList);

        BasicDBObject drugDBObject = new BasicDBObject(DBObjectToVariantAnnotationConverter.DRUG_NAME_FIELD, "TOMM40")
                .append(DBObjectToVariantAnnotationConverter.DRUG_NAME_FIELD, "PA164712505")
                .append(DBObjectToVariantAnnotationConverter.DRUG_SOURCE_FIELD, "PharmGKB");
        BasicDBList drugDBList = new BasicDBList();
        drugDBList.add(drugDBObject);
        dbObject.append("drug", drugDBList);

        BasicDBObject clinicalDBObject = new BasicDBObject();
        BasicDBObject cosmicDBObject = new BasicDBObject("mutationId", "3749517")
                .append("primarySite", "large_intestine")
                .append("siteSubtype", "rectum")
                .append("primaryHistology", "carcinoma")
                .append("histologySubtype", "adenocarcinoma")
                .append("sampleSource", "NS")
                .append("tumourOrigin", "primary")
                .append("geneName", "APOE")
                .append("mutationSomaticStatus", "Confirmed somatic variant");
        BasicDBList cosmicDBList = new BasicDBList();
        cosmicDBList.add(cosmicDBObject);
        clinicalDBObject.append("cosmic", cosmicDBList);
        BasicDBList traitDBList1 = new BasicDBList();
        traitDBList1.add("Alzheimer's disease biomarkers");
        BasicDBObject gwasDBObject = new BasicDBObject("snpIdCurrent", "429358")
                .append("traits", traitDBList1)
                .append("riskAlleleFrequency", 0.28)
                .append("reportedGenes", "APOE");
        BasicDBList gwasDBList = new BasicDBList();
        gwasDBList.add(gwasDBObject);
        clinicalDBObject.append("gwas", gwasDBList);
        BasicDBList traitDBList2 = new BasicDBList();
        traitDBList2.add("APOE4(-)-FREIBURG");
        BasicDBList geneNameDBList = new BasicDBList();
        geneNameDBList.add("APOE");
        BasicDBObject clinvarDBObject = new BasicDBObject("accession", "RCV000019456")
                .append("clinicalSignificance", "Pathogenic")
                .append("traits", traitDBList2)
                .append("geneNames", geneNameDBList)
                .append("reviewStatus", "CLASSIFIED_BY_SINGLE_SUBMITTER");
        BasicDBList clinvarDBList = new BasicDBList();
        clinvarDBList.add(clinvarDBObject);
        clinicalDBObject.append("clinvar", clinvarDBList);
        dbObject.append("clinical", clinicalDBObject);
    }

    @Test
    public void testConvertToDataModelType() throws Exception {
        DBObjectToVariantAnnotationConverter dbObjectToVariantAnnotationConverter = new DBObjectToVariantAnnotationConverter();
        VariantAnnotation convertedVariantAnnotation = dbObjectToVariantAnnotationConverter.convertToDataModelType(dbObject);
        assertEquals(convertedVariantAnnotation.getConsequenceTypes().get(1).getProteinVariantAnnotation().getReference(), "CYS");
        assertEquals(convertedVariantAnnotation.getGeneDrugInteraction().get(0).getDrugName(), "PA164712505");
        assertEquals(convertedVariantAnnotation.getVariantTraitAssociation().getCosmic().get(0).getPrimarySite(), "large_intestine");

    }

    @Test
    public void testConvertToStorageType() throws Exception {
        DBObjectToVariantAnnotationConverter dbObjectToVariantAnnotationConverter = new DBObjectToVariantAnnotationConverter();
        DBObject convertedDBObject = dbObjectToVariantAnnotationConverter.convertToStorageType(variantAnnotation);
        assertEquals(130, (int) ((BasicDBObject) ((List) convertedDBObject.get(DBObjectToVariantAnnotationConverter.CONSEQUENCE_TYPE_FIELD)).get(2)).get(DBObjectToVariantAnnotationConverter.AA_POSITION_FIELD));
        assertEquals("phylop", (String) ((BasicDBObject) ((List) convertedDBObject.get(DBObjectToVariantAnnotationConverter.CONSERVED_REGION_SCORE_FIELD)).get(1)).get(DBObjectToVariantAnnotationConverter.SCORE_SOURCE_FIELD));
        assertEquals("RCV000019456", (String) ((BasicDBObject) ((List) ((BasicDBObject) convertedDBObject.get(DBObjectToVariantAnnotationConverter.CLINICAL_DATA_FIELD)).get(DBObjectToVariantAnnotationConverter.CLINVAR_FIELD)).get(0)).get("accession"));

    }
}