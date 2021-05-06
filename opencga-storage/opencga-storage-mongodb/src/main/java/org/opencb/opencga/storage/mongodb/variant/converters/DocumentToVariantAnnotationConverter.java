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
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectWriter;
import com.fasterxml.jackson.databind.SerializationFeature;
import org.apache.avro.generic.GenericRecord;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.bson.Document;
import org.opencb.biodata.models.variant.Variant;
import org.opencb.biodata.models.variant.annotation.ConsequenceTypeMappings;
import org.opencb.biodata.models.variant.avro.*;
import org.opencb.commons.datastore.core.ComplexTypeConverter;
import org.opencb.opencga.storage.core.variant.adaptors.VariantField;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

import static org.opencb.opencga.storage.mongodb.variant.converters.DocumentToVariantConverter.ANNOTATION_FIELD;

/**
 * Created by jacobo on 13/01/15.
 */
public class DocumentToVariantAnnotationConverter
        extends AbstractDocumentConverter
        implements ComplexTypeConverter<VariantAnnotation, Document> {

    public static final String ANNOT_ID_FIELD = "id";

    public static final String GENE_SO_FIELD = "_gn_so";

    public static final String CONSEQUENCE_TYPE_FIELD = "ct";
    public static final String CT_GENE_NAME_FIELD = "gn";
    public static final String CT_ENSEMBL_GENE_ID_FIELD = "ensg";
    public static final String CT_ENSEMBL_TRANSCRIPT_ID_FIELD = "enst";
    public static final String CT_RELATIVE_POS_FIELD = "relPos";
    public static final String CT_CODON_FIELD = "codon";
    public static final String CT_STRAND_FIELD = "strand";
    public static final String CT_BIOTYPE_FIELD = "bt";
    public static final String CT_EXON_OVERLAP_FIELD = "exn";
    public static final String CT_EXON_OVERLAP_NUMBER_FIELD = "n";
    public static final String CT_EXON_OVERLAP_PERCENTAGE_FIELD = "p";
    public static final String CT_TRANSCRIPT_ANNOT_FLAGS = "flags";
    public static final String CT_C_DNA_POSITION_FIELD = "cDnaPos";
    public static final String CT_CDS_POSITION_FIELD = "cdsPos";
    public static final String CT_AA_POSITION_FIELD = "aaPos";
    public static final String CT_AA_REFERENCE_FIELD = "aaRef";
    public static final String CT_AA_ALTERNATE_FIELD = "aaAlt";
    public static final String CT_SO_ACCESSION_FIELD = "so";
    public static final String CT_PROTEIN_KEYWORDS = "kw";
    public static final String CT_PROTEIN_SUBSTITUTION_SCORE_FIELD = "ps_score";
    public static final String CT_PROTEIN_POLYPHEN_FIELD = "polyphen";
    public static final String CT_PROTEIN_SIFT_FIELD = "sift";
    public static final String CT_PROTEIN_FEATURE_FIELD = "pd";
    public static final String CT_PROTEIN_FEATURE_ID_FIELD = "id";
    public static final String CT_PROTEIN_FEATURE_START_FIELD = "start";
    public static final String CT_PROTEIN_FEATURE_END_FIELD = "end";
    public static final String CT_PROTEIN_FEATURE_TYPE_FIELD = "type";
    public static final String CT_PROTEIN_FEATURE_DESCRIPTION_FIELD = "desc";
    public static final String CT_PROTEIN_UNIPROT_ACCESSION = "uni_a";
    public static final String CT_PROTEIN_UNIPROT_NAME = "uni_n";
    public static final String CT_PROTEIN_ID = "p_id";
    public static final String CT_PROTEIN_UNIPROT_VARIANT_ID = "uni_var";
    public static final String CT_PROTEIN_FUNCTIONAL_DESCRIPTION = "desc";

    public static final String DISPLAY_CONSEQUENCE_TYPE_FIELD = "d_ct";

    public static final String HGVS_FIELD = "hgvs";

    public static final String CYTOBANDS_FIELD = "cytob";
    public static final String CYTOBAND_STAIN_FIELD = "stain";
    public static final String CYTOBAND_NAME_FIELD = "name";
    public static final String CYTOBAND_START_FIELD = "start";
    public static final String CYTOBAND_END_FIELD = "end";

    public static final String XREFS_FIELD = "xrefs";
    public static final String XREF_ID_FIELD = "id";
    public static final String XREF_SOURCE_FIELD = "src";

    public static final String POPULATION_FREQUENCIES_FIELD = "popFq";
    public static final String POPULATION_FREQUENCY_STUDY_FIELD = "study";
    public static final String POPULATION_FREQUENCY_POP_FIELD = "pop";
    //    public static final String POPULATION_FREQUENCY_REFERENCE_ALLELE_FIELD = "ref";
//    public static final String POPULATION_FREQUENCY_ALTERNATE_ALLELE_FIELD = "alt";
    public static final String POPULATION_FREQUENCY_REFERENCE_FREQUENCY_FIELD = "refFq";
    public static final String POPULATION_FREQUENCY_ALTERNATE_FREQUENCY_FIELD = "altFq";
    public static final String POPULATION_FREQUENCY_REF_HOM_GT_FIELD = "hetFq";
    public static final String POPULATION_FREQUENCY_HET_GT_FIELD = "refHomFq";
    public static final String POPULATION_FREQUENCY_ALT_HOM_GT_FIELD = "altHomFq";

    public static final String CONSERVED_REGION_SCORE_FIELD = "cr_score";
    public static final String CONSERVED_REGION_PHYLOP_FIELD = "cr_phylop";
    public static final String CONSERVED_REGION_PHASTCONS_FIELD = "cr_phastcons";
    public static final String CONSERVED_REGION_GERP_FIELD = "cr_gerp";

    public static final String GENE_TRAIT_FIELD = "gn_trait";
    public static final String GENE_TRAIT_ID_FIELD = "id";
    public static final String GENE_TRAIT_NAME_FIELD = "name";
    public static final String GENE_TRAIT_HPO_FIELD = "hpo";
    public static final String GENE_TRAIT_SCORE_FIELD = "sc";
    public static final String GENE_TRAIT_PUBMEDS_FIELD = "nPubmed";
    public static final String GENE_TRAIT_TYPES_FIELD = "types";
    public static final String GENE_TRAIT_SOURCES_FIELD = "srcs";
    public static final String GENE_TRAIT_SOURCE_FIELD = "src";

    public static final String DRUG_FIELD = "drug";
    public static final String DRUG_NAME_FIELD = "dn";
    public static final String DRUG_GENE_FIELD = CT_GENE_NAME_FIELD;
    public static final String DRUG_SOURCE_FIELD = "src";
    public static final String DRUG_STUDY_TYPE_FIELD = "st";
    public static final String DRUG_TYPE_FIELD = "type";

    public static final String SCORE_SCORE_FIELD = "sc";
    public static final String SCORE_SOURCE_FIELD = "src";
    public static final String SCORE_DESCRIPTION_FIELD = "desc";

    public static final String CLINICAL_DATA_FIELD = "clinical";
//    public static final String CLINICAL_COSMIC_FIELD = "cosmic";
//    public static final String CLINICAL_GWAS_FIELD = "gwas";
//    public static final String CLINICAL_CLINVAR_FIELD = "clinvar";

    public static final String FUNCTIONAL_SCORE = "fn_score";
    public static final String FUNCTIONAL_CADD_RAW_FIELD = "fn_cadd_r";
    public static final String FUNCTIONAL_CADD_SCALED_FIELD = "fn_cadd_s";

    public static final String REPEATS_FIELD = "repeats";
    public static final String REPEATS_CHROMOSOME_FIELD = "chr";
    public static final String REPEATS_START_FIELD = "start";
    public static final String REPEATS_END_FIELD = "end";
    public static final String REPEATS_CONSENSUS_SIZE_FIELD = "consensusSize";
    public static final String REPEATS_COPY_NUMBER_FIELD = "cn";
    public static final String REPEATS_PERCENTAGE_MATCH_FIELD = "pm";
    public static final String REPEATS_ID_FIELD = "id";
    public static final String REPEATS_PERIOD_FIELD = "period";
    public static final String REPEATS_SCORE_FIELD = "sc";
    public static final String REPEATS_SEQUENCE_FIELD = "sequence";
    public static final String REPEATS_SOURCE_FIELD = "src";

    public static final String DEFAULT_STRAND_VALUE = "+";
    public static final String DEFAULT_DRUG_SOURCE = "dgidb";
    public static final String DEFAULT_DRUG_TYPE = "n/a";

    public static final Map<String, String> SCORE_FIELD_MAP;
    public static final String DB_SNP = "dbSNP";

    private final ObjectMapper jsonObjectMapper;
    private final ObjectWriter writer;

    protected static Logger logger = LoggerFactory.getLogger(DocumentToVariantAnnotationConverter.class);

    public static final String POLYPHEN = "polyphen";
    public static final String SIFT = "sift";

    public static final String PHAST_CONS = "phastCons";
    public static final String PHYLOP = "phylop";
    public static final String GERP = "gerp";

    public static final String CADD_SCALED = "cadd_scaled";
    public static final String CADD_RAW = "cadd_raw";

    static {
        Map<String, String> scoreFieldMap = new HashMap<>(7);
        scoreFieldMap.put(SIFT, ANNOTATION_FIELD + "." + CONSEQUENCE_TYPE_FIELD + "." + CT_PROTEIN_SIFT_FIELD);
        scoreFieldMap.put(POLYPHEN, ANNOTATION_FIELD + "." + CONSEQUENCE_TYPE_FIELD + "." + CT_PROTEIN_POLYPHEN_FIELD);
        scoreFieldMap.put(PHAST_CONS, ANNOTATION_FIELD + "." + CONSERVED_REGION_PHASTCONS_FIELD);
        scoreFieldMap.put(PHYLOP, ANNOTATION_FIELD + "." + CONSERVED_REGION_PHYLOP_FIELD);
        scoreFieldMap.put(GERP, ANNOTATION_FIELD + "." + CONSERVED_REGION_GERP_FIELD);
        scoreFieldMap.put(CADD_SCALED, ANNOTATION_FIELD + "." + FUNCTIONAL_CADD_SCALED_FIELD);
        scoreFieldMap.put(CADD_RAW, ANNOTATION_FIELD + "." + FUNCTIONAL_CADD_RAW_FIELD);
        SCORE_FIELD_MAP = Collections.unmodifiableMap(scoreFieldMap);
    }

    private Integer annotationId = null;
    private Map<Integer, String> annotationIds = Collections.emptyMap();

    public DocumentToVariantAnnotationConverter(Map<Integer, String> annotationIds) {
        this();
        this.annotationIds = annotationIds;
    }

    public DocumentToVariantAnnotationConverter(Integer annotationId) {
        this();
        this.annotationId = annotationId;
    }

    public DocumentToVariantAnnotationConverter() {
        jsonObjectMapper = new ObjectMapper();
        jsonObjectMapper.configure(SerializationFeature.WRITE_NULL_MAP_VALUES, false);
        jsonObjectMapper.setSerializationInclusion(JsonInclude.Include.NON_NULL);
        writer = jsonObjectMapper.writer();
    }

    @Override
    public VariantAnnotation convertToDataModelType(Document object) {
        return convertToDataModelType(object, null, null);
    }


    public VariantAnnotation convertToDataModelType(Document object, Document customAnnotation, Variant variant) {
        String chromosome = null;
        String reference = null;
        String alternate = null;

        VariantAnnotation va;
        if (variant != null) {
            chromosome = variant.getChromosome();
            reference = variant.getReference();
            alternate = variant.getAlternate();

            va = newVariantAnnotation(variant);
        } else {
            va = new VariantAnnotation();
        }

        //ConsequenceType
        List<ConsequenceType> consequenceTypes = new LinkedList<>();
        Object cts = object.get(CONSEQUENCE_TYPE_FIELD);
        if (cts != null && cts instanceof List) {
            for (Object o : ((List) cts)) {
                if (o instanceof Document) {
                    Document ct = (Document) o;

                    //SO accession name
                    List<String> soAccessionNames = new LinkedList<>();
                    if (ct.containsKey(CT_SO_ACCESSION_FIELD)) {
                        if (ct.get(CT_SO_ACCESSION_FIELD) instanceof List) {
                            List<Integer> list = (List) ct.get(CT_SO_ACCESSION_FIELD);
                            for (Integer so : list) {
                                soAccessionNames.add(ConsequenceTypeMappings.accessionToTerm.get(so));
                            }
                        } else {
                            soAccessionNames.add(ConsequenceTypeMappings.accessionToTerm.get(ct.getInteger(CT_SO_ACCESSION_FIELD)));
                        }
                    }

                    //ProteinSubstitutionScores
                    List<Score> proteinSubstitutionScores = new LinkedList<>();
                    if (ct.containsKey(CT_PROTEIN_SUBSTITUTION_SCORE_FIELD)) {
                        List<Document> list = (List) ct.get(CT_PROTEIN_SUBSTITUTION_SCORE_FIELD);
                        for (Document document : list) {
                            proteinSubstitutionScores.add(buildScore(document));
                        }
                    }
                    addScore(ct, proteinSubstitutionScores, POLYPHEN, CT_PROTEIN_POLYPHEN_FIELD);
                    addScore(ct, proteinSubstitutionScores, SIFT, CT_PROTEIN_SIFT_FIELD);


                    List<ProteinFeature> features = new ArrayList<>();
                    if (ct.containsKey(CT_PROTEIN_FEATURE_FIELD)) {
                        List<Document> featureDocuments = (List) ct.get(CT_PROTEIN_FEATURE_FIELD);
                        for (Document featureDocument : featureDocuments) {
                            features.add(new ProteinFeature(
                                    getDefault(featureDocument, CT_PROTEIN_FEATURE_ID_FIELD, ""),
                                    getDefault(featureDocument, CT_PROTEIN_FEATURE_START_FIELD, 0),
                                    getDefault(featureDocument, CT_PROTEIN_FEATURE_END_FIELD, 0),
                                    getDefault(featureDocument, CT_PROTEIN_FEATURE_TYPE_FIELD, ""),
                                    getDefault(featureDocument, CT_PROTEIN_FEATURE_DESCRIPTION_FIELD, "")
                            ));
                        }
                    }

                    ProteinVariantAnnotation proteinVariantAnnotation = buildProteinVariantAnnotation(
                            getDefault(ct, CT_PROTEIN_UNIPROT_ACCESSION, (String) null),
                            getDefault(ct, CT_PROTEIN_UNIPROT_NAME, (String) null),
                            getDefault(ct, CT_PROTEIN_ID, (String) null),
                            getDefault(ct, CT_AA_POSITION_FIELD, 0),
                            getDefault(ct, CT_AA_REFERENCE_FIELD, ""),
                            getDefault(ct, CT_AA_ALTERNATE_FIELD, ""),
                            getDefault(ct, CT_PROTEIN_UNIPROT_VARIANT_ID, (String) null),
                            getDefault(ct, CT_PROTEIN_FUNCTIONAL_DESCRIPTION, (String) null),
                            proteinSubstitutionScores,
                            getDefault(ct, CT_PROTEIN_KEYWORDS, Collections.emptyList()),
                            features);
                    consequenceTypes.add(buildConsequenceType(
                            getDefault(ct, CT_GENE_NAME_FIELD, ""),
                            getDefault(ct, CT_ENSEMBL_GENE_ID_FIELD, ""),
                            getDefault(ct, CT_ENSEMBL_TRANSCRIPT_ID_FIELD, ""),
                            getDefault(ct, CT_STRAND_FIELD, "+"),
                            getDefault(ct, CT_BIOTYPE_FIELD, ""),
                            getDefault(ct, CT_EXON_OVERLAP_FIELD, Collections.emptyList()),
                            getDefault(ct, CT_TRANSCRIPT_ANNOT_FLAGS, Collections.emptyList()),
                            getDefault(ct, CT_C_DNA_POSITION_FIELD, 0),
                            getDefault(ct, CT_CDS_POSITION_FIELD, 0),
                            getDefault(ct, CT_CODON_FIELD, ""),
                            soAccessionNames,
                            proteinVariantAnnotation));
                }
            }

        }
        va.setConsequenceTypes(consequenceTypes);
        Integer displaySO = object.getInteger(DISPLAY_CONSEQUENCE_TYPE_FIELD);
        if (displaySO != null) {
            va.setDisplayConsequenceType(ConsequenceTypeMappings.accessionToTerm.get(displaySO));
        }

        va.setHgvs(getDefault(object, HGVS_FIELD, Collections.emptyList()));

        List<Document> cytobandsDocument = getDefault(object, CYTOBANDS_FIELD, Collections.emptyList());
        for (Document c : cytobandsDocument) {
            List<Cytoband> cytobands = new ArrayList<>(cytobandsDocument.size());
            cytobands.add(new Cytoband(
                    chromosome,
                    getDefault(c, CYTOBAND_STAIN_FIELD, ""),
                    getDefault(c, CYTOBAND_NAME_FIELD, ""),
                    getDefault(c, CYTOBAND_START_FIELD, 0),
                    getDefault(c, CYTOBAND_END_FIELD, 0)
            ));
            va.setCytoband(cytobands);
        }

        //Conserved Region Scores
        List<Score> conservedRegionScores = new LinkedList<>();
        if (object.containsKey(CONSERVED_REGION_SCORE_FIELD)) {
            List<Document> list = (List) object.get(CONSERVED_REGION_SCORE_FIELD);
            for (Document dbObject : list) {
                conservedRegionScores.add(buildScore(dbObject));
            }
        }
        addScore(object, conservedRegionScores, PHAST_CONS, CONSERVED_REGION_PHASTCONS_FIELD);
        addScore(object, conservedRegionScores, PHYLOP, CONSERVED_REGION_PHYLOP_FIELD);
        addScore(object, conservedRegionScores, GERP, CONSERVED_REGION_GERP_FIELD);
        va.setConservation(conservedRegionScores);

        //Population frequencies
        List<PopulationFrequency> populationFrequencies = new LinkedList<>();
        if (object.containsKey(POPULATION_FREQUENCIES_FIELD)) {
            List<Document> list = (List) object.get(POPULATION_FREQUENCIES_FIELD);
            for (Document dbObject : list) {
                populationFrequencies.add(new PopulationFrequency(
                        getDefault(dbObject, POPULATION_FREQUENCY_STUDY_FIELD, ""),
                        getDefault(dbObject, POPULATION_FREQUENCY_POP_FIELD, ""),
                        reference,
                        alternate,
                        getDefault(dbObject, POPULATION_FREQUENCY_REFERENCE_FREQUENCY_FIELD, -1.0F),
                        getDefault(dbObject, POPULATION_FREQUENCY_ALTERNATE_FREQUENCY_FIELD, -1.0F),
                        getDefault(dbObject, POPULATION_FREQUENCY_REF_HOM_GT_FIELD, -1.0F),
                        getDefault(dbObject, POPULATION_FREQUENCY_HET_GT_FIELD, -1.0F),
                        getDefault(dbObject, POPULATION_FREQUENCY_ALT_HOM_GT_FIELD, -1.0F)
                ));
            }
        }
        va.setPopulationFrequencies(populationFrequencies);

        // Gene trait association
        List<GeneTraitAssociation> geneTraitAssociations = new LinkedList<>();
        if (object.containsKey(GENE_TRAIT_FIELD)) {
            List<Document> list = (List) object.get(GENE_TRAIT_FIELD);
            for (Document document : list) {
                geneTraitAssociations.add(new GeneTraitAssociation(
                        getDefault(document, GENE_TRAIT_ID_FIELD, ""),
                        getDefault(document, GENE_TRAIT_NAME_FIELD, ""),
                        getDefault(document, GENE_TRAIT_HPO_FIELD, (String) null),
                        getDefault(document, GENE_TRAIT_SCORE_FIELD, (Float) null),
                        getDefault(document, GENE_TRAIT_PUBMEDS_FIELD, 0),
                        getDefault(document, GENE_TRAIT_TYPES_FIELD, Collections.emptyList()),
                        getDefault(document, GENE_TRAIT_SOURCES_FIELD, Collections.emptyList()),
                        getDefault(document, GENE_TRAIT_SOURCE_FIELD, "")
                ));
            }
        }
        va.setGeneTraitAssociation(geneTraitAssociations);


        // Drug-Gene Interactions
        List<GeneDrugInteraction> drugs = new LinkedList<>();
        if (object.containsKey(DRUG_FIELD)) {
            List<Document> list = (List) object.get(DRUG_FIELD);
            for (Document dbObject : list) {
                //drugs.add(dbObject.toMap());
                drugs.add(new GeneDrugInteraction(
                        getDefault(dbObject, DRUG_GENE_FIELD, ""),
                        getDefault(dbObject, DRUG_NAME_FIELD, ""),
                        getDefault(dbObject, DRUG_SOURCE_FIELD, DEFAULT_DRUG_SOURCE),
                        getDefault(dbObject, DRUG_STUDY_TYPE_FIELD, ""),
                        getDefault(dbObject, DRUG_TYPE_FIELD, DEFAULT_DRUG_TYPE),
                        null, null, null));
            }
        }
        va.setGeneDrugInteraction(drugs);

        //XREfs
        Object xrs = object.get(XREFS_FIELD);
        if (xrs != null && xrs instanceof List) {
            List<Xref> xrefs = new LinkedList<>();
            for (Object o : (List) xrs) {
                if (o instanceof Document) {
                    Document xref = (Document) o;
                    String id = xref.getString(XREF_ID_FIELD);
                    String source = xref.getString(XREF_SOURCE_FIELD);
                    if (source.equals(DB_SNP)) {
                        va.setId(id);
                    }
                    xrefs.add(new Xref(id, source));
                }
            }
            va.setXrefs(xrefs);
        }

        //Functional score
        List<Score> functionalScore = new LinkedList<>();
        if (object.containsKey(FUNCTIONAL_SCORE)) {
            List<Document> scores = object.get(FUNCTIONAL_SCORE, List.class);
            for (Document document : scores) {
                functionalScore.add(buildScore(document));
            }
        }
        addScore(object, functionalScore, CADD_SCALED, FUNCTIONAL_CADD_SCALED_FIELD);
        addScore(object, functionalScore, CADD_RAW, FUNCTIONAL_CADD_RAW_FIELD);
        va.setFunctionalScore(functionalScore);

        //Clinical Data
        if (object.containsKey(CLINICAL_DATA_FIELD)) {
            va.setTraitAssociation(parseClinicalData(object.get(CLINICAL_DATA_FIELD)));
        }

        if (customAnnotation != null) {
            Map<String, AdditionalAttribute> additionalAttributes = convertAdditionalAttributesToDataModelType(customAnnotation);
            va.setAdditionalAttributes(additionalAttributes);
        } else {
            if (!annotationIds.isEmpty()) {
                Object o = object.get(ANNOT_ID_FIELD);
                if (o != null && o instanceof Number) {
                    if (va.getAdditionalAttributes() == null) {
                        va.setAdditionalAttributes(new HashMap<>());
                    }
                    va.getAdditionalAttributes().compute(VariantField.AdditionalAttributes.GROUP_NAME.key(), (key, value) -> {
                        if (value == null) {
                            HashMap<String, String> map = new HashMap<>(1);
                            value = new AdditionalAttribute(map);
                        }
                        value.getAttribute().put(VariantField.AdditionalAttributes.ANNOTATION_ID.key(),
                                annotationIds.get(((Number) o).intValue()));
                        return value;
                    });
                }
            }
        }

        List<Document> repeats = getList(object, REPEATS_FIELD);
        if (repeats != null && !repeats.isEmpty()) {
            va.setRepeat(new ArrayList<>(repeats.size()));
            for (Document repeat : repeats) {
                va.getRepeat().add(new Repeat(
                        repeat.getString(REPEATS_ID_FIELD),
                        repeat.getString(REPEATS_CHROMOSOME_FIELD),
                        repeat.getInteger(REPEATS_START_FIELD),
                        repeat.getInteger(REPEATS_END_FIELD),
                        repeat.getInteger(REPEATS_PERIOD_FIELD),
                        repeat.getInteger(REPEATS_CONSENSUS_SIZE_FIELD),
                        getDefault(repeat, REPEATS_COPY_NUMBER_FIELD, (Float) null),
                        getDefault(repeat, REPEATS_PERCENTAGE_MATCH_FIELD, (Float) null),
                        getDefault(repeat, REPEATS_SCORE_FIELD, (Float) null),
                        repeat.getString(REPEATS_SEQUENCE_FIELD),
                        repeat.getString(REPEATS_SOURCE_FIELD)
                ));
            }
        }

        return va;
    }

    protected static VariantAnnotation newVariantAnnotation(Variant variant) {
        VariantAnnotation va;
        va = new VariantAnnotation();
        va.setChromosome(variant.getChromosome());
        va.setReference(variant.getReference());
        va.setAlternate(variant.getAlternate());
        va.setStart(variant.getStart());
        va.setEnd(variant.getEnd());
        return va;
    }

    public void addScore(Document object, List<Score> functionalScore, String source, String key) {
        if (object.containsKey(key)) {
            Document document = (Document) object.get(key);
            functionalScore.add(buildScore(source, document));
        }
    }

    private Score buildScore(Document document) {
        return buildScore("", document);
    }

    private Score buildScore(String source, Document document) {
        return new Score(
                getDefault(document, SCORE_SCORE_FIELD, 0.0),
                getDefault(document, SCORE_SOURCE_FIELD, source),
                getDefault(document, SCORE_DESCRIPTION_FIELD, (String) null)
        );
    }

    private ConsequenceType buildConsequenceType(String geneName, String ensemblGeneId, String ensemblTranscriptId, String strand,
                                                 String biotype, List<Document> exonOverlap, List<String> transcriptAnnotationFlags,
                                                 Integer cDnaPosition, Integer cdsPosition, String codon,
                                                 List<String> soNameList, ProteinVariantAnnotation proteinVariantAnnotation) {
        List<SequenceOntologyTerm> soTerms = new ArrayList<>(soNameList.size());
        for (String soName : soNameList) {
            soTerms.add(new SequenceOntologyTerm(ConsequenceTypeMappings.getSoAccessionString(soName), soName));
        }
        List<ExonOverlap> exonOverlapList = new ArrayList<>(exonOverlap.size());
        for (Document document : exonOverlap) {
            ExonOverlap e = new ExonOverlap(
                    document.getString(CT_EXON_OVERLAP_NUMBER_FIELD),
                    getDefault(document, CT_EXON_OVERLAP_PERCENTAGE_FIELD, 0F));
            exonOverlapList.add(e);
        }

        return new ConsequenceType(geneName, ensemblGeneId, ensemblTranscriptId, ensemblGeneId, ensemblTranscriptId, strand, biotype, null,
                exonOverlapList, transcriptAnnotationFlags, transcriptAnnotationFlags, cDnaPosition, cdsPosition, codon,
                proteinVariantAnnotation, soTerms);
    }

    private ProteinVariantAnnotation buildProteinVariantAnnotation(String uniprotAccession, String uniprotName, int aaPosition,
                                                                   String aaReference, String aaAlternate, String uniprotVariantId,
                                                                   String functionalDescription, List<Score> proteinSubstitutionScores,
                                                                   List<String> keywords, List<ProteinFeature> features) {
        return buildProteinVariantAnnotation(uniprotAccession, uniprotName, uniprotAccession, aaPosition, aaReference, aaAlternate, uniprotVariantId, functionalDescription, proteinSubstitutionScores, keywords, features);
    }

    private ProteinVariantAnnotation buildProteinVariantAnnotation(String uniprotAccession, String uniprotName, String proteinId, int aaPosition,
                                                                   String aaReference, String aaAlternate, String uniprotVariantId,
                                                                   String functionalDescription, List<Score> proteinSubstitutionScores,
                                                                   List<String> keywords, List<ProteinFeature> features) {
        if (areAllEmpty(uniprotAccession, uniprotName, proteinId, aaPosition, aaReference, aaAlternate,
                uniprotVariantId, proteinSubstitutionScores, keywords, features, functionalDescription)) {
            return null;
        } else {
            return new ProteinVariantAnnotation(uniprotAccession, uniprotName, proteinId, aaPosition,
                    aaReference, aaAlternate, uniprotVariantId, functionalDescription, proteinSubstitutionScores, keywords, features);
        }
    }

    private List<EvidenceEntry> parseClinicalData(Object clinicalData) {
        if (clinicalData instanceof List) {
            List documents = (List) clinicalData;
            List<EvidenceEntry> evidenceEntries = new ArrayList<>(documents.size());
            for (Object object : documents) {
                try {
                    EvidenceEntry evidenceEntry = jsonObjectMapper.convertValue(object, EvidenceEntry.class);
//                    for (int i = 0; i < evidenceEntry.getSchema().getFields().size(); i++) {
//                        if (evidenceEntry.get(i) == null) {
//                            evidenceEntry.put(i, "");
//                        }
//                    }
                    evidenceEntries.add(evidenceEntry);
                } catch (Exception e) {
                    logger.warn("Error parsing evidence entry: " + e.getMessage());
                    logger.debug("Error parsing evidence entry", e);
                }
            }
            if (!evidenceEntries.isEmpty()) {
                return evidenceEntries;
            }
        }
        return null;
    }

    public Map<String, AdditionalAttribute> convertAdditionalAttributesToDataModelType(Document customAnnotation) {
        Map<String, AdditionalAttribute> attributeMap = new HashMap<>();
        for (String key : customAnnotation.keySet()) {
            Document document = customAnnotation.get(key, Document.class);
            HashMap<String, String> map = new HashMap<>();
            document.forEach((k, value) -> map.put(k, value.toString()));
            AdditionalAttribute attribute = new AdditionalAttribute(map);
            attributeMap.put(key, attribute);
        }
        return attributeMap;
    }

    @Override
    public Document convertToStorageType(VariantAnnotation variantAnnotation) {
        Document document = new Document();
        Set<Document> xrefs = new HashSet<>();
        List<Document> cts = new LinkedList<>();

        //Annotation ID
        document.put(ANNOT_ID_FIELD, annotationId);

        //Variant ID
        if (variantAnnotation.getId() != null && !variantAnnotation.getId().isEmpty()) {
            xrefs.add(convertXrefToStorage(variantAnnotation.getId(), DB_SNP));
        }

        //ConsequenceType
        if (variantAnnotation.getConsequenceTypes() != null) {
            Set<String> gnSo = new HashSet<>();
            List<ConsequenceType> consequenceTypes = variantAnnotation.getConsequenceTypes();
            for (ConsequenceType consequenceType : consequenceTypes) {
                Document ct = new Document();

                putNotNull(ct, CT_GENE_NAME_FIELD, consequenceType.getGeneName());
                putNotNull(ct, CT_ENSEMBL_GENE_ID_FIELD, consequenceType.getEnsemblGeneId());
                putNotNull(ct, CT_ENSEMBL_TRANSCRIPT_ID_FIELD, consequenceType.getEnsemblTranscriptId());
//                putNotNull(ct, RELATIVE_POS_FIELD, consequenceType.getRelativePosition());
                putNotNull(ct, CT_CODON_FIELD, consequenceType.getCodon());
                putNotDefault(ct, CT_STRAND_FIELD, consequenceType.getStrand(), DEFAULT_STRAND_VALUE);
                putNotNull(ct, CT_BIOTYPE_FIELD, consequenceType.getBiotype());
                if (consequenceType.getExonOverlap() != null && !consequenceType.getExonOverlap().isEmpty()) {
                    List<Document> exonOverlapDocuments = new ArrayList<>(consequenceType.getExonOverlap().size());
                    for (ExonOverlap exonOverlap : consequenceType.getExonOverlap()) {
                        exonOverlapDocuments.add(new Document(CT_EXON_OVERLAP_NUMBER_FIELD, exonOverlap.getNumber())
                                .append(CT_EXON_OVERLAP_PERCENTAGE_FIELD, exonOverlap.getPercentage()));
                    }
                    ct.put(CT_EXON_OVERLAP_FIELD, exonOverlapDocuments);
                }
                putNotNull(ct, CT_TRANSCRIPT_ANNOT_FLAGS, consequenceType.getTranscriptAnnotationFlags());
                putNotNull(ct, CT_C_DNA_POSITION_FIELD, consequenceType.getCdnaPosition());
                putNotNull(ct, CT_CDS_POSITION_FIELD, consequenceType.getCdsPosition());

                ProteinVariantAnnotation proteinVariantAnnotation = consequenceType.getProteinVariantAnnotation();
                if (consequenceType.getSequenceOntologyTerms() != null) {
                    List<Integer> soAccession = new LinkedList<>();
                    for (SequenceOntologyTerm entry : consequenceType.getSequenceOntologyTerms()) {
                        soAccession.add(ConsequenceTypeMappings.termToAccession.get(entry.getName()));
                    }
                    putNotNull(ct, CT_SO_ACCESSION_FIELD, soAccession);

                    for (Integer so : soAccession) {
                        if (StringUtils.isNotEmpty(consequenceType.getGeneName())) {
                            gnSo.add(buildGeneSO(consequenceType.getGeneName(), so));
                        }
                        if (StringUtils.isNotEmpty(consequenceType.getEnsemblGeneId())) {
                            gnSo.add(buildGeneSO(consequenceType.getEnsemblGeneId(), so));
                        }
                        if (StringUtils.isNotEmpty(consequenceType.getEnsemblTranscriptId())) {
                            gnSo.add(buildGeneSO(consequenceType.getEnsemblTranscriptId(), so));
                        }
                        if (proteinVariantAnnotation != null) {
                            if (StringUtils.isNotEmpty(proteinVariantAnnotation.getUniprotAccession())) {
                                gnSo.add(buildGeneSO(proteinVariantAnnotation.getUniprotAccession(), so));
                            }
                            if (StringUtils.isNotEmpty(proteinVariantAnnotation.getUniprotName())) {
                                gnSo.add(buildGeneSO(proteinVariantAnnotation.getUniprotName(), so));
                            }
                        }
                    }
                }
                //Protein annotation
                if (proteinVariantAnnotation != null) {
                    putNotNull(ct, CT_AA_POSITION_FIELD, proteinVariantAnnotation.getPosition());
                    putNotNull(ct, CT_AA_REFERENCE_FIELD, proteinVariantAnnotation.getReference());
                    putNotNull(ct, CT_AA_ALTERNATE_FIELD, proteinVariantAnnotation.getAlternate());
                    putNotNull(ct, CT_PROTEIN_UNIPROT_ACCESSION, proteinVariantAnnotation.getUniprotAccession());
                    putNotNull(ct, CT_PROTEIN_UNIPROT_NAME, proteinVariantAnnotation.getUniprotName());
                    putNotNull(ct, CT_PROTEIN_ID, proteinVariantAnnotation.getProteinId());
                    putNotNull(ct, CT_PROTEIN_UNIPROT_VARIANT_ID, proteinVariantAnnotation.getUniprotVariantId());
                    putNotNull(ct, CT_PROTEIN_FUNCTIONAL_DESCRIPTION, proteinVariantAnnotation.getFunctionalDescription());
                    //Protein substitution region score
                    if (proteinVariantAnnotation.getSubstitutionScores() != null) {
                        List<Document> proteinSubstitutionScores = new LinkedList<>();
                        for (Score score : proteinVariantAnnotation.getSubstitutionScores()) {
                            if (score != null) {
                                if (score.getSource().equals(POLYPHEN)) {
                                    putNotNull(ct, CT_PROTEIN_POLYPHEN_FIELD, convertScoreToStorageNoSource(score));
                                } else if (score.getSource().equals(SIFT)) {
                                    putNotNull(ct, CT_PROTEIN_SIFT_FIELD, convertScoreToStorageNoSource(score));
                                } else {
                                    proteinSubstitutionScores.add(convertScoreToStorage(score));
                                }
                            }
                        }
                        putNotNull(ct, CT_PROTEIN_SUBSTITUTION_SCORE_FIELD, proteinSubstitutionScores);
                    }
                    putNotNull(ct, CT_PROTEIN_KEYWORDS, proteinVariantAnnotation.getKeywords());

                    List<ProteinFeature> features = proteinVariantAnnotation.getFeatures();
                    if (features != null) {
                        List<Document> documentFeatures = new ArrayList<>(features.size());
                        for (ProteinFeature feature : features) {
                            Document documentFeature = new Document();
                            putNotNull(documentFeature, CT_PROTEIN_FEATURE_ID_FIELD, feature.getId());
                            putNotNull(documentFeature, CT_PROTEIN_FEATURE_START_FIELD, feature.getStart());
                            putNotNull(documentFeature, CT_PROTEIN_FEATURE_END_FIELD, feature.getEnd());
                            putNotNull(documentFeature, CT_PROTEIN_FEATURE_TYPE_FIELD, feature.getType());
                            putNotNull(documentFeature, CT_PROTEIN_FEATURE_DESCRIPTION_FIELD, feature.getDescription());
                            documentFeatures.add(documentFeature);
                        }
                        putNotNull(ct, CT_PROTEIN_FEATURE_FIELD, documentFeatures);
                    }

                    if (StringUtils.isNotEmpty(proteinVariantAnnotation.getUniprotAccession())) {
                        xrefs.add(convertXrefToStorage(proteinVariantAnnotation.getUniprotAccession(), "UniProt"));
                    }
                    if (StringUtils.isNotEmpty(proteinVariantAnnotation.getUniprotName())) {
                        xrefs.add(convertXrefToStorage(proteinVariantAnnotation.getUniprotName(), "UniProt"));
                    }
                    if (StringUtils.isNotEmpty(proteinVariantAnnotation.getUniprotVariantId())) {
                        xrefs.add(convertXrefToStorage(proteinVariantAnnotation.getUniprotVariantId(), "UniProt"));
                    }
                }

                cts.add(ct);

                if (StringUtils.isNotEmpty(consequenceType.getGeneName())) {
                    xrefs.add(convertXrefToStorage(consequenceType.getGeneName(), "HGNC"));
                }
                if (StringUtils.isNotEmpty(consequenceType.getEnsemblGeneId())) {
                    xrefs.add(convertXrefToStorage(consequenceType.getEnsemblGeneId(), "ensemblGene"));
                }
                if (StringUtils.isNotEmpty(consequenceType.getEnsemblTranscriptId())) {
                    xrefs.add(convertXrefToStorage(consequenceType.getEnsemblTranscriptId(), "ensemblTranscript"));
                }

            }
            putNotNull(document, GENE_SO_FIELD, gnSo);
            putNotNull(document, CONSEQUENCE_TYPE_FIELD, cts);
        }

        if (variantAnnotation.getDisplayConsequenceType() != null) {
            Integer accession = ConsequenceTypeMappings.termToAccession.get(variantAnnotation.getDisplayConsequenceType());
            document.put(DISPLAY_CONSEQUENCE_TYPE_FIELD, accession);
        }

        if (variantAnnotation.getCytoband() != null && !variantAnnotation.getCytoband().isEmpty()) {
            List<Document> cytobands = new ArrayList<>(variantAnnotation.getCytoband().size());
            for (Cytoband cytoband : variantAnnotation.getCytoband()) {
                Document d = new Document();
                putNotNull(d, CYTOBAND_STAIN_FIELD, cytoband.getStain());
                putNotNull(d, CYTOBAND_NAME_FIELD, cytoband.getName());
                putNotNull(d, CYTOBAND_START_FIELD, cytoband.getStart());
                putNotNull(d, CYTOBAND_END_FIELD, cytoband.getEnd());
                cytobands.add(d);
            }
            document.put(CYTOBANDS_FIELD, cytobands);
        }

        putNotNull(document, HGVS_FIELD, variantAnnotation.getHgvs());

        //Conserved region score
        if (variantAnnotation.getConservation() != null) {
            List<Document> conservedRegionScores = new LinkedList<>();
            for (Score score : variantAnnotation.getConservation()) {
                if (score != null) {
                    if (score.getSource().equals(PHYLOP)) {
                        putNotNull(document, CONSERVED_REGION_PHYLOP_FIELD, convertScoreToStorageNoSource(score));
                    } else if (score.getSource().equals(PHAST_CONS)) {
                        putNotNull(document, CONSERVED_REGION_PHASTCONS_FIELD, convertScoreToStorageNoSource(score));
                    } else if (score.getSource().equals(GERP)) {
                        putNotNull(document, CONSERVED_REGION_GERP_FIELD, convertScoreToStorageNoSource(score));
                    } else {
                        conservedRegionScores.add(convertScoreToStorage(score));
                    }
                }
            }
            putNotNull(document, CONSERVED_REGION_SCORE_FIELD, conservedRegionScores);
        }

        // Gene trait association
        if (variantAnnotation.getGeneTraitAssociation() != null) {
            List<Document> geneTraitAssociations = new LinkedList<>();
            for (GeneTraitAssociation geneTraitAssociation : variantAnnotation.getGeneTraitAssociation()) {
                if (geneTraitAssociation != null) {
                    Document d = new Document();
                    putNotNull(d, GENE_TRAIT_ID_FIELD, geneTraitAssociation.getId());
                    putNotNull(d, GENE_TRAIT_NAME_FIELD, geneTraitAssociation.getName());
                    putNotNull(d, GENE_TRAIT_SCORE_FIELD, geneTraitAssociation.getScore());
                    putNotNull(d, GENE_TRAIT_HPO_FIELD, geneTraitAssociation.getHpo());
                    if (StringUtils.isNotEmpty(geneTraitAssociation.getHpo())) {
                        xrefs.add(convertXrefToStorage(geneTraitAssociation.getHpo(), "hpo"));
                    }
                    putNotNull(d, GENE_TRAIT_PUBMEDS_FIELD, geneTraitAssociation.getNumberOfPubmeds());
                    putNotNull(d, GENE_TRAIT_TYPES_FIELD, geneTraitAssociation.getAssociationTypes());
                    putNotNull(d, GENE_TRAIT_SOURCES_FIELD, geneTraitAssociation.getSources());
                    putNotNull(d, GENE_TRAIT_SOURCE_FIELD, geneTraitAssociation.getSource());

//                    if (StringUtils.isNotEmpty(geneTraitAssociation.getHpo())) {
//                        xrefs.add(convertXrefToStorage(geneTraitAssociation.getId(), geneTraitAssociation.getSource()));
//                    }

                    geneTraitAssociations.add(d);
                }
            }
            putNotNull(document, GENE_TRAIT_FIELD, geneTraitAssociations);
        }

        //Population frequencies
        if (variantAnnotation.getPopulationFrequencies() != null) {
            List<Document> populationFrequencies = new LinkedList<>();
            for (PopulationFrequency populationFrequency : variantAnnotation.getPopulationFrequencies()) {
                if (populationFrequency != null) {
                    populationFrequencies.add(convertPopulationFrequencyToStorage(populationFrequency));
                }
            }
            putNotNull(document, POPULATION_FREQUENCIES_FIELD, populationFrequencies);
        }

        // Drug-Gene Interactions
        if (variantAnnotation.getGeneDrugInteraction() != null) {
            List<Document> drugGeneInteractions = new LinkedList<>();
            List<GeneDrugInteraction> geneDrugInteractionList = variantAnnotation.getGeneDrugInteraction();
            if (geneDrugInteractionList != null) {
                for (GeneDrugInteraction geneDrugInteraction : geneDrugInteractionList) {
                    Document drugDbObject = new Document(DRUG_GENE_FIELD, geneDrugInteraction.getGeneName());
                    putNotNull(drugDbObject, DRUG_NAME_FIELD, geneDrugInteraction.getDrugName());
                    putNotDefault(drugDbObject, DRUG_SOURCE_FIELD, geneDrugInteraction.getSource(), DEFAULT_DRUG_SOURCE);
                    putNotNull(drugDbObject, DRUG_STUDY_TYPE_FIELD, geneDrugInteraction.getStudyType());
                    putNotDefault(drugDbObject, DRUG_TYPE_FIELD, geneDrugInteraction.getType(), DEFAULT_DRUG_TYPE);
                    drugGeneInteractions.add(drugDbObject);
                }
            }
            putNotNull(document, DRUG_FIELD, drugGeneInteractions);
        }

        //XREFs
        if (variantAnnotation.getXrefs() != null) {
            for (Xref xref : variantAnnotation.getXrefs()) {
                xrefs.add(convertXrefToStorage(xref.getId(), xref.getSource()));
            }
        }
        putNotNull(document, XREFS_FIELD, xrefs);

        //Functional score
        if (variantAnnotation.getFunctionalScore() != null) {
            List<Document> scores = new ArrayList<>(variantAnnotation.getFunctionalScore().size());
            for (Score score : variantAnnotation.getFunctionalScore()) {
                if (score != null) {
                    if (score.getSource().equals(CADD_RAW)) {
                        putNotNull(document, FUNCTIONAL_CADD_RAW_FIELD, convertScoreToStorageNoSource(score));
                    } else if (score.getSource().equals(CADD_SCALED)) {
                        putNotNull(document, FUNCTIONAL_CADD_SCALED_FIELD, convertScoreToStorageNoSource(score));
                    } else {
                        scores.add(convertScoreToStorage(score));
                    }
                }
            }
            putNotNull(document, FUNCTIONAL_SCORE, scores);
        }

        //Clinical Data
        if (CollectionUtils.isNotEmpty(variantAnnotation.getTraitAssociation())) {
            document.put(CLINICAL_DATA_FIELD, generateClinicalDBList(variantAnnotation.getTraitAssociation()));

            if (variantAnnotation.getTraitAssociation() != null) {
                variantAnnotation.getTraitAssociation()
                        .stream()
                        .filter(e -> StringUtils.isNotEmpty(e.getId()))
                        .forEach(e -> xrefs.add(convertXrefToStorage(e.getId(), e.getSource() != null ? e.getSource().getName() : null)));
            }
        }

        if (variantAnnotation.getRepeat() != null && !variantAnnotation.getRepeat().isEmpty()) {
            List<Document> repeats = new ArrayList<>(variantAnnotation.getRepeat().size());
            for (Repeat repeat : variantAnnotation.getRepeat()) {
                Document repeatDocument = new Document();
                putNotNull(repeatDocument, REPEATS_CHROMOSOME_FIELD, repeat.getChromosome());
                putNotNull(repeatDocument, REPEATS_START_FIELD, repeat.getStart());
                putNotNull(repeatDocument, REPEATS_END_FIELD, repeat.getEnd());
                putNotNull(repeatDocument, REPEATS_CONSENSUS_SIZE_FIELD, repeat.getConsensusSize());
                putNotNull(repeatDocument, REPEATS_COPY_NUMBER_FIELD, repeat.getCopyNumber());
                putNotNull(repeatDocument, REPEATS_PERCENTAGE_MATCH_FIELD, repeat.getPercentageMatch());
                putNotNull(repeatDocument, REPEATS_ID_FIELD, repeat.getId());
                putNotNull(repeatDocument, REPEATS_PERIOD_FIELD, repeat.getPeriod());
                putNotNull(repeatDocument, REPEATS_SCORE_FIELD, repeat.getScore());
                putNotNull(repeatDocument, REPEATS_SEQUENCE_FIELD, repeat.getSequence());
                putNotNull(repeatDocument, REPEATS_SOURCE_FIELD, repeat.getSource());
                repeats.add(repeatDocument);
            }
            document.put(REPEATS_FIELD, repeats);
        }

        return document;
    }

    public static String buildGeneSO(String gene, Integer so) {
        return gene == null ? null : gene + '_' + so;
    }


    private <T> List<Document> generateClinicalDBList(List<T> objectList) {
        if (objectList != null) {
            List<Document> list = new ArrayList<>(objectList.size());
            for (T object : objectList) {
                try {
                    if (object instanceof GenericRecord) {
                        list.add(Document.parse(object.toString()));
                    } else {
                        list.add(Document.parse(writer.writeValueAsString(object)));
                    }
                } catch (JsonProcessingException e) {
                    e.printStackTrace();
                    logger.error("Error serializing Clinical Data " + object.getClass(), e);
                }
            }
            return list;
        }
        return null;
    }

    public Document convertScoreToStorageNoSource(Score score) {
        return convertScoreToStorage(score.getScore(), null, score.getDescription());
    }

    private Document convertScoreToStorage(Score score) {
        return convertScoreToStorage(score.getScore(), score.getSource(), score.getDescription());
    }

    private Document convertScoreToStorage(double score, String source, String description) {
        Document dbObject = new Document(SCORE_SCORE_FIELD, score);
        putNotNull(dbObject, SCORE_SOURCE_FIELD, source);
        putNotNull(dbObject, SCORE_DESCRIPTION_FIELD, description);
        return dbObject;
    }

    private Document convertPopulationFrequencyToStorage(PopulationFrequency populationFrequency) {
        Document dbObject = new Document(POPULATION_FREQUENCY_STUDY_FIELD, populationFrequency.getStudy());
        putNotNull(dbObject, POPULATION_FREQUENCY_POP_FIELD, populationFrequency.getPopulation());
        putNotNull(dbObject, POPULATION_FREQUENCY_REFERENCE_FREQUENCY_FIELD, populationFrequency.getRefAlleleFreq());
        putNotNull(dbObject, POPULATION_FREQUENCY_ALTERNATE_FREQUENCY_FIELD, populationFrequency.getAltAlleleFreq());
        putNotNull(dbObject, POPULATION_FREQUENCY_REF_HOM_GT_FIELD, populationFrequency.getRefHomGenotypeFreq());
        putNotNull(dbObject, POPULATION_FREQUENCY_HET_GT_FIELD, populationFrequency.getHetGenotypeFreq());
        putNotNull(dbObject, POPULATION_FREQUENCY_ALT_HOM_GT_FIELD, populationFrequency.getAltHomGenotypeFreq());
        return dbObject;
    }

    private Document convertXrefToStorage(String id, String source) {
        Document dbObject = new Document(XREF_ID_FIELD, id);
        dbObject.put(XREF_SOURCE_FIELD, source);
        return dbObject;
    }

    public Document convertToStorageType(Map<String, AdditionalAttribute> attributes) {
        Document document = new Document();
        attributes.forEach((key, attribute) -> {
            document.put(key, convertToStorageType(attribute));
        });
        return document;
    }

    public static Document convertToStorageType(AdditionalAttribute attribute) {
        Document document = new Document();
        document.putAll(attribute.getAttribute());
        return document;
    }

}
