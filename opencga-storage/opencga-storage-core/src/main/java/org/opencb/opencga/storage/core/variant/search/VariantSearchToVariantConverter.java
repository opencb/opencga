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

package org.opencb.opencga.storage.core.variant.search;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectReader;
import com.fasterxml.jackson.databind.ObjectWriter;
import htsjdk.variant.vcf.VCFConstants;
import org.apache.commons.collections.MapUtils;
import org.apache.commons.lang3.StringUtils;
import org.opencb.biodata.models.variant.StudyEntry;
import org.opencb.biodata.models.variant.Variant;
import org.opencb.biodata.models.variant.annotation.ConsequenceTypeMappings;
import org.opencb.biodata.models.variant.avro.*;
import org.opencb.biodata.models.variant.stats.VariantStats;
import org.opencb.commons.datastore.core.ComplexTypeConverter;
import org.opencb.commons.utils.CollectionUtils;
import org.opencb.commons.utils.ListUtils;
import org.opencb.opencga.core.common.ArrayUtils;
import org.opencb.opencga.storage.core.variant.adaptors.VariantField;
import org.opencb.opencga.storage.core.variant.annotation.converters.VariantTraitAssociationToEvidenceEntryConverter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.*;

import static org.opencb.opencga.storage.core.variant.adaptors.VariantField.AdditionalAttributes.GROUP_NAME;
import static org.opencb.opencga.storage.core.variant.adaptors.VariantField.AdditionalAttributes.RELEASE;

/**
 * Created by imedina on 14/11/16.
 */
public class VariantSearchToVariantConverter implements ComplexTypeConverter<Variant, VariantSearchModel> {

    public static final double MISSING_VALUE = -100.0;
    private static final String LIST_SEP = "___";
    private static final String FIELD_SEP = " -- ";

    private Logger logger = LoggerFactory.getLogger(VariantSearchToVariantConverter.class);
    private final VariantTraitAssociationToEvidenceEntryConverter evidenceEntryConverter;
    private Set<VariantField> includeFields;

    public VariantSearchToVariantConverter() {
        evidenceEntryConverter = new VariantTraitAssociationToEvidenceEntryConverter();
    }

    public VariantSearchToVariantConverter(Set<VariantField> includeFields) {
        this();
        this.includeFields = includeFields;
    }

    /**
     * Conversion: from storage type to data model.
     *
     * @param variantSearchModel Storage type object
     * @return Data model object
     */
    @Override
    public Variant convertToDataModelType(VariantSearchModel variantSearchModel) {
        // set chromosome, start, end, ref, alt from ID
        Variant variant = new Variant(variantSearchModel.getId());

        // set ID, chromosome, start, end, ref, alt, type
        variant.setId(variantSearchModel.getVariantId());

        // set variant type
        if (StringUtils.isNotEmpty(variantSearchModel.getType())) {
            variant.setType(VariantType.valueOf(variantSearchModel.getType()));
        }

        // Study management
        Map<String, StudyEntry> studyEntryMap = new HashMap<>();
        if (variantSearchModel.getStudies() != null && CollectionUtils.isNotEmpty(variantSearchModel.getStudies())) {
            List<StudyEntry> studies = new ArrayList<>();
            variantSearchModel.getStudies().forEach(studyId -> {
                StudyEntry entry = new StudyEntry(studyId);
                studies.add(entry);
                studyEntryMap.put(studyId, entry);
            });
            variant.setStudies(studies);
        }

        // Genotypes and sample data and format
        if (MapUtils.isNotEmpty(variantSearchModel.getSampleFormat())) {
            for (String studyId: studyEntryMap.keySet()) {
                String stringToList;
                String suffix = VariantSearchUtils.FIELD_SEPARATOR + studyId + VariantSearchUtils.FIELD_SEPARATOR;
                StudyEntry studyEntry = studyEntryMap.get(studyId);

                // Format
                stringToList = variantSearchModel.getSampleFormat().get("sampleFormat" + suffix + "format");
                if (StringUtils.isNotEmpty(stringToList)) {
                    studyEntry.setFormat(Arrays.asList(stringToList.split(LIST_SEP)));
                }

                // Sample Data management
                stringToList = variantSearchModel.getSampleFormat().get("sampleFormat" + suffix + "sampleName");
                if (StringUtils.isNotEmpty(stringToList)) {
                    String[] sampleNames = stringToList.split(LIST_SEP);
                    List<List<String>> sampleData = new ArrayList<>();
                    Map<String, Integer> samplePosition = new HashMap<>();
                    int pos = 0;
                    for (String sampleName: sampleNames) {
                        suffix = VariantSearchUtils.FIELD_SEPARATOR + studyId + VariantSearchUtils.FIELD_SEPARATOR + sampleName;
                        stringToList = variantSearchModel.getSampleFormat().get("sampleFormat" + suffix);
                        if (StringUtils.isNotEmpty(stringToList)) {
                            sampleData.add(Arrays.asList(stringToList.split(LIST_SEP)));
                            samplePosition.put(sampleName, pos++);
                        }
//                        else {
//                            logger.error("Error converting samplesFormat, sample '{}' is missing or empty, value: '{}'",
//                                    sampleName, stringToList);
//                        }
                    }

                    if (ListUtils.isNotEmpty(sampleData)) {
                        studyEntry.setSamplesData(sampleData);
                        studyEntry.setSamplesPosition(samplePosition);
                    }
                }
            }
        }

        // File info management
        if (MapUtils.isNotEmpty(variantSearchModel.getFileInfo())) {
            ObjectReader reader = new ObjectMapper().reader(HashMap.class);
            for (String key: variantSearchModel.getFileInfo().keySet()) {
                // key consists of 'fileInfo' + "__" + studyId + "__" + fileId
                String[] fields = key.split(VariantSearchUtils.FIELD_SEPARATOR);
                FileEntry fileEntry = new FileEntry(fields[2], null, new HashMap<>());
                try {
                    // We obtain the original call
                    Map<String, String> fileInfoAttributes = reader.readValue(variantSearchModel.getFileInfo().get(key));
                    if (MapUtils.isNotEmpty(fileInfoAttributes)) {
                        fileEntry.setCall(fileInfoAttributes.get("fileCall"));
                        fileInfoAttributes.remove("fileCall");
                        fileEntry.setAttributes(fileInfoAttributes);
                    }
                } catch (IOException e) {
                    logger.error("Error converting fileInfo from variant search model: {}", e.getMessage());
                } finally {
                    variant.getStudy(fields[1]).getFiles().add(fileEntry);
                }
            }
        }

        // Stats management
        if (MapUtils.isNotEmpty(variantSearchModel.getStats())) {
            for (String key: variantSearchModel.getStats().keySet()) {
                // key consists of 'stats' + "__" + studyId + "__" + cohort
                String[] fields = key.split(VariantSearchUtils.FIELD_SEPARATOR);
                if (studyEntryMap.containsKey(fields[1])) {
                    VariantStats variantStats = new VariantStats();
                    variantStats.setRefAlleleFreq(1 - variantSearchModel.getStats().get(key));
                    variantStats.setAltAlleleFreq(variantSearchModel.getStats().get(key));
                    variantStats.setMaf(Math.min(variantSearchModel.getStats().get(key), 1 - variantSearchModel.getStats().get(key)));
                    studyEntryMap.get(fields[1]).setStats(fields[2], variantStats);
                }
            }
        }

        // process annotation
        variant.setAnnotation(getVariantAnnotation(variantSearchModel, variant));

        return variant;
    }

    public VariantAnnotation getVariantAnnotation(VariantSearchModel variantSearchModel, Variant variant) {

        if (includeFields != null && !includeFields.contains(VariantField.ANNOTATION)) {
            return null;
        }

        VariantAnnotation variantAnnotation = new VariantAnnotation();
        variantAnnotation.setChromosome(variant.getChromosome());
        variantAnnotation.setStart(variant.getStart());
        variantAnnotation.setEnd(variant.getEnd());
        variantAnnotation.setAlternate(variant.getAlternate());
        variantAnnotation.setReference(variant.getReference());

        // Set 'release' if it was not missing
        if (variantSearchModel.getRelease() > 0) {
            Map<String, String> attribute = new HashMap<>();
            attribute.put(RELEASE.key(), String.valueOf(variantSearchModel.getRelease()));
            variantAnnotation.setAdditionalAttributes(new HashMap<>());
            variantAnnotation.getAdditionalAttributes().put(GROUP_NAME.key(), new AdditionalAttribute(attribute));
        }

        // Xrefs
        List<Xref> xrefs = new ArrayList<>();
        if (ListUtils.isNotEmpty(variantSearchModel.getXrefs())) {
            for (String xref: variantSearchModel.getXrefs()) {
                if (xref == null) {
                    continue;
                }
                if (xref.startsWith("rs")) {
                    xrefs.add(new Xref(xref, "dbSNP"));
                    continue;
                }
                if (xref.startsWith("ENSG")) {
                    xrefs.add(new Xref(xref, "ensemblGene"));
                    continue;
                }
                if (xref.startsWith("ENST")) {
                    xrefs.add(new Xref(xref, "ensemblTranscript"));
                }
            }
        }
        variantAnnotation.setXrefs(xrefs);

        // Init the consequence type map with protein variant annotation
        // and set displayConsequenceType, hgvs, cytobands and repeats from 'other' field
        Map<String, ConsequenceType> consequenceTypeMap = new HashMap<>();
        variantAnnotation.setHgvs(new ArrayList<>());
        variantAnnotation.setCytoband(new ArrayList<>());
        variantAnnotation.setRepeat(new ArrayList<>());
        for (String other : variantSearchModel.getOther()) {
            // Sanity check
            if (StringUtils.isEmpty(other)) {
                continue;
            }
            String[] fields = other.split(FIELD_SEP);
            switch (fields[0]) {
                case "DCT":
                    variantAnnotation.setDisplayConsequenceType(fields[1]);
                    break;
                case "HGVS":
                    variantAnnotation.getHgvs().add(fields[1]);
                    break;
                case "CB":
                    Cytoband cytoband = Cytoband.newBuilder()
                            .setChromosome(variant.getChromosome())
                            .setName(fields[1])
                            .setStain(fields[2])
                            .setStart(Integer.parseInt(fields[3])).setEnd(Integer.parseInt(fields[4]))
                            .build();
                    variantAnnotation.getCytoband().add(cytoband);
                    break;
                case "RP":
                    Repeat repeat = Repeat.newBuilder()
                            .setId(fields[1])
                            .setSource(fields[2])
                            .setChromosome(variant.getChromosome())
                            .setStart(Integer.parseInt(fields[5]))
                            .setEnd(Integer.parseInt(fields[6]))
                            .setCopyNumber(Float.parseFloat(fields[3]))
                            .setPercentageMatch(Float.parseFloat(fields[4]))
                            .setPeriod(null)
                            .setConsensusSize(null)
                            .setScore(null)
                            .setSequence(null)
                            .build();
                    variantAnnotation.getRepeat().add(repeat);
                    break;
                case "TRANS":
                    // Create consequence type from transcript info:
                    //       1            2             3                 4               5           6
                    // transcriptId -- biotype -- annotationFlags -- cdnaPosition -- cdsPosition -- codon
                    //           7                   8                 9                 10           11
                    // -- uniprotAccession -- uniprotAccession -- uniprotVariantId -- position -- aaChange
                    //     12           13            14                15
                    // siftScore -- siftDescr -- poliphenScore -- poliphenDescr
                    ConsequenceType consequenceType = new ConsequenceType();
                    if (fields.length > 2) {
                        consequenceType.setEnsemblTranscriptId(fields[1]);
                        consequenceType.setBiotype(fields[2]);
                    }
                    if (fields.length > 3) {
                        if (fields[3].length() > 0) {
                            consequenceType.setTranscriptAnnotationFlags(Arrays.asList(fields[3].split(",")));
                        }
                    }
                    if (fields.length > 4) {
                        consequenceType.setCdnaPosition(Integer.parseInt(fields[4]));
                        consequenceType.setCdsPosition(Integer.parseInt(fields[5]));
                        if (fields.length > 6) {
                            // Sometimes, codon (i.e., split at 5) is null, check it!
                            consequenceType.setCodon(fields[6]);
                        }
                    }
                    if (fields.length > 7) {
                        // Create, init and add protein variant annotation to the consequence type
                        ProteinVariantAnnotation protVarAnnotation = new ProteinVariantAnnotation();
                        // Uniprot info
                        protVarAnnotation.setUniprotAccession(fields[7]);
                        protVarAnnotation.setUniprotName(fields[8]);
                        protVarAnnotation.setUniprotVariantId(fields[9]);
                        if (StringUtils.isNotEmpty(fields[10])) {
                            try {
                                protVarAnnotation.setPosition(Integer.parseInt(fields[10]));
                            } catch (NumberFormatException e) {
                                logger.warn("Parsing position: " + e.getMessage());
                            }
                        }
                        if (StringUtils.isNotEmpty(fields[11]) && fields[11].contains("/")) {
                            String[] refAlt = fields[11].split("/");
                            protVarAnnotation.setReference(refAlt[0]);
                            protVarAnnotation.setAlternate(refAlt[1]);
                        }
                        // Sift score
                        List<Score> scores = new ArrayList(2);
                        if (fields.length > 12
                                && (StringUtils.isNotEmpty(fields[12]) || StringUtils.isNotEmpty(fields[13]))) {
                            Score score = new Score();
                            score.setSource("sift");
                            if (StringUtils.isNotEmpty(fields[12])) {
                                try {
                                    score.setScore(Double.parseDouble(fields[12]));
                                } catch (NumberFormatException e) {
                                    logger.warn("Parsing Sift score: " + e.getMessage());
                                }
                            }
                            score.setDescription(fields[13]);
                            scores.add(score);
                        }
                        // Polyphen score
                        if (fields.length > 14
                                && (StringUtils.isNotEmpty(fields[14]) || StringUtils.isNotEmpty(fields[15]))) {
                            Score score = new Score();
                            score.setSource("polyphen");
                            if (StringUtils.isNotEmpty(fields[14])) {
                                try {
                                    score.setScore(Double.parseDouble(fields[14]));
                                } catch (NumberFormatException e) {
                                    logger.warn("Parsing Polyphen score: " + e.getMessage());
                                }
                            }
                            score.setDescription(fields[15]);
                            scores.add(score);
                        }
                        protVarAnnotation.setSubstitutionScores(scores);

                        // Finally, set protein variant annotation in consequence type
                        consequenceType.setProteinVariantAnnotation(protVarAnnotation);
                    }

                    // The key is the ENST id
                    consequenceTypeMap.put(fields[1], consequenceType);
                    break;
                default:
                    logger.warn("Unknown key in 'other' array in Solr: " + fields[0]);
            }
        }

        // consequence types
        String geneName = null;
        String ensGene = null;
        if (ListUtils.isNotEmpty(variantSearchModel.getGenes())) {
            for (String name : variantSearchModel.getGenes()) {
                if (!name.startsWith("ENS")) {
                    geneName = name;
                } else if (name.startsWith("ENSG")) {
                    ensGene = name;
                } else if (name.startsWith("ENST")) {
                    ConsequenceType consequenceType = consequenceTypeMap.getOrDefault(name, null);
                    if (consequenceType == null) {
                        consequenceType = new ConsequenceType();
                        consequenceType.setEnsemblTranscriptId(name);
                        consequenceTypeMap.put(name, consequenceType);
                        logger.warn("No information found in Solr field 'other' for transcript '{}'", name);
//                        throw new InternalError("Transcript '" + name + "' missing in schema field name 'other'");
                    }
                    consequenceType.setGeneName(geneName);
                    consequenceType.setEnsemblGeneId(ensGene);
                }
            }
        }

        // prepare protein substitution scores: sift and polyphen
        List<Score> scores;
//        ProteinVariantAnnotation proteinAnnotation = new ProteinVariantAnnotation();
//        if (!ArrayUtils.equals(variantSearchModel.getSift(), MISSING_VALUE)
//                || !ArrayUtils.equals(variantSearchModel.getPolyphen(), MISSING_VALUE)) {
//            scores = new ArrayList<>();
//            if (!ArrayUtils.equals(variantSearchModel.getSift(), MISSING_VALUE)) {
//                scores.add(new Score(variantSearchModel.getSift(), "sift", variantSearchModel.getSiftDesc()));
//            }
//            if (!ArrayUtils.equals(variantSearchModel.getPolyphen(), MISSING_VALUE)) {
//                scores.add(new Score(variantSearchModel.getPolyphen(), "polyphen", variantSearchModel.getPolyphenDesc()));
//            }
//            proteinAnnotation.setSubstitutionScores(scores);
//        }

        // and finally, update the SO acc. for each conseq. type and setProteinVariantAnnotation if SO accession is 1583
        Set<Integer> geneRelatedSoTerms = new HashSet<>();
        if (variantSearchModel.getGeneToSoAcc() != null) {
            for (String geneToSoAcc : variantSearchModel.getGeneToSoAcc()) {
                String[] fields = geneToSoAcc.split("_");
                if (consequenceTypeMap.containsKey(fields[0])) {
                    int soAcc = Integer.parseInt(fields[1]);
                    geneRelatedSoTerms.add(soAcc);  // we memorise the SO term for next block

                    SequenceOntologyTerm sequenceOntologyTerm = new SequenceOntologyTerm();
                    sequenceOntologyTerm.setAccession("SO:" + String.format("%07d", soAcc));
                    sequenceOntologyTerm.setName(ConsequenceTypeMappings.accessionToTerm.get(soAcc));
                    if (consequenceTypeMap.get(fields[0]).getSequenceOntologyTerms() == null) {
                        consequenceTypeMap.get(fields[0]).setSequenceOntologyTerms(new ArrayList<>());
                    }
                    consequenceTypeMap.get(fields[0]).getSequenceOntologyTerms().add(sequenceOntologyTerm);

//                    // only set protein for that conseq. type if annotated protein and SO acc is 1583 (missense_variant)
//                    if (soAcc == 1583) {
//                        consequenceTypeMap.get(fields[0]).setProteinVariantAnnotation(proteinAnnotation);
//                    }
                }
            }
        }

        // We convert the Map into an array
        ArrayList<ConsequenceType> consequenceTypes = new ArrayList<>(consequenceTypeMap.values());

        // Add non-gene related SO terms
        if (variantSearchModel.getSoAcc() != null) {
            for (Integer soAcc : variantSearchModel.getSoAcc()) {
                // let's process all non-gene related terms such as regulatory_region_variant or intergenic_variant
                if (!geneRelatedSoTerms.contains(soAcc)) {
                    SequenceOntologyTerm sequenceOntologyTerm = new SequenceOntologyTerm();
                    sequenceOntologyTerm.setAccession("SO:" + String.format("%07d", soAcc));
                    sequenceOntologyTerm.setName(ConsequenceTypeMappings.accessionToTerm.get(soAcc));

                    ConsequenceType consequenceType = new ConsequenceType();
                    consequenceType.setEnsemblGeneId("");
                    consequenceType.setGeneName("");
                    consequenceType.setEnsemblTranscriptId("");
                    consequenceType.setSequenceOntologyTerms(Collections.singletonList(sequenceOntologyTerm));
                    consequenceTypes.add(consequenceType);
                }
            }
        }

        // and update the variant annotation with the consequence types
        variantAnnotation.setConsequenceTypes(consequenceTypes);

        // set populations
        List<PopulationFrequency> populationFrequencies = new ArrayList<>();
        if (variantSearchModel.getPopFreq() != null && variantSearchModel.getPopFreq().size() > 0) {
            for (String key : variantSearchModel.getPopFreq().keySet()) {
                PopulationFrequency populationFrequency = new PopulationFrequency();
                String[] fields = key.split(VariantSearchUtils.FIELD_SEPARATOR);
                populationFrequency.setStudy(fields[1]);
                populationFrequency.setPopulation(fields[2]);
                populationFrequency.setRefAlleleFreq(1 - variantSearchModel.getPopFreq().get(key));
                populationFrequency.setAltAlleleFreq(variantSearchModel.getPopFreq().get(key));
                populationFrequencies.add(populationFrequency);
            }
        }
        variantAnnotation.setPopulationFrequencies(populationFrequencies);

        // Set conservations scores
        scores = new ArrayList<>();
        if (!ArrayUtils.equals(variantSearchModel.getPhylop(), MISSING_VALUE)) {
            scores.add(new Score(variantSearchModel.getPhylop(), "phylop", ""));
        }
        if (!ArrayUtils.equals(variantSearchModel.getPhastCons(), MISSING_VALUE)) {
            scores.add(new Score(variantSearchModel.getPhastCons(), "phastCons", ""));
        }
        if (!ArrayUtils.equals(variantSearchModel.getGerp(), MISSING_VALUE)) {
            scores.add(new Score(variantSearchModel.getGerp(), "gerp", ""));
        }
        variantAnnotation.setConservation(scores);

        // Set CADD scores
        scores = new ArrayList<>();
        if (!ArrayUtils.equals(variantSearchModel.getCaddRaw(), MISSING_VALUE)) {
            scores.add(new Score(variantSearchModel.getCaddRaw(), "cadd_raw", ""));
        }
        if (!ArrayUtils.equals(variantSearchModel.getCaddScaled(), MISSING_VALUE)) {
            scores.add(new Score(variantSearchModel.getCaddScaled(), "cadd_scaled", ""));
        }
        variantAnnotation.setFunctionalScore(scores);

        // set HPO, ClinVar and Cosmic
        if (variantSearchModel.getTraits() != null) {
            Map<String, ClinVar> clinVarMap = new HashMap<>();
            List<ClinVar> clinVarList = new ArrayList<>();
            List<Cosmic> cosmicList = new ArrayList<>();
            List<GeneTraitAssociation> geneTraitAssociationList = new ArrayList<>();

            for (String trait : variantSearchModel.getTraits()) {
                String[] fields = StringUtils.splitByWholeSeparatorPreserveAllTokens(trait, FIELD_SEP);
                switch (fields[0]) {
                    case "HP":
                        // Gene trait: HP -- hpo -- id -- name
                        GeneTraitAssociation geneTraitAssociation = new GeneTraitAssociation();
                        geneTraitAssociation.setHpo(fields[1]);
                        geneTraitAssociation.setId(fields[2]);
                        geneTraitAssociation.setName(fields[3]);
                        geneTraitAssociationList.add(geneTraitAssociation);
                        break;
                    case "CV":
                        // Variant trait: CV -- accession -- trait
                        if (!clinVarMap.containsKey(fields[1])) {
                            String clinicalSignificance = "";
                            if (fields.length > 3 && fields[3].length() > 3) {
                                clinicalSignificance = fields[3].substring(3);
                            }
                            ClinVar clinVar = new ClinVar(fields[1], clinicalSignificance, new ArrayList<>(), new ArrayList<>(), "");
                            clinVarMap.put(fields[1], clinVar);
                            clinVarList.add(clinVar);
                        }
                        clinVarMap.get(fields[1]).getTraits().add(fields[2]);
                        break;
                    case "CM":
                        // Variant trait: CM -- mutation id -- primary histology -- histology subtype
                        Cosmic cosmic = new Cosmic();
                        cosmic.setMutationId(fields[1]);
                        cosmic.setPrimaryHistology(fields[2]);
                        cosmic.setHistologySubtype(fields[3]);
                        cosmicList.add(cosmic);
                        break;
                    case "KW":
                    case "PD":
                        // These are taken from consequence type non-indexed field
                        break;
                    default: {
                        logger.warn("Unknown trait type: " + fields[0] + ", it should be HPO, ClinVar or Cosmic");
                        break;
                    }
                }
            }

            // TODO to be removed in next versions
            VariantTraitAssociation variantTraitAssociation = new VariantTraitAssociation();
            // This fills the old data model: variantTraitAssociation
            if (CollectionUtils.isNotEmpty(clinVarList)) {
                variantTraitAssociation.setClinvar(clinVarList);
            }
            if (CollectionUtils.isNotEmpty(cosmicList)) {
                variantTraitAssociation.setCosmic(cosmicList);
            }
            variantAnnotation.setVariantTraitAssociation(variantTraitAssociation);

            // This fills the new data model: traitAssociation
            List<EvidenceEntry> evidenceEntries = new ArrayList<>();
            // Clinvar -> traitAssociation
            for (ClinVar clinvar: clinVarList) {
                evidenceEntries.add(evidenceEntryConverter.fromClinVar(clinvar));
            }
            // Cosmic -> traitAssociation
            for (Cosmic cosmic: cosmicList) {
                evidenceEntries.add(evidenceEntryConverter.fromCosmic(cosmic));
            }
            variantAnnotation.setTraitAssociation(evidenceEntries);

            // Set the gene disease annotation
            if (CollectionUtils.isNotEmpty(geneTraitAssociationList)) {
                variantAnnotation.setGeneTraitAssociation(geneTraitAssociationList);
            }
        }

        return variantAnnotation;
    }

    /**
     * Conversion: from data model to storage type.
     *
     * @param variant Data model object
     * @return Storage type object
     */
    @Override
    public VariantSearchModel convertToStorageType(Variant variant) {
        VariantSearchModel variantSearchModel = new VariantSearchModel();

        // Set general Variant attributes: id, dbSNP, chromosome, start, end, type
        variantSearchModel.setId(variant.toString());       // Internal unique ID e.g.  3:1000:AT:-
        variantSearchModel.setVariantId(variant.getId());
        variantSearchModel.setChromosome(variant.getChromosome());
        variantSearchModel.setStart(variant.getStart());
        variantSearchModel.setEnd(variant.getEnd());
        variantSearchModel.setType(variant.getType().toString());

        // This field contains all possible IDs: id, dbSNP, names, genes, transcripts, protein, clinvar, hpo, ...
        // This will help when searching by variant id. This is added at the end of the method after collecting all IDs
        Set<String> xrefs = new HashSet<>();
        xrefs.add(variantSearchModel.getId());
        xrefs.add(variantSearchModel.getVariantId());
        if (variant.getNames() != null && !variant.getNames().isEmpty()) {
            variant.getNames().forEach(name -> {
                if (name != null) {
                    xrefs.add(name);
                }
            });
        }

        // convert Study related information
        if (CollectionUtils.isNotEmpty(variant.getStudies())) {
            ObjectWriter writer = new ObjectMapper().writer();

            for (StudyEntry studyEntry : variant.getStudies()) {
                String studyId = studyIdToSearchModel(studyEntry.getStudyId());
                variantSearchModel.getStudies().add(studyId);

                // We store the cohort stats with the format stats_STUDY_COHORT = value, e.g. stats_1kg_phase3_ALL=0.02
                if (studyEntry.getStats() != null && studyEntry.getStats().size() > 0) {
                    Map<String, VariantStats> studyStats = studyEntry.getStats();
                    for (String key : studyStats.keySet()) {
                        variantSearchModel.getStats().put("stats" + VariantSearchUtils.FIELD_SEPARATOR + studyId
                                + VariantSearchUtils.FIELD_SEPARATOR + key, studyStats.get(key).getAltAlleleFreq());
                    }
                }

                // samples, genotypes and format fields conversion
                if (MapUtils.isNotEmpty(studyEntry.getSamplesPosition()) && ListUtils.isNotEmpty(studyEntry.getOrderedSamplesName())) {
                    List<String> sampleNames = studyEntry.getOrderedSamplesName();
                    // sanity check, the number od sample names and sample data must be the same
                    if (ListUtils.isNotEmpty(studyEntry.getSamplesData()) && sampleNames.size() == studyEntry.getSamplesData().size()) {
                        String suffix = VariantSearchUtils.FIELD_SEPARATOR + studyId + VariantSearchUtils.FIELD_SEPARATOR;
                        // Save sample formats in a map (after, to JSON string), including sample names and GT
                        variantSearchModel.getSampleFormat().put("sampleFormat" + suffix + "sampleName",
                                StringUtils.join(sampleNames, LIST_SEP));

                        // find the index position of DP in the FORMAT
                        int dpIndexPos = -1;
                        if (ListUtils.isNotEmpty(studyEntry.getFormat())) {
                            variantSearchModel.getSampleFormat().put("sampleFormat" + suffix + "format",
                                    StringUtils.join(studyEntry.getFormat(), LIST_SEP));

                            // find the index position of DP in the FORMAT
                            for (int i = 0; i < studyEntry.getFormat().size(); i++) {
                                if ("DP".equalsIgnoreCase(studyEntry.getFormat().get(i))) {
                                    dpIndexPos = i;
                                    break;
                                }
                            }
                        }

                        for (int i = 0; i < sampleNames.size(); i++) {
                            suffix = VariantSearchUtils.FIELD_SEPARATOR + studyId
                                    + VariantSearchUtils.FIELD_SEPARATOR + sampleNames.get(i);

                            // Save genotype (gt) and depth (dp) where study and sample name as key
                            variantSearchModel.getGt().put("gt" + suffix, studyEntry.getSampleData(i).get(0));

                            if (dpIndexPos != -1) {
                                String dpValue = studyEntry.getSampleData(i).get(dpIndexPos);
                                // Skip if empty
                                if (!StringUtils.isEmpty(dpValue) && !dpValue.equals(VCFConstants.EMPTY_INFO_FIELD)) {
                                    try {
                                        variantSearchModel.getDp().put("dp" + suffix, Integer.valueOf(dpValue));
                                    } catch (NumberFormatException e) {
                                        logger.error("Problem converting from variant to variant search when getting DP"
                                                + " value from sample {}: {}", sampleNames.get(i), e.getMessage());
                                    }
                                }
                            }

                            // Save formats for each sample (after, to JSON string)
                            if (ListUtils.isNotEmpty(studyEntry.getSamplesData().get(i))) {
                                variantSearchModel.getSampleFormat().put("sampleFormat" + suffix,
                                        StringUtils.join(studyEntry.getSamplesData().get(i), LIST_SEP));
                            }
                        }
                    } else {
                        logger.error("Mismatch sizes: please, check your sample names, sample data and format array");
                    }
                }

                // QUAL, FILTER and file info fields management
                if (ListUtils.isNotEmpty(studyEntry.getFiles())) {
                    for (FileEntry fileEntry: studyEntry.getFiles()) {
                        // Call is stored in Solr fileInfo with key "fileCall"
                        Map<String, String> fileInfoMap = new LinkedHashMap<>();
                        if (StringUtils.isNotEmpty(fileEntry.getCall())) {
                            fileInfoMap.put("fileCall", fileEntry.getCall());
                        }
                        // Info fields are stored in Solr fileInfo
                        if (MapUtils.isNotEmpty(fileEntry.getAttributes())) {
                            fileInfoMap.putAll(fileEntry.getAttributes());

                            // In addition, store QUAL and FILTER separately
                            String qual = fileEntry.getAttributes().get(StudyEntry.QUAL);
                            if (StringUtils.isNotEmpty(qual)) {
                                variantSearchModel.getQual().put("qual" + VariantSearchUtils.FIELD_SEPARATOR + studyId
                                        + VariantSearchUtils.FIELD_SEPARATOR + fileEntry.getFileId(), Float.parseFloat(qual));
                            }

                            String filter = fileEntry.getAttributes().get(StudyEntry.FILTER);
                            if (StringUtils.isNotEmpty(filter)) {
                                variantSearchModel.getFilter().put("filter" + VariantSearchUtils.FIELD_SEPARATOR + studyId
                                        + VariantSearchUtils.FIELD_SEPARATOR + fileEntry.getFileId(), filter);
                            }
                        }
                        if (MapUtils.isNotEmpty(fileInfoMap)) {
                            try {
                                variantSearchModel.getFileInfo().put("fileInfo" + VariantSearchUtils.FIELD_SEPARATOR + studyId
                                                + VariantSearchUtils.FIELD_SEPARATOR + fileEntry.getFileId(),
                                        writer.writeValueAsString(fileInfoMap));
                            } catch (JsonProcessingException e) {
                                logger.info("Error converting fileInfo for study {} and file {}", studyId, fileEntry.getFileId());
                            }
                        }
                    }
                }
            }
        }

        // We init all annotation numeric values to MISSING_VALUE, this fixes two different scenarios:
        // 1. No Variant Annotation has been found, probably because it is a SV longer than 100bp.
        // 2. There are some conservation or CADD scores missing
        variantSearchModel.setSift(MISSING_VALUE);
        variantSearchModel.setPolyphen(MISSING_VALUE);

        variantSearchModel.setPhastCons(MISSING_VALUE);
        variantSearchModel.setPhylop(MISSING_VALUE);
        variantSearchModel.setGerp(MISSING_VALUE);

        variantSearchModel.setCaddRaw(MISSING_VALUE);
        variantSearchModel.setCaddScaled(MISSING_VALUE);

        // Process Variant Annotation
        VariantAnnotation variantAnnotation = variant.getAnnotation();
        if (variantAnnotation != null) {

            // This object will store all info and descriptions for full-text search
            Set<String> traits = new HashSet<>();

            // Set release field
            int release = -1;   // default value if missing is -1
            if (variantAnnotation.getAdditionalAttributes() != null
                    && variantAnnotation.getAdditionalAttributes().get(GROUP_NAME.key()) != null) {
                String releaseStr = variantAnnotation.getAdditionalAttributes().get(GROUP_NAME.key()).getAttribute().get(RELEASE.key());
                // example: release = "2,3,4"
                if (StringUtils.isNotEmpty(releaseStr)) {
                    releaseStr = releaseStr.split(",")[0];
                    if (StringUtils.isNumeric(releaseStr)) {
                        release = Integer.parseInt(releaseStr);
                    }
                }
            }
            variantSearchModel.setRelease(release);

            // Add cytoband names
            if (variantAnnotation.getCytoband() != null) {
                for (Cytoband cytoband : variantAnnotation.getCytoband()) {
                    xrefs.add(cytoband.getChromosome() + cytoband.getName());
                }
            }

            // Add all XRefs coming from the variant annotation
            if (variantAnnotation.getXrefs() != null && !variantAnnotation.getXrefs().isEmpty()) {
                variantAnnotation.getXrefs().forEach(xref -> {
                    if (xref != null) {
                        xrefs.add(xref.getId());
                    }
                });
            }

            // Add all HGVS coming from the variant annotation
            if (ListUtils.isNotEmpty(variantAnnotation.getHgvs())) {
                xrefs.addAll(variantAnnotation.getHgvs());
            }

            // Set Genes and Consequence Types and create Other list to insert transcript info (biotype, protein
            // variant annotation,...)
            List<ConsequenceType> consequenceTypes = variantAnnotation.getConsequenceTypes();
            List<String> other = new ArrayList<>();
            if (consequenceTypes != null) {
                // This MUST be a LinkedHashMap to keep the order of the elements!
                Map<String, Set<String>> genes = new LinkedHashMap<>();
                Set<Integer> soAccessions = new LinkedHashSet<>();
                Set<String> geneToSOAccessions = new LinkedHashSet<>();
                Set<String> biotypes = new LinkedHashSet<>();

                for (ConsequenceType conseqType : consequenceTypes) {
                    StringBuilder trans = new StringBuilder();

                    // Set genes and biotypes if exist
                    if (StringUtils.isNotEmpty(conseqType.getGeneName())) {
                        if (!genes.containsKey(conseqType.getGeneName())) {
                            genes.put(conseqType.getGeneName(), new LinkedHashSet<>());
                        }
                        // DO NOT change the order of the following code
                        genes.get(conseqType.getGeneName()).add(conseqType.getGeneName());
                        genes.get(conseqType.getGeneName()).add(conseqType.getEnsemblGeneId());
                        genes.get(conseqType.getGeneName()).add(conseqType.getEnsemblTranscriptId());

                        if (StringUtils.isNotEmpty(conseqType.getEnsemblTranscriptId())) {
                            trans.append("TRANS").append(FIELD_SEP).append(conseqType.getEnsemblTranscriptId());
                            trans.append(FIELD_SEP).append(StringUtils.isEmpty(conseqType.getBiotype())
                                    ? "" : conseqType.getBiotype());
                            trans.append(FIELD_SEP);
                            if (ListUtils.isNotEmpty(conseqType.getTranscriptAnnotationFlags())) {
                                trans.append(StringUtils.join(conseqType.getTranscriptAnnotationFlags(), ","));
                            }
                        }

                        xrefs.add(conseqType.getGeneName());
                        xrefs.add(conseqType.getEnsemblGeneId());
                        xrefs.add(conseqType.getEnsemblTranscriptId());

                        if (StringUtils.isNotEmpty(conseqType.getBiotype())) {
                            biotypes.add(conseqType.getBiotype());
                        }
                    }

                    // Remove 'SO:' prefix to Store SO Accessions as integers and also store the gene - SO acc relation
                    for (SequenceOntologyTerm sequenceOntologyTerm : conseqType.getSequenceOntologyTerms()) {
                        int soNumber = Integer.parseInt(sequenceOntologyTerm.getAccession().substring(3));
                        soAccessions.add(soNumber);

                        if (StringUtils.isNotEmpty(conseqType.getGeneName())) {
                            geneToSOAccessions.add(conseqType.getGeneName() + "_" + soNumber);
                            geneToSOAccessions.add(conseqType.getEnsemblGeneId() + "_" + soNumber);
                            geneToSOAccessions.add(conseqType.getEnsemblTranscriptId() + "_" + soNumber);

                            // Add a combination with the transcript flag
                            if (conseqType.getTranscriptAnnotationFlags() != null) {
                                for (String transcriptFlag : conseqType.getTranscriptAnnotationFlags()) {
                                    geneToSOAccessions.add(conseqType.getGeneName() + "_" + soNumber + "_" + transcriptFlag);
                                    geneToSOAccessions.add(conseqType.getEnsemblGeneId() + "_" + soNumber + "_" + transcriptFlag);
                                    geneToSOAccessions.add(conseqType.getEnsemblTranscriptId() + "_" + soNumber + "_" + transcriptFlag);

                                    // This is useful when no gene or transcript is used, for example we want 'LoF' in 'basic' transcripts
                                    geneToSOAccessions.add(soNumber + "_" + transcriptFlag);
                                }
                            }
                        }
                    }

                    //
                    if (StringUtils.isNotEmpty(conseqType.getCodon())
                            || (conseqType.getCdnaPosition() != null && conseqType.getCdnaPosition() > 0)
                            || (conseqType.getCdsPosition() != null && conseqType.getCdsPosition() > 0)) {
                        // Sanity check
                        if (trans.length() == 0) {
                            logger.warn("Codon information without Ensembl transcript ID");
                        } else {
                            trans.append(FIELD_SEP)
                                    .append(conseqType.getCdnaPosition() == null ? 0 : conseqType.getCdnaPosition())
                                    .append(FIELD_SEP)
                                    .append(conseqType.getCdsPosition() == null ? 0 : conseqType.getCdsPosition())
                                    .append(FIELD_SEP)
                                    .append(StringUtils.isNotEmpty(conseqType.getCodon()) ? conseqType.getCodon() : "");
                        }
                    }

                    if (conseqType.getProteinVariantAnnotation() != null) {
                        ProteinVariantAnnotation protVarAnnotation = conseqType.getProteinVariantAnnotation();

                        // Add UniProt accession, name and ID to xrefs
                        trans.append(FIELD_SEP);
                        if (StringUtils.isNotEmpty(protVarAnnotation.getUniprotAccession())) {
                            trans.append(protVarAnnotation.getUniprotAccession());
                            xrefs.add(protVarAnnotation.getUniprotAccession());
                        }

                        trans.append(FIELD_SEP);
                        if (StringUtils.isNotEmpty(protVarAnnotation.getUniprotName())) {
                            trans.append(protVarAnnotation.getUniprotName());
                            xrefs.add(protVarAnnotation.getUniprotName());
                        }

                        trans.append(FIELD_SEP);
                        if (StringUtils.isNotEmpty(protVarAnnotation.getUniprotVariantId())) {
                            trans.append(protVarAnnotation.getUniprotVariantId());
                            xrefs.add(protVarAnnotation.getUniprotVariantId());
                        }

                        trans.append(FIELD_SEP).append(protVarAnnotation.getPosition() == null
                                ? 0 : protVarAnnotation.getPosition());

                        trans.append(FIELD_SEP);
                        if (StringUtils.isNotEmpty(protVarAnnotation.getReference())
                                && StringUtils.isNotEmpty(protVarAnnotation.getAlternate())) {
                            trans.append(protVarAnnotation.getReference()).append("/")
                                    .append(protVarAnnotation.getAlternate());
                        }

                        // Create transcript info and add it into the other list
                        Score sift = getScore(protVarAnnotation.getSubstitutionScores(), "sift");
                        Score polyph = getScore(protVarAnnotation.getSubstitutionScores(), "polyphen");
                        trans.append(FIELD_SEP);
                        if (sift != null) {
                            trans.append(sift.getScore()).append(FIELD_SEP).append(sift.getDescription());
                        } else {
                            trans.append(FIELD_SEP);
                        }
                        trans.append(FIELD_SEP);
                        if (polyph != null) {
                            trans.append(polyph.getScore()).append(FIELD_SEP).append(polyph.getDescription());
                        } else {
                            trans.append(FIELD_SEP);
                        }

                        // Add keywords to and Features to traits
                        if (protVarAnnotation.getKeywords() != null) {
                            for (String keyword : protVarAnnotation.getKeywords()) {
                                traits.add("KW" + FIELD_SEP + protVarAnnotation.getUniprotAccession()
                                        + FIELD_SEP + keyword);
                            }
                        }

                        // Add protein domains
                        if (protVarAnnotation.getFeatures() != null) {
                            for (ProteinFeature proteinFeature : protVarAnnotation.getFeatures()) {
                                if (StringUtils.isNotEmpty(proteinFeature.getId())) {
                                    // We store them in xrefs and traits, the number of these IDs is very small
                                    xrefs.add(proteinFeature.getId());
                                    traits.add("PD" + FIELD_SEP + proteinFeature.getId() + FIELD_SEP
                                            + proteinFeature.getDescription());
                                }
                            }
                        }
                    }
                    if (StringUtils.isNotEmpty(conseqType.getEnsemblTranscriptId()) && trans.length() > 0) {
                        other.add(trans.toString());
                    }
                }

                // We store the accumulated data
                genes.forEach((s, strings) -> variantSearchModel.getGenes().addAll(strings));
                variantSearchModel.setSoAcc(new ArrayList<>(soAccessions));
                variantSearchModel.setGeneToSoAcc(new ArrayList<>(geneToSOAccessions));
                variantSearchModel.setBiotypes(new ArrayList<>(biotypes));

                // We now process Sift and Polyphen
                setProteinScores(consequenceTypes, variantSearchModel);
            }

            // Set Populations frequencies
            if (variantAnnotation.getPopulationFrequencies() != null) {
                Map<String, Float> populationFrequencies = new HashMap<>();
                for (PopulationFrequency populationFrequency : variantAnnotation.getPopulationFrequencies()) {
                    populationFrequencies.put("popFreq" + VariantSearchUtils.FIELD_SEPARATOR + populationFrequency.getStudy()
                                    + VariantSearchUtils.FIELD_SEPARATOR + populationFrequency.getPopulation(),
                            populationFrequency.getAltAlleleFreq());
                }
                if (!populationFrequencies.isEmpty()) {
                    variantSearchModel.setPopFreq(populationFrequencies);
                }
            }

            // Set Conservation scores
            if (variantAnnotation.getConservation() != null) {
                for (Score score : variantAnnotation.getConservation()) {
                    switch (score.getSource()) {
                        case "phastCons":
                            variantSearchModel.setPhastCons(score.getScore());
                            break;
                        case "phylop":
                            variantSearchModel.setPhylop(score.getScore());
                            break;
                        case "gerp":
                            variantSearchModel.setGerp(score.getScore());
                            break;
                        default:
                            logger.warn("Unknown 'conservation' source: score.getSource() = " + score.getSource());
                            break;
                    }
                }
            }

            // Set CADD
            if (variantAnnotation.getFunctionalScore() != null) {
                for (Score score : variantAnnotation.getFunctionalScore()) {
                    switch (score.getSource()) {
                        case "cadd_raw":
                        case "caddRaw":
                            variantSearchModel.setCaddRaw(score.getScore());
                            break;
                        case "cadd_scaled":
                        case "caddScaled":
                            variantSearchModel.setCaddScaled(score.getScore());
                            break;
                        default:
                            logger.warn("Unknown 'functional score' source: score.getSource() = " + score.getSource());
                            break;
                    }
                }
            }

            // Set variant traits: ClinVar, Cosmic, HPO, ...
            if (variantAnnotation.getVariantTraitAssociation() != null) {
                if (variantAnnotation.getVariantTraitAssociation().getClinvar() != null) {
                    variantAnnotation.getVariantTraitAssociation().getClinvar()
                            .forEach(cv -> {
                                xrefs.add(cv.getAccession());
                                cv.getTraits().forEach(cvt -> traits.add("CV" + FIELD_SEP + cv.getAccession()
                                        + FIELD_SEP + cvt + FIELD_SEP + "cs:"
                                        + cv.getClinicalSignificance()));
                            });
                }
                if (variantAnnotation.getVariantTraitAssociation().getCosmic() != null) {
                    variantAnnotation.getVariantTraitAssociation().getCosmic()
                            .forEach(cosmic -> {
                                xrefs.add(cosmic.getMutationId());
                                traits.add("CM" + FIELD_SEP + cosmic.getMutationId() + FIELD_SEP
                                        + cosmic.getPrimaryHistology() + FIELD_SEP
                                        + cosmic.getHistologySubtype());
                            });
                }
            }
            if (variantAnnotation.getGeneTraitAssociation() != null
                    && CollectionUtils.isNotEmpty(variantAnnotation.getGeneTraitAssociation())) {
                for (GeneTraitAssociation geneTraitAssociation : variantAnnotation.getGeneTraitAssociation()) {
                    switch (geneTraitAssociation.getSource().toLowerCase()) {
                        case "hpo":
                            traits.add("HP" + FIELD_SEP + geneTraitAssociation.getHpo() + FIELD_SEP
                                    + geneTraitAssociation.getId() + " -- " + geneTraitAssociation.getName());
                            break;
//                        case "disgenet":
//                            traits.add("DG -- " + geneTraitAssociation.getId() + " -- " + geneTraitAssociation.getName());
//                            break;
                        default:
                            break;
                    }
                }
            }
            variantSearchModel.setTraits(new ArrayList<>(traits));

            // Now we fill other field
            if (StringUtils.isNotEmpty(variantAnnotation.getDisplayConsequenceType())) {
                other.add("DCT" + FIELD_SEP + variantAnnotation.getDisplayConsequenceType());
            }
            if (variantAnnotation.getHgvs() != null) {
                for (String hgvs : variantAnnotation.getHgvs()) {
                    other.add("HGVS" + FIELD_SEP + hgvs);
                }
            }
            if (variantAnnotation.getCytoband() != null) {
                for (Cytoband cytoband : variantAnnotation.getCytoband()) {
                    other.add("CB" + FIELD_SEP + cytoband.getName() + FIELD_SEP + cytoband.getStain()
                            + FIELD_SEP + cytoband.getStart() + FIELD_SEP + cytoband.getEnd());
                }
            }
            if (variantAnnotation.getRepeat() != null) {
                for (Repeat repeat : variantAnnotation.getRepeat()) {
                    other.add("RP" + FIELD_SEP + repeat.getId() + FIELD_SEP + repeat.getSource()
                            + FIELD_SEP + repeat.getCopyNumber() + FIELD_SEP + repeat.getPercentageMatch()
                            + FIELD_SEP + repeat.getStart() + FIELD_SEP + repeat.getEnd());
                }
            }
            variantSearchModel.setOther(other);
        }

        variantSearchModel.setXrefs(new ArrayList<>(xrefs));
        return variantSearchModel;
    }

    public static String studyIdToSearchModel(String studyId) {
        return studyId.substring(studyId.lastIndexOf(':') + 1);
    }

    public List<VariantSearchModel> convertListToStorageType(List<Variant> variants) {
        List<VariantSearchModel> variantSearchModelList = new ArrayList<>(variants.size());
        for (Variant variant : variants) {
            VariantSearchModel variantSearchModel = convertToStorageType(variant);
            if (variantSearchModel.getId() != null) {
                variantSearchModelList.add(variantSearchModel);
            }
        }
        return variantSearchModelList;
    }

    /**
     * Retrieve the protein substitution scores and descriptions from a consequence
     * type annotation: sift or polyphen, and update the variant search model.
     *
     * @param consequenceTypes   List of consequence type target
     * @param variantSearchModel Variant search model to update
     */
    private void setProteinScores(List<ConsequenceType> consequenceTypes, VariantSearchModel variantSearchModel) {
        double sift = 10;
        String siftDesc = "";
        double polyphen = MISSING_VALUE;
        String polyphenDesc = "";

        if (consequenceTypes != null) {
            for (ConsequenceType consequenceType : consequenceTypes) {
                if (consequenceType.getProteinVariantAnnotation() != null
                        && consequenceType.getProteinVariantAnnotation().getSubstitutionScores() != null) {
                    for (Score score : consequenceType.getProteinVariantAnnotation().getSubstitutionScores()) {
                        String source = score.getSource();
                        if (("sift").equals(source)) {
                            if (score.getScore() < sift) {
                                sift = score.getScore();
                                siftDesc = score.getDescription();
                            }
                        } else if (("polyphen").equals(source)) {
                            if (score.getScore() > polyphen) {
                                polyphen = score.getScore();
                                polyphenDesc = score.getDescription();
                            }
                        }
                    }
                }
            }
        }

        // If sift not exist we set it to -100.0
        if (ArrayUtils.equals(sift, 10)) {
            sift = MISSING_VALUE;
        }

        // set scores
        variantSearchModel.setSift(sift);
        variantSearchModel.setPolyphen(polyphen);

        // set descriptions
        variantSearchModel.setSiftDesc(siftDesc);
        variantSearchModel.setPolyphenDesc(polyphenDesc);
    }

    private Score getScore(List<Score> scores, String source) {
        if (ListUtils.isNotEmpty(scores) && StringUtils.isNotEmpty(source)) {
            for (Score score: scores) {
                if (source.equals(score.getSource())) {
                    return score;
                }
            }
        }
        return null;
    }
}
