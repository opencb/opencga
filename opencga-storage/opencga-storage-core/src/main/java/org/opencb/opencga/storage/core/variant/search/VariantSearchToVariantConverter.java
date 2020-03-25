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
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.collections.MapUtils;
import org.apache.commons.lang3.StringUtils;
import org.opencb.biodata.models.variant.StudyEntry;
import org.opencb.biodata.models.variant.Variant;
import org.opencb.biodata.models.variant.annotation.ConsequenceTypeMappings;
import org.opencb.biodata.models.variant.avro.*;
import org.opencb.biodata.models.variant.stats.VariantStats;
import org.opencb.commons.datastore.core.ComplexTypeConverter;
import org.opencb.commons.utils.ListUtils;
import org.opencb.opencga.storage.core.variant.adaptors.VariantField;
import org.opencb.opencga.storage.core.variant.annotation.converters.VariantTraitAssociationToEvidenceEntryConverter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.*;

import static org.opencb.opencga.storage.core.variant.adaptors.VariantField.AdditionalAttributes.GROUP_NAME;
import static org.opencb.opencga.storage.core.variant.adaptors.VariantField.AdditionalAttributes.RELEASE;
import static org.opencb.opencga.storage.core.variant.search.VariantSearchUtils.FIELD_SEPARATOR;

//import org.opencb.opencga.core.common.ArrayUtils;

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

    private Map<String, StudyEntry> studyEntryMap;
    private Map<String, VariantScore> scoreStudyMap;
    private List<String> other = new ArrayList<>();

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
        studyEntryMap = new HashMap<>();
        scoreStudyMap = new LinkedHashMap<>();
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
                String suffix = FIELD_SEPARATOR + studyId + FIELD_SEPARATOR;
                StudyEntry studyEntry = studyEntryMap.get(studyId);

                // Format
                stringToList = variantSearchModel.getSampleFormat().get("sampleFormat" + suffix + "format");
                if (StringUtils.isNotEmpty(stringToList)) {
                    studyEntry.setSampleDataKeys(Arrays.asList(StringUtils.splitByWholeSeparatorPreserveAllTokens(stringToList, LIST_SEP)));
                }

                // Sample Data management
                stringToList = variantSearchModel.getSampleFormat().get("sampleFormat" + suffix + "sampleName");
                if (StringUtils.isNotEmpty(stringToList)) {
                    String[] sampleNames = StringUtils.splitByWholeSeparatorPreserveAllTokens(stringToList, LIST_SEP);
                    List<SampleEntry> sampleEntries = new ArrayList<>();
                    Map<String, Integer> samplePosition = new HashMap<>();
                    int pos = 0;
                    for (String sampleName: sampleNames) {
                        suffix = FIELD_SEPARATOR + studyId + FIELD_SEPARATOR + sampleName;
                        stringToList = variantSearchModel.getSampleFormat().get("sampleFormat" + suffix);
                        if (StringUtils.isNotEmpty(stringToList)) {
                            List<String> data = Arrays.asList(StringUtils.splitByWholeSeparatorPreserveAllTokens(stringToList, LIST_SEP));
                            sampleEntries.add(new SampleEntry(null, null, data));
                            samplePosition.put(sampleName, pos++);
                        }
//                        else {
//                            logger.error("Error converting samplesFormat, sample '{}' is missing or empty, value: '{}'",
//                                    sampleName, stringToList);
//                        }
                    }

                    if (CollectionUtils.isNotEmpty(sampleEntries)) {
                        studyEntry.setSamples(sampleEntries);
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
                String[] fields = StringUtils.splitByWholeSeparator(key, FIELD_SEPARATOR);
                FileEntry fileEntry = new FileEntry(fields[2], null, new HashMap<>());
                try {
                    // We obtain the original call
                    Map<String, String> fileData = reader.readValue(variantSearchModel.getFileInfo().get(key));
                    if (MapUtils.isNotEmpty(fileData)) {
                        String fileCall = fileData.get("fileCall");
                        if (fileCall != null && !fileCall.isEmpty()) {
                            int i = fileCall.lastIndexOf(':');
                            OriginalCall call = new OriginalCall(
                                    fileCall.substring(0, i),
                                    Integer.valueOf(fileCall.substring(i + 1)));
                            fileEntry.setCall(call);
                        }

                        fileData.remove("fileCall");
                        fileEntry.setData(fileData);
                    }
                } catch (IOException e) {
                    logger.error("Error converting fileInfo from variant search model: {}", e.getMessage());
                } finally {
                    variant.getStudy(fields[1]).getFiles().add(fileEntry);
                }
            }
        }

        // Allele stats management
        if (MapUtils.isNotEmpty(variantSearchModel.getAltStats())) {
            for (String key: variantSearchModel.getAltStats().keySet()) {
                // key consists of 'altStats' + "__" + studyId + "__" + cohort
                String[] fields = StringUtils.splitByWholeSeparator(key, FIELD_SEPARATOR);
                if (studyEntryMap.containsKey(fields[1])) {
                    VariantStats variantStats;
                    if (studyEntryMap.get(fields[1]).getStats() != null) {
                        variantStats = studyEntryMap.get(fields[1]).getStats(fields[2]);
                        if (variantStats == null) {
                            variantStats = new VariantStats(fields[2]);
                        }
                    } else {
                        variantStats = new VariantStats(fields[2]);
                    }
                    variantStats.setRefAlleleFreq(1 - variantSearchModel.getAltStats().get(key));
                    variantStats.setAltAlleleFreq(variantSearchModel.getAltStats().get(key));
                    variantStats.setMaf(Math.min(variantSearchModel.getAltStats().get(key), 1 - variantSearchModel.getAltStats().get(key)));
                    studyEntryMap.get(fields[1]).addStats(variantStats);
                }
            }
        }

        // Filter stats (PASS) management
        if (MapUtils.isNotEmpty(variantSearchModel.getPassStats())) {
            for (String key: variantSearchModel.getPassStats().keySet()) {
                // key consists of 'passStats' + "__" + studyId + "__" + cohort
                String[] fields = StringUtils.splitByWholeSeparator(key, FIELD_SEPARATOR);
                if (studyEntryMap.containsKey(fields[1])) {
                    VariantStats variantStats;
                    if (studyEntryMap.get(fields[1]).getStats() != null) {
                        variantStats = studyEntryMap.get(fields[1]).getStats(fields[2]);
                        if (variantStats == null) {
                            variantStats = new VariantStats(fields[2]);
                        }
                    } else {
                        variantStats = new VariantStats(fields[2]);
                    }
                    Map<String, Float> filterFreq = new HashMap<>();
                    filterFreq.put(VCFConstants.PASSES_FILTERS_v4, variantSearchModel.getPassStats().get(key));
                    variantStats.setFilterFreq(filterFreq);
                    studyEntryMap.get(fields[1]).addStats(variantStats);
                }
            }
        }

        // Process annotation (for performance purposes, variant scores are processed too)
        variant.setAnnotation(getVariantAnnotation(variantSearchModel, variant));

        // Set variant scores from score study map
        if (MapUtils.isNotEmpty(scoreStudyMap)) {
            for (Map.Entry<String, VariantScore> entry : scoreStudyMap.entrySet()) {
                String studyId = entry.getKey().split(FIELD_SEP)[0];
                if (studyEntryMap.get(studyId).getScores() == null) {
                    studyEntryMap.get(studyId).setScores(new ArrayList<>());
                }
                studyEntryMap.get(studyId).getScores().add(entry.getValue());
            }
        }

        return variant;
    }

    public VariantAnnotation getVariantAnnotation(VariantSearchModel variantSearchModel, Variant variant) {

        if (includeFields != null && !includeFields.contains(VariantField.ANNOTATION)) {
            updateScoreStudyMap(variantSearchModel);
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
            String[] fields = StringUtils.splitByWholeSeparatorPreserveAllTokens(other, FIELD_SEP);
            switch (fields[0]) {
                case "DCT":
                    variantAnnotation.setDisplayConsequenceType(fields[1]);
                    break;
                case "HGVS":
                    variantAnnotation.getHgvs().add(fields[1]);
                    break;
                case "SC":
                    updateScoreStudyMap(fields);
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
                            .setCopyNumber(parseFloat(fields[3], null))
                            .setPercentageMatch(parseFloat(fields[4], null))
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
                        List<Score> scores = new ArrayList<>(2);
                        if (fields.length > 12
                                && (StringUtils.isNotEmpty(fields[12]) || StringUtils.isNotEmpty(fields[13]))) {
                            Score score = new Score();
                            score.setSource("sift");
                            if (StringUtils.isNotEmpty(fields[12])) {
                                score.setScore(parseDouble(fields[12], null, "Exception parsing Sift score"));
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
                                score.setScore(parseDouble(fields[14], null, "Exception parsing Polyphen score"));
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
                    break;
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
//        if (!equalWithEpsilon(variantSearchModel.getSift(), MISSING_VALUE)
//                || !equalWithEpsilon(variantSearchModel.getPolyphen(), MISSING_VALUE)) {
//            scores = new ArrayList<>();
//            if (!equalWithEpsilon(variantSearchModel.getSift(), MISSING_VALUE)) {
//                scores.add(new Score(variantSearchModel.getSift(), "sift", variantSearchModel.getSiftDesc()));
//            }
//            if (!equalWithEpsilon(variantSearchModel.getPolyphen(), MISSING_VALUE)) {
//                scores.add(new Score(variantSearchModel.getPolyphen(), "polyphen", variantSearchModel.getPolyphenDesc()));
//            }
//            proteinAnnotation.setSubstitutionScores(scores);
//        }

        // and finally, update the SO acc. for each conseq. type and setProteinVariantAnnotation if SO accession is 1583
        Set<Integer> geneRelatedSoTerms = new HashSet<>();
        if (variantSearchModel.getGeneToSoAcc() != null) {
            for (String geneToSoAcc : variantSearchModel.getGeneToSoAcc()) {
                String[] fields = geneToSoAcc.split("_");
                if (fields.length == 2 && StringUtils.isNumeric(fields[1]) && consequenceTypeMap.containsKey(fields[0])) {
                    int soAcc = Integer.parseInt(fields[1]);
                    geneRelatedSoTerms.add(soAcc);  // we memorise the SO term for next block

                    SequenceOntologyTerm sequenceOntologyTerm = new SequenceOntologyTerm();
                    sequenceOntologyTerm.setAccession("SO:" + String.format("%07d", soAcc));
                    sequenceOntologyTerm.setName(ConsequenceTypeMappings.accessionToTerm.get(soAcc));
                    if (consequenceTypeMap.get(fields[0]).getSequenceOntologyTerms() == null) {
                        consequenceTypeMap.get(fields[0]).setSequenceOntologyTerms(new ArrayList<>());
                    }
                    consequenceTypeMap.get(fields[0]).getSequenceOntologyTerms().add(sequenceOntologyTerm);
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

        // set population frequencies
        List<PopulationFrequency> populationFrequencies = new ArrayList<>();
        if (variantSearchModel.getPopFreq() != null && variantSearchModel.getPopFreq().size() > 0) {
            for (String key : variantSearchModel.getPopFreq().keySet()) {
                PopulationFrequency populationFrequency = new PopulationFrequency();
                String[] fields = StringUtils.splitByWholeSeparator(key, FIELD_SEPARATOR);
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
        if (!equalWithEpsilon(variantSearchModel.getPhylop(), MISSING_VALUE)) {
            scores.add(new Score(variantSearchModel.getPhylop(), "phylop", ""));
        }
        if (!equalWithEpsilon(variantSearchModel.getPhastCons(), MISSING_VALUE)) {
            scores.add(new Score(variantSearchModel.getPhastCons(), "phastCons", ""));
        }
        if (!equalWithEpsilon(variantSearchModel.getGerp(), MISSING_VALUE)) {
            scores.add(new Score(variantSearchModel.getGerp(), "gerp", ""));
        }
        variantAnnotation.setConservation(scores);

        // Set CADD scores
        scores = new ArrayList<>();
        if (!equalWithEpsilon(variantSearchModel.getCaddRaw(), MISSING_VALUE)) {
            scores.add(new Score(variantSearchModel.getCaddRaw(), "cadd_raw", ""));
        }
        if (!equalWithEpsilon(variantSearchModel.getCaddScaled(), MISSING_VALUE)) {
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
                        // Variant trait: CV -- accession -- trait -- clinicalSignificance
                        if (!clinVarMap.containsKey(fields[1])) {
                            String clinicalSignificance = "";
                            if (fields.length > 3 && StringUtils.isNotEmpty(fields[3])) {
                                clinicalSignificance = fields[3];
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

    private void updateScoreStudyMap(VariantSearchModel variantSearchModel) {
        // Get cohort1 and cohort2
        if (scoreStudyMap.size() > 0 && CollectionUtils.isNotEmpty(variantSearchModel.getOther())) {
            for (String other : variantSearchModel.getOther()) {
                if (StringUtils.isNotEmpty(other) && other.startsWith("SC")) {
                    updateScoreStudyMap(StringUtils.splitByWholeSeparatorPreserveAllTokens(other, FIELD_SEP));
                }
            }
        }
    }

    private void updateScoreStudyMap(String[] fields) {
        // Fields content: SC -- studyId -- scoreId -- score -- p-value -- cohort1 -- cohort2
        if (studyEntryMap.containsKey(fields[1])) {
            String scoreStudyKey = fields[1] + FIELD_SEP + fields[2];
            if (!scoreStudyMap.containsKey(scoreStudyKey)) {
                VariantScore variantScore = new VariantScore();
                variantScore.setId(fields[2]);
                scoreStudyMap.put(scoreStudyKey, variantScore);
            }
            scoreStudyMap.get(scoreStudyKey).setScore(Float.parseFloat(fields[3]));
            scoreStudyMap.get(scoreStudyKey).setPValue(Float.parseFloat(fields[4]));
            scoreStudyMap.get(scoreStudyKey).setCohort1(fields[5]);
            if (fields.length > 6) {
                scoreStudyMap.get(scoreStudyKey).setCohort2(fields[6]);
            }
        }
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

        // Create the Other list to insert scores and transcripts info (biotype, protein, variant annotation,...)
        other = new ArrayList<>();

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
        convertStudies(variant, variantSearchModel);

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

            // Set Genes and Consequence Types
            List<ConsequenceType> consequenceTypes = variantAnnotation.getConsequenceTypes();
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
                        // One gene can contain several transcripts and therefore several Consequence Types
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

                            // Add the combination of Gene and Biotype, this will prevent variants to be returned when they overlap
                            // two different genes where the overlapping gene has the wanted Biotype.
                            geneToSOAccessions.add(conseqType.getGeneName() + "_" + conseqType.getBiotype());
                            geneToSOAccessions.add(conseqType.getEnsemblGeneId() + "_" + conseqType.getBiotype());
                            geneToSOAccessions.add(conseqType.getEnsemblTranscriptId() + "_" + conseqType.getBiotype());
                        }
                    }

                    // Remove 'SO:' prefix to Store SO Accessions as integers and also store the gene - SO acc relation
                    for (SequenceOntologyTerm sequenceOntologyTerm : conseqType.getSequenceOntologyTerms()) {
                        int soIdInt = Integer.parseInt(sequenceOntologyTerm.getAccession().substring(3));
                        soAccessions.add(soIdInt);

                        if (StringUtils.isNotEmpty(conseqType.getGeneName())) {
                            geneToSOAccessions.add(conseqType.getGeneName() + "_" + soIdInt);
                            geneToSOAccessions.add(conseqType.getEnsemblGeneId() + "_" + soIdInt);
                            geneToSOAccessions.add(conseqType.getEnsemblTranscriptId() + "_" + soIdInt);

                            if (StringUtils.isNotEmpty(conseqType.getBiotype())) {
                                geneToSOAccessions.add(conseqType.getGeneName() + "_" + conseqType.getBiotype() + "_" + soIdInt);
                                geneToSOAccessions.add(conseqType.getEnsemblGeneId() + "_" + conseqType.getBiotype() + "_" + soIdInt);
                                geneToSOAccessions.add(conseqType.getEnsemblTranscriptId() + "_" + conseqType.getBiotype() + "_" + soIdInt);

                                // This is useful when no gene or transcript is passed, for example we want 'LoF' in real 'protein_coding'
                                geneToSOAccessions.add(conseqType.getBiotype() + "_" + soIdInt);
                            }

                            // Add a combination with the transcript flag
                            if (conseqType.getTranscriptAnnotationFlags() != null) {
                                for (String transcriptFlag : conseqType.getTranscriptAnnotationFlags()) {
                                    if (transcriptFlag.equalsIgnoreCase("basic") || transcriptFlag.equalsIgnoreCase("CCDS")) {
                                        geneToSOAccessions.add(conseqType.getGeneName() + "_" + soIdInt + "_" + transcriptFlag);
                                        geneToSOAccessions.add(conseqType.getEnsemblGeneId() + "_" + soIdInt + "_" + transcriptFlag);
                                        geneToSOAccessions.add(conseqType.getEnsemblTranscriptId() + "_" + soIdInt + "_" + transcriptFlag);
                                        // This is useful when no gene or transcript is used, for example 'LoF' in 'basic' transcripts
                                        geneToSOAccessions.add(soIdInt + "_" + transcriptFlag);
                                    }
                                }
                            }
                        }
                    }

                    //
                    if (StringUtils.isNotEmpty(conseqType.getCodon())
                            || (conseqType.getCdnaPosition() != null && conseqType.getCdnaPosition() > 0)
                            || (conseqType.getCdsPosition() != null && conseqType.getCdsPosition() > 0)) {
                        if (trans.length() == 0) {  // Sanity check
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
            Map<String, Float> populationFrequencies = new HashMap<>();
            if (variantAnnotation.getPopulationFrequencies() != null) {
                for (PopulationFrequency populationFrequency : variantAnnotation.getPopulationFrequencies()) {
                    populationFrequencies.put("popFreq"
                                    + FIELD_SEPARATOR + populationFrequency.getStudy()
                                    + FIELD_SEPARATOR + populationFrequency.getPopulation(),
                            populationFrequency.getAltAlleleFreq());
                }
            }
            // Add 0.0 for mot commonly used populations, this will allow to skip a NON EXIST query and improve performance
            populationFrequencies.putIfAbsent("popFreq" + FIELD_SEPARATOR + "1kG_phase3__ALL", 0.0f);
            populationFrequencies.putIfAbsent("popFreq" + FIELD_SEPARATOR + "GNOMAD_GENOMES__ALL", 0.0f);
//            populationFrequencies.putIfAbsent("popFreq" + FIELD_SEPARATOR + "GNOMAD_EXOMES__ALL", 0.0f);
            // Set population frequencies into the model
            variantSearchModel.setPopFreq(populationFrequencies);

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
            if (CollectionUtils.isNotEmpty(variantAnnotation.getTraitAssociation())) {
                Set<String> clinSigSet = new HashSet<>();
                for (EvidenceEntry ev : variantAnnotation.getTraitAssociation()) {
                    if (ev.getSource() != null && StringUtils.isNotEmpty(ev.getSource().getName())) {
                        if (StringUtils.isNotEmpty(ev.getId())) {
                            xrefs.add(ev.getId());
                        }
                        if ("clinvar".equalsIgnoreCase(ev.getSource().getName())) {
                            String clinSigSuffix = "";
                            if (ev.getVariantClassification() != null
                                    && ev.getVariantClassification().getClinicalSignificance() != null) {
                                clinSigSuffix = FIELD_SEP + ev.getVariantClassification().getClinicalSignificance().name();
                                clinSigSet.add(ev.getVariantClassification().getClinicalSignificance().name());
                            }
                            if (CollectionUtils.isNotEmpty(ev.getHeritableTraits())) {
                                for (HeritableTrait trait : ev.getHeritableTraits()) {
                                    traits.add("CV" + FIELD_SEP + ev.getId() + FIELD_SEP + trait + clinSigSuffix);
                                }
                            }
                        } else if ("cosmic".equalsIgnoreCase(ev.getSource().getName())) {
                            if (ev.getSomaticInformation() != null) {
                                traits.add("CM" + FIELD_SEP + ev.getId() + FIELD_SEP + ev.getSomaticInformation().getHistologySubtype()
                                        + FIELD_SEP + ev.getSomaticInformation().getHistologySubtype());
                            }
                        }
                    }
                }
                if (CollectionUtils.isNotEmpty(clinSigSet)) {
                    variantSearchModel.setClinicalSig(new ArrayList<>(clinSigSet));
                }
            }
            if (variantAnnotation.getGeneTraitAssociation() != null
                    && CollectionUtils.isNotEmpty(variantAnnotation.getGeneTraitAssociation())) {
                for (GeneTraitAssociation geneTraitAssociation : variantAnnotation.getGeneTraitAssociation()) {
                    switch (geneTraitAssociation.getSource().toLowerCase()) {
                        case "hpo":
                            traits.add("HP" + FIELD_SEP + geneTraitAssociation.getHpo() + FIELD_SEP + geneTraitAssociation.getId()
                                    + FIELD_SEP + geneTraitAssociation.getName());
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
        }
        if (CollectionUtils.isNotEmpty(other)) {
            variantSearchModel.setOther(other);
        }

        variantSearchModel.setXrefs(new ArrayList<>(xrefs));
        return variantSearchModel;
    }

    private void convertStudies(Variant variant, VariantSearchModel variantSearchModel) {
        // Sanity check
        if (CollectionUtils.isEmpty(variant.getStudies())) {
            return;
        }

        ObjectWriter writer = new ObjectMapper().writer();

        for (StudyEntry studyEntry : variant.getStudies()) {
            String studyId = studyIdToSearchModel(studyEntry.getStudyId());
            variantSearchModel.getStudies().add(studyId);

            // We store the cohort stats:
            //    - altStats__STUDY__COHORT = alternalte allele freq, e.g. altStats_1kg_phase3_ALL=0.02
            //    - passStats__STUDY__COHORT = pass filter freq
            if (studyEntry.getStats() != null && studyEntry.getStats().size() > 0) {
                List<VariantStats> studyStats = studyEntry.getStats();
                for (VariantStats stats : studyStats) {
                    String cohortId = stats.getCohortId();
                    // Alternate allele frequency
                    variantSearchModel.getAltStats().put("altStats" + FIELD_SEPARATOR + studyId + FIELD_SEPARATOR + cohortId,
                            stats.getAltAlleleFreq());

                    // PASS filter frequency
                    if (MapUtils.isNotEmpty(stats.getFilterCount())
                            && stats.getFilterCount().containsKey(VCFConstants.PASSES_FILTERS_v4)) {
                        variantSearchModel.getPassStats().put("passStats" + FIELD_SEPARATOR + studyId + FIELD_SEPARATOR + cohortId,
                                stats.getFilterFreq().get(VCFConstants.PASSES_FILTERS_v4));
                    }
                }
            }

            if (MapUtils.isNotEmpty(studyEntry.getSamplesPosition()) && ListUtils.isNotEmpty(studyEntry.getOrderedSamplesName())) {
                List<String> sampleNames = studyEntry.getOrderedSamplesName();
                // sanity check, the number od sample names and sample data must be the same
                if (CollectionUtils.isNotEmpty(studyEntry.getSamples()) && sampleNames.size() == studyEntry.getSamples().size()) {
                    String suffix = FIELD_SEPARATOR + studyId + FIELD_SEPARATOR;
                    // Save sample formats in a map (after, to JSON string), including sample names and GT
                    variantSearchModel.getSampleFormat().put("sampleFormat" + suffix + "sampleName",
                            StringUtils.join(sampleNames, LIST_SEP));

                    // find the index position of DP in the FORMAT
                    int dpIndexPos = -1;
                    if (CollectionUtils.isNotEmpty(studyEntry.getSampleDataKeys())) {
                        variantSearchModel.getSampleFormat().put("sampleFormat" + suffix + "format",
                                StringUtils.join(studyEntry.getSampleDataKeys(), LIST_SEP));

                        // find the index position of DP in the FORMAT
                        for (int i = 0; i < studyEntry.getSampleDataKeys().size(); i++) {
                            if ("DP".equalsIgnoreCase(studyEntry.getSampleDataKeys().get(i))) {
                                dpIndexPos = i;
                                break;
                            }
                        }
                    }

                    for (int i = 0; i < sampleNames.size(); i++) {
                        suffix = FIELD_SEPARATOR + studyId
                                + FIELD_SEPARATOR + sampleNames.get(i);

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
                        if (studyEntry.getSamples().get(i) != null) {
                            variantSearchModel.getSampleFormat().put("sampleFormat" + suffix,
                                    StringUtils.join(studyEntry.getSamples().get(i).getData(), LIST_SEP));
                        }
                    }
                } else {
                    logger.error("Mismatch sizes: please, check your sample names, sample data and format array");
                }
            }

            // VariantScore: score and p-value are stored and indexed in two different maps:
            //   - score__STUDY_ID__SCORE_ID
            //   - scorePValue__STUDY_ID__SCORE_ID
            // and the score, pValue, cohort1 and cohort2 into the Other list (not indexed)
            if (CollectionUtils.isNotEmpty(studyEntry.getScores())) {
                for (VariantScore score : studyEntry.getScores()) {
                    String suffix = FIELD_SEPARATOR + studyId + FIELD_SEPARATOR + score.getId();
                    // score (indexed)
                    variantSearchModel.getScore().put("score" + suffix, score.getScore());

                    // pValue (indexed)
                    variantSearchModel.getScorePValue().put("scorePValue" + suffix, score.getPValue());

                    // and save score, pValue, cohort1 and cohort2 into the Other list (not indexed)
                    other.add("SC" + FIELD_SEP + studyId + FIELD_SEP + score.getId() + FIELD_SEP + score.getScore() + FIELD_SEP
                            + score.getPValue() + FIELD_SEP + score.getCohort1()
                            + (StringUtils.isNotEmpty(score.getCohort2()) ? (FIELD_SEP + score.getCohort2()) : ""));
                }
            }

            // QUAL, FILTER and file info fields management
            if (ListUtils.isNotEmpty(studyEntry.getFiles())) {
                for (FileEntry fileEntry : studyEntry.getFiles()) {
                    // Call is stored in Solr fileInfo with key "fileCall"
                    Map<String, String> fileInfoMap = new LinkedHashMap<>();
                    if (fileEntry.getCall() != null) {
                        fileInfoMap.put("fileCall", fileEntry.getCall().getVariantId() + ":" + fileEntry.getCall().getAlleleIndex());
                    }
                    // Info fields are stored in Solr fileInfo
                    if (MapUtils.isNotEmpty(fileEntry.getData())) {
                        fileInfoMap.putAll(fileEntry.getData());

                        // In addition, store QUAL and FILTER separately
                        String qual = fileEntry.getData().get(StudyEntry.QUAL);
                        if (StringUtils.isNotEmpty(qual)) {
                            variantSearchModel.getQual().put("qual" + FIELD_SEPARATOR + studyId
                                    + FIELD_SEPARATOR + fileEntry.getFileId(), Float.parseFloat(qual));
                        }

                        String filter = fileEntry.getData().get(StudyEntry.FILTER);
                        if (StringUtils.isNotEmpty(filter)) {
                            variantSearchModel.getFilter().put("filter" + FIELD_SEPARATOR + studyId
                                    + FIELD_SEPARATOR + fileEntry.getFileId(), filter);
                        }
                    }
                    if (MapUtils.isNotEmpty(fileInfoMap)) {
                        try {
                            variantSearchModel.getFileInfo().put("fileInfo" + FIELD_SEPARATOR + studyId
                                            + FIELD_SEPARATOR + fileEntry.getFileId(),
                                    writer.writeValueAsString(fileInfoMap));
                        } catch (JsonProcessingException e) {
                            logger.info("Error converting fileInfo for study {} and file {}", studyId, fileEntry.getFileId());
                        }
                    }
                }
            }
        }
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
        if (equalWithEpsilon(sift, 10)) {
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

    boolean equalWithEpsilon(double first, double second) {
//        return Precision.equals(d1, d2, Precision.EPSILON);
        final double epsilon = 0.000000000000001;
        return (Math.abs(second - first) < epsilon);
    }

    private Double parseDouble(String value, Double defaultValue) {
        return parseDouble(value, defaultValue, null);
    }

    private Double parseDouble(String value, Double defaultValue, String message) {
        try {
            return Double.parseDouble(value);
        } catch (NumberFormatException e) {
            if (message != null) {
                logger.warn(message + ". Value: '" + value + "'");
            }
            return defaultValue;
        }
    }

    private Float parseFloat(String value, Float defaultValue) {
        return parseFloat(value, defaultValue, null);
    }

    private Float parseFloat(String value, Float defaultValue, String message) {
        try {
            return Float.parseFloat(value);
        } catch (NumberFormatException e) {
            if (message != null) {
                logger.warn(message + ". Value: '" + value + "'");
            }
            return defaultValue;
        }
    }
}
