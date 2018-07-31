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
import org.opencb.opencga.storage.core.variant.annotation.converters.VariantTraitAssociationToEvidenceEntryConverter;
import org.opencb.opencga.storage.core.variant.search.solr.VariantSearchManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.*;

/**
 * Created by imedina on 14/11/16.
 */
public class VariantSearchToVariantConverter implements ComplexTypeConverter<Variant, VariantSearchModel> {

    public static final double MISSING_VALUE = -100.0;
    private Logger logger = LoggerFactory.getLogger(VariantSearchManager.class);
    private final VariantTraitAssociationToEvidenceEntryConverter evidenceEntryConverter;

    private Map<String, String> genericMap = new HashMap<>();
    private List<String> genericList = new ArrayList<>();

    public VariantSearchToVariantConverter() {
        evidenceEntryConverter = new VariantTraitAssociationToEvidenceEntryConverter();
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
            variantSearchModel.getStudies().forEach(s -> {
                StudyEntry entry = new StudyEntry();
                entry.setStudyId(s);
                studies.add(entry);
                studyEntryMap.put(s, entry);
            });
            variant.setStudies(studies);
        }

        // File info management
        Map<String, FileEntry> fileEntryMap = new HashMap<>();
        if (MapUtils.isNotEmpty(variantSearchModel.getFileInfo())) {
            for (String key: variantSearchModel.getFileInfo().keySet()) {
                // key consists of 'fileInfo' + "__" + studyId + "__" + fileId
                String[] fields = key.split("__");
                FileEntry fileEntry = fileEntryMap.get(fields[2]);
                if (fileEntry == null) {
                    fileEntry = new FileEntry(fields[2], null, new HashMap<>());
                    fileEntryMap.put(fields[2], fileEntry);
                    variant.getStudy(fields[1]).getFiles().add(fileEntry);
                }
                Map<String, String> map = (Map<String, String>) jsonToObject(variantSearchModel.getFileInfo().get(key),
                        genericMap.getClass());
                if (MapUtils.isNotEmpty(map)) {
                    for (String infoName: map.keySet()) {
                        if ("fileCall".equals(infoName)) {
                            fileEntry.setCall(map.get(infoName));
                        } else {
                            fileEntry.getAttributes().put(infoName, map.get(infoName));
                        }
                    }
                }
            }
        }

        // Genotypes and sample data and format
        if (MapUtils.isNotEmpty(variantSearchModel.getSampleFormat())) {
            for (String studyId: studyEntryMap.keySet()) {
                StudyEntry studyEntry = studyEntryMap.get(studyId);

                // Sample names
                String json = variantSearchModel.getSampleFormat().get("sampleFormat__" + studyId + "__sampleName");
                List<String> sampleNames = (List<String>) jsonToObject(json, genericList.getClass());
                Map<String, Integer> samplePosition = new HashMap<>();
                for (int i = 0; i < sampleNames.size(); i++) {
                    samplePosition.put(sampleNames.get(i), i);
                }
                studyEntry.setSamplesPosition(samplePosition);

                // Format
                json = variantSearchModel.getSampleFormat().get("sampleFormat__" + studyId + "__format");
                List<String> formats = (List<String>) jsonToObject(json, genericList.getClass());
                studyEntry.setFormat(formats);

                // Sample data
                studyEntry.setSamplesData(new ArrayList());
                for (String sampleName: sampleNames) {
                    json = variantSearchModel.getSampleFormat().get("sampleFormat__" + studyId + "__" + sampleName);
                    List<String> sd = (List<String>) jsonToObject(json, genericList.getClass());
                    studyEntry.getSamplesData().add(sd);
                }
            }
        }

        // Stats management
        if (MapUtils.isNotEmpty(variantSearchModel.getStats())) {
            for (String key: variantSearchModel.getStats().keySet()) {
                // key consists of 'stats' + "__" + studyId + "__" + cohort
                String[] fields = key.split("__");
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
        VariantAnnotation variantAnnotation = new VariantAnnotation();

        // Set 'release' if it was not missing
        if (variantSearchModel.getRelease() > 0) {
            Map<String, String> attribute = new HashMap<>();
            attribute.put("release", String.valueOf(variantSearchModel.getRelease()));
            variantAnnotation.getAdditionalAttributes().put("opencga", new AdditionalAttribute(attribute));
        }

        // Xrefs
        List<String> hgvs = new ArrayList<>();
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
                if (xref.startsWith("hgvs:")) {
                    // HGVS are stored with the prefix 'hgvs:'
                    hgvs.add(xref.substring(5));
                }
            }
        }
        variantAnnotation.setHgvs(hgvs);
        variantAnnotation.setXrefs(xrefs);

        // consequence types
        String gene = null;
        String ensGene = null;
        Map<String, ConsequenceType> consequenceTypeMap = new HashMap<>();
        if (variantSearchModel.getGenes() != null) {
            for (int i = 0; i < variantSearchModel.getGenes().size(); i++) {
                if (!variantSearchModel.getGenes().get(i).startsWith("ENS")) {
                    gene = variantSearchModel.getGenes().get(i);
                }
                if (variantSearchModel.getGenes().get(i).startsWith("ENSG")) {
                    ensGene = variantSearchModel.getGenes().get(i);
                }
                if (variantSearchModel.getGenes().get(i).startsWith("ENST")) {
                    ConsequenceType consequenceType = new ConsequenceType();
                    consequenceType.setGeneName(gene);
                    consequenceType.setEnsemblGeneId(ensGene);
                    consequenceType.setEnsemblTranscriptId(variantSearchModel.getGenes().get(i));
                    // setProteinVariantAnnotation is postponed, since it will only be set if SO accession is 1583

                    // The key is the ENST id
                    consequenceTypeMap.put(variantSearchModel.getGenes().get(i), consequenceType);
                }
            }
        }

        // prepare protein substitution scores: sift and polyphen
        List<Score> scores;
        ProteinVariantAnnotation proteinAnnotation = new ProteinVariantAnnotation();
        if (!ArrayUtils.equals(variantSearchModel.getSift(), MISSING_VALUE)
                || !ArrayUtils.equals(variantSearchModel.getPolyphen(), MISSING_VALUE)) {
            scores = new ArrayList<>();
            if (!ArrayUtils.equals(variantSearchModel.getSift(), MISSING_VALUE)) {
                scores.add(new Score(variantSearchModel.getSift(), "sift", variantSearchModel.getSiftDesc()));
            }
            if (!ArrayUtils.equals(variantSearchModel.getPolyphen(), MISSING_VALUE)) {
                scores.add(new Score(variantSearchModel.getPolyphen(), "polyphen", variantSearchModel.getPolyphenDesc()));
            }
            proteinAnnotation.setSubstitutionScores(scores);
        }

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

                    // only set protein for that conseq. type if annotated protein and SO acc is 1583 (missense_variant)
                    if (soAcc == 1583) {
                        consequenceTypeMap.get(fields[0]).setProteinVariantAnnotation(proteinAnnotation);
                    }
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
                String[] fields = key.split("__");
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
                String[] fields = StringUtils.splitByWholeSeparatorPreserveAllTokens(trait, " -- ");
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

        // Set displayConsequenceType, hgvs, cytobands and repeats from 'other' field
        variantAnnotation.setHgvs(new ArrayList<>());
        variantAnnotation.setCytoband(new ArrayList<>());
        variantAnnotation.setRepeat(new ArrayList<>());
        for (String other : variantSearchModel.getOther()) {
            String[] fields = other.split(" -- ");
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
                default:
                    logger.warn("Unknown key in 'other' array in Solr: " + fields[0]);
                    break;
            }
        }

        // set variant annotation
        variant.setAnnotation(variantAnnotation);

        return variant;
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

        // Set Studies Alias
        if (variant.getStudies() != null && CollectionUtils.isNotEmpty(variant.getStudies())) {
            // Studies and stats
            List<String> studies = new ArrayList<>();
            Map<String, Float> stats = new HashMap<>();

            // FILTER, QUAL and file info
            Map<String, String> filter = new HashMap<>();
            Map<String, Float> qual = new HashMap<>();
            Map<String, String> fileInfo = new HashMap<>();

            // Genotypes and format
            Map<String, String> gt = new HashMap<>();
            Map<String, String> sampleFormat = new HashMap<>();

            for (StudyEntry studyEntry : variant.getStudies()) {
                String studyId = studyEntry.getStudyId();
                if (studyId.contains(":")) {
                    String[] split = studyId.split(":");
                    studyId = split[split.length - 1];
                }
                studies.add(studyId);

                // We store the cohort stats with the format stats_STUDY_COHORT = value, e.g. stats_1kg_phase3_ALL=0.02
                if (studyEntry.getStats() != null && studyEntry.getStats().size() > 0) {
                    Map<String, VariantStats> studyStats = studyEntry.getStats();
                    for (String key : studyStats.keySet()) {
                        stats.put("stats__" + studyId + "__" + key, studyStats.get(key).getAltAlleleFreq());
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

                            // In additon, store QUAL and FILTER separately
                            String value = fileEntry.getAttributes().get("QUAL");
                            if (StringUtils.isNotEmpty(value)) {
                                qual.put("qual__" + studyId + "__" + fileEntry.getFileId(), Float.parseFloat(value));
                            }

                            value = fileEntry.getAttributes().get("FILTER");
                            if (StringUtils.isNotEmpty(value)) {
                                filter.put("filter__" + studyId + "__" + fileEntry.getFileId(), value);
                            }
                        }
                        if (MapUtils.isNotEmpty(fileInfoMap)) {
                            String json = objectToJson(fileInfoMap);
                            if (StringUtils.isNotEmpty(json)) {
                                fileInfo.put("fileInfo__" + studyId + "__" + fileEntry.getFileId(), json);
                            }
                        }
                    }
                }

                // Samples, genotypes and format fields management
                if (MapUtils.isNotEmpty(studyEntry.getSamplesPosition())) {
                    List<String> sampleNames = studyEntry.getOrderedSamplesName();
                    if (ListUtils.isNotEmpty(sampleNames)) {
                        // Sanity check
                        if (ListUtils.isNotEmpty(studyEntry.getSamplesData())
                                && sampleNames.size() == studyEntry.getSamplesData().size()) {
                            // Save sample formats in a map (after, to JSON string), including sample names and GT
                            String json = objectToJson(sampleNames);
                            sampleFormat.put("sampleFormat__" + studyId + "__sampleName", json);
                            json = objectToJson(studyEntry.getFormat());
                            sampleFormat.put("sampleFormat__" + studyId + "__format", json);
                            for (int i = 0; i < sampleNames.size(); i++) {
                                // Save genotype where study and sample name as key
                                gt.put("gt__" + studyId + "__" + sampleNames.get(i),
                                        studyEntry.getSampleData(i).get(0));

                                // Save formats for each sample (after, to JSON string)
                                json = objectToJson(studyEntry.getSamplesData().get(i));
                                sampleFormat.put("sampleFormat__" + studyId + "__" + sampleNames.get(i), json);
                            }
                        } else {
                            logger.error("Mismatch sizes: please, check your sample names, sample data and format array");
                        }
                    }
                }
            }

            // Set studies and stats
            if (ListUtils.isNotEmpty(studies)) {
                variantSearchModel.setStudies(studies);
            }
            if (MapUtils.isNotEmpty(stats)) {
                variantSearchModel.setStats(stats);
            }
            // Set FILTER, QUAL and file info maps if not empty
            if (MapUtils.isNotEmpty(filter)) {
                variantSearchModel.setFilter(filter);
            }
            if (MapUtils.isNotEmpty(qual)) {
                variantSearchModel.setQual(qual);
            }
            if (MapUtils.isNotEmpty(fileInfo)) {
                variantSearchModel.setFileInfo(fileInfo);
            }
            // Set genotypes and format maps if not empty
            if (MapUtils.isNotEmpty(gt)) {
                variantSearchModel.setGt(gt);
            }
            if (MapUtils.isNotEmpty(sampleFormat)) {
                variantSearchModel.setSampleFormat(sampleFormat);
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
            if (variantAnnotation.getAdditionalAttributes() != null && variantAnnotation.getAdditionalAttributes().get("opencga") != null) {
                String releaseStr = variantAnnotation.getAdditionalAttributes().get("opencga").getAttribute().get("release");
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
            if (variantAnnotation.getHgvs() != null && !variantAnnotation.getHgvs().isEmpty()) {
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

                for (ConsequenceType consequenceType : consequenceTypes) {

                    // Set genes and biotypes if exist
                    if (StringUtils.isNotEmpty(consequenceType.getGeneName())) {
                        if (!genes.containsKey(consequenceType.getGeneName())) {
                            genes.put(consequenceType.getGeneName(), new LinkedHashSet<>());
                        }
                        // DO NOT change the order of the following code
                        genes.get(consequenceType.getGeneName()).add(consequenceType.getGeneName());
                        genes.get(consequenceType.getGeneName()).add(consequenceType.getEnsemblGeneId());
                        genes.get(consequenceType.getGeneName()).add(consequenceType.getEnsemblTranscriptId());

                        xrefs.add(consequenceType.getGeneName());
                        xrefs.add(consequenceType.getEnsemblGeneId());
                        xrefs.add(consequenceType.getEnsemblTranscriptId());

                        if (StringUtils.isNotEmpty(consequenceType.getBiotype())) {
                            biotypes.add(consequenceType.getBiotype());
                        }
                    }

                    // Remove 'SO:' prefix to Store SO Accessions as integers and also store the gene - SO acc relation
                    for (SequenceOntologyTerm sequenceOntologyTerm : consequenceType.getSequenceOntologyTerms()) {
                        int soNumber = Integer.parseInt(sequenceOntologyTerm.getAccession().substring(3));
                        soAccessions.add(soNumber);

                        if (StringUtils.isNotEmpty(consequenceType.getGeneName())) {
                            geneToSOAccessions.add(consequenceType.getGeneName() + "_" + soNumber);
                            geneToSOAccessions.add(consequenceType.getEnsemblGeneId() + "_" + soNumber);
                            geneToSOAccessions.add(consequenceType.getEnsemblTranscriptId() + "_" + soNumber);
                        }
                    }

                    if (consequenceType.getProteinVariantAnnotation() != null) {
                        ProteinVariantAnnotation proteinVariantAnnotation = consequenceType.getProteinVariantAnnotation();

                        // Add UniProt accession, name and ID to xrefs
                        if (StringUtils.isNotEmpty(proteinVariantAnnotation.getUniprotAccession())) {
                            xrefs.add(proteinVariantAnnotation.getUniprotAccession());
                        }
                        if (StringUtils.isNotEmpty(proteinVariantAnnotation.getUniprotName())) {
                            xrefs.add(proteinVariantAnnotation.getUniprotName());
                        }
                        if (StringUtils.isNotEmpty(proteinVariantAnnotation.getUniprotVariantId())) {
                            xrefs.add(proteinVariantAnnotation.getUniprotVariantId());
                        }

                        // Add keywords to and Features to traits
                        if (proteinVariantAnnotation.getKeywords() != null) {
                            for (String keyword : proteinVariantAnnotation.getKeywords()) {
                                traits.add("KW -- " + proteinVariantAnnotation.getUniprotAccession() + " -- " + keyword);
                            }
                        }

                        // Add protein domains
                        if (proteinVariantAnnotation.getFeatures() != null) {
                            for (ProteinFeature proteinFeature : proteinVariantAnnotation.getFeatures()) {
                                if (StringUtils.isNotEmpty(proteinFeature.getId())) {
                                    // We store them in xrefs and traits, the number of these IDs is very small
                                    xrefs.add(proteinFeature.getId());
                                    traits.add("PD -- " + proteinFeature.getId() + " -- " + proteinFeature.getDescription());
                                }
                            }
                        }
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
                    populationFrequencies.put("popFreq__" + populationFrequency.getStudy() + "__"
                            + populationFrequency.getPopulation(), populationFrequency.getAltAlleleFreq());
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
                                cv.getTraits().forEach(cvt -> traits.add("CV" + " -- " + cv.getAccession() + " -- " + cvt
                                        + " -- cs:" + cv.getClinicalSignificance()));
                            });
                }
                if (variantAnnotation.getVariantTraitAssociation().getCosmic() != null) {
                    variantAnnotation.getVariantTraitAssociation().getCosmic()
                            .forEach(cosmic -> {
                                xrefs.add(cosmic.getMutationId());
                                traits.add("CM -- " + cosmic.getMutationId() + " -- "
                                        + cosmic.getPrimaryHistology() + " -- " + cosmic.getHistologySubtype());
                            });
                }
            }
            if (variantAnnotation.getGeneTraitAssociation() != null
                    && CollectionUtils.isNotEmpty(variantAnnotation.getGeneTraitAssociation())) {
                for (GeneTraitAssociation geneTraitAssociation : variantAnnotation.getGeneTraitAssociation()) {
                    switch (geneTraitAssociation.getSource().toLowerCase()) {
                        case "hpo":
                            traits.add("HP -- " + geneTraitAssociation.getHpo() + " -- "
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
            List<String> other = new ArrayList<>();
            if (StringUtils.isNotEmpty(variantAnnotation.getDisplayConsequenceType())) {
                other.add("DCT -- " + variantAnnotation.getDisplayConsequenceType());
            }
            if (variantAnnotation.getHgvs() != null) {
                for (String hgvs : variantAnnotation.getHgvs()) {
                    other.add("HGVS -- " + hgvs);
                }
            }
            if (variantAnnotation.getCytoband() != null) {
                for (Cytoband cytoband : variantAnnotation.getCytoband()) {
                    other.add("CB -- " + cytoband.getName() + " -- " + cytoband.getStain()
                            + " -- " + cytoband.getStart() + " -- " + cytoband.getEnd());
                }
            }
            if (variantAnnotation.getRepeat() != null) {
                for (Repeat repeat : variantAnnotation.getRepeat()) {
                    other.add("RP -- " + repeat.getId() + " -- " + repeat.getSource() + " -- " + repeat.getCopyNumber()
                            + " -- " + repeat.getPercentageMatch() + " -- " + repeat.getStart() + " -- " + repeat.getEnd());
                }
            }
            variantSearchModel.setOther(other);
        }

        variantSearchModel.setXrefs(new ArrayList<>(xrefs));
        return variantSearchModel;
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

    public String objectToJson(Object obj) {
        ObjectWriter writer = new ObjectMapper().writer();
        try {
            return writer.writeValueAsString(obj);
        } catch (JsonProcessingException e) {
            logger.info("Error converting object to JSON format");
            return null;
        }
    }

    public Object jsonToObject(String json, Class objClass) {
        ObjectReader reader = new ObjectMapper().reader(objClass);
        try {
            return reader.readValue(json);
        } catch (IOException e) {
            logger.info("Error converting JSON format to object");
            return null;
        }
    }
}

