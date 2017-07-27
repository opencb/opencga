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

package org.opencb.opencga.storage.core.search;

import org.apache.commons.lang3.StringUtils;
import org.opencb.biodata.models.variant.StudyEntry;
import org.opencb.biodata.models.variant.Variant;
import org.opencb.biodata.models.variant.annotation.ConsequenceTypeMappings;
import org.opencb.biodata.models.variant.avro.*;
import org.opencb.biodata.models.variant.stats.VariantStats;
import org.opencb.commons.datastore.core.ComplexTypeConverter;
import org.opencb.opencga.storage.core.search.solr.VariantSearchManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

/**
 * Created by wasim on 14/11/16.
 */
public class VariantSearchToVariantConverter implements ComplexTypeConverter<Variant, VariantSearchModel> {

    public static final double MISSING_VALUE = -100.0;
    private Logger logger = LoggerFactory.getLogger(VariantSearchManager.class);

    /**
     * Conversion: from storage type to data model.
     *
     * @param variantSearchModel    Storage type object
     * @return                 Data model object
     */
    @Override
    public Variant convertToDataModelType(VariantSearchModel variantSearchModel) {
        // set chromosome, start, end, ref, alt from ID
        Variant variant = new Variant(variantSearchModel.getId());

        // set ID, chromosome, start, end, ref, alt, type
        variant.setId(variantSearchModel.getVariantId());

        // set variant type
        variant.setType(VariantType.valueOf(variantSearchModel.getType()));

        // set studies and stats
        Map<String, StudyEntry> studyEntryMap = new HashMap<>();
        if (variantSearchModel.getStudies() != null && variantSearchModel.getStudies().size() > 0) {
            List<StudyEntry> studies = new ArrayList<>();
            variantSearchModel.getStudies().forEach(s -> {
                StudyEntry entry = new StudyEntry();
                entry.setStudyId(s);
                studies.add(entry);
                studyEntryMap.put(s, entry);
            });
            variant.setStudies(studies);
        }
        if (variantSearchModel.getStats() != null && variantSearchModel.getStats().size() > 0) {
            for (String key: variantSearchModel.getStats().keySet()) {
                // key consists of 'stats' + "__" + studyId + "__" + cohort
                String[] fields = key.split("__");
                if (fields[1].contains("_")) {
                    String[] split = fields[1].split("_");
                    fields[1] = split[split.length - 1];
                }
                if (studyEntryMap.containsKey(fields[1])) {
                    VariantStats variantStats = new VariantStats();
                    variantStats.setMaf(variantSearchModel.getStats().get(key));

                    studyEntryMap.get(fields[1]).setStats(fields[2], variantStats);
                }
//                else {
//                    System.out.println("Something wrong happened: stats " + key + ", but there is no study for that stats.");
//                }
            }
        }

        // process annotation
        VariantAnnotation variantAnnotation = new VariantAnnotation();

        // consequence types
        String gene = null;
        String ensGene = null;
        Map<String, ConsequenceType> consequenceTypeMap = new HashMap<>();
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

        // prepare protein substitution scores: sift and polyphen
        List<Score> scores;
        ProteinVariantAnnotation proteinAnnotation = new ProteinVariantAnnotation();
        if (variantSearchModel.getSift() != MISSING_VALUE || variantSearchModel.getPolyphen() != MISSING_VALUE) {
            scores = new ArrayList<>();
            if (variantSearchModel.getSift() != MISSING_VALUE) {
                scores.add(new Score(variantSearchModel.getSift(), "sift", variantSearchModel.getSiftDesc()));
            }
            if (variantSearchModel.getPolyphen() != MISSING_VALUE) {
                scores.add(new Score(variantSearchModel.getPolyphen(), "polyphen", variantSearchModel.getPolyphenDesc()));
            }
            proteinAnnotation.setSubstitutionScores(scores);
        }

        // and finally, update the SO accession for each consequence type
        // and setProteinVariantAnnotation if SO accession is 1583
        for (String geneToSoAcc: variantSearchModel.getGeneToSoAcc()) {
            String[] fields = geneToSoAcc.split("_");
            if (consequenceTypeMap.containsKey(fields[0])) {
                int soAcc = Integer.parseInt(fields[1]);
                SequenceOntologyTerm sequenceOntologyTerm = new SequenceOntologyTerm();
                sequenceOntologyTerm.setAccession("SO:" + String.format("%07d", soAcc));
                sequenceOntologyTerm.setName(ConsequenceTypeMappings.accessionToTerm.get(soAcc));
                if (consequenceTypeMap.get(fields[0]).getSequenceOntologyTerms() == null) {
                    consequenceTypeMap.get(fields[0]).setSequenceOntologyTerms(new ArrayList<>());
                }
                consequenceTypeMap.get(fields[0]).getSequenceOntologyTerms().add(sequenceOntologyTerm);

                // only set protein for that consequence type
                // if annotated protein and SO accession is 1583 (missense_variant)
                if (soAcc == 1583) {
                    consequenceTypeMap.get(fields[0]).setProteinVariantAnnotation(proteinAnnotation);
                }
            }
        }
        // and update the variant annotation with the consequence types
        variantAnnotation.setConsequenceTypes(new ArrayList<>(consequenceTypeMap.values()));

        // set populations
        if (variantSearchModel.getPopFreq() != null && variantSearchModel.getPopFreq().size() > 0) {
            List<PopulationFrequency> populationFrequencies = new ArrayList<>();
            for (String key : variantSearchModel.getPopFreq().keySet()) {
                PopulationFrequency populationFrequency = new PopulationFrequency();
                String[] fields = key.split("__");
                populationFrequency.setStudy(fields[1]);
                populationFrequency.setPopulation(fields[2]);
                populationFrequency.setAltAlleleFreq(variantSearchModel.getPopFreq().get(key));
                populationFrequencies.add(populationFrequency);
            }
            variantAnnotation.setPopulationFrequencies(populationFrequencies);
        }

        // Set conservations scores
        scores = new ArrayList<>();
        if (variantSearchModel.getPhylop() != MISSING_VALUE) {
            scores.add(new Score(variantSearchModel.getPhylop(), "phylop", ""));
        }
        if (variantSearchModel.getPhastCons() != MISSING_VALUE) {
            scores.add(new Score(variantSearchModel.getPhastCons(), "phastCons", ""));
        }
        if (variantSearchModel.getGerp() != MISSING_VALUE) {
            scores.add(new Score(variantSearchModel.getGerp(), "gerp", ""));
        }
        variantAnnotation.setConservation(scores);

        // Set CADD scores
        scores = new ArrayList<>();
        if (variantSearchModel.getCaddRaw() != MISSING_VALUE) {
            scores.add(new Score(variantSearchModel.getCaddRaw(), "cadd_raw", ""));
        }
        if (variantSearchModel.getCaddScaled() != MISSING_VALUE) {
            scores.add(new Score(variantSearchModel.getCaddScaled(), "cadd_scaled", ""));
        }
        variantAnnotation.setFunctionalScore(scores);

        // set HPO, ClinVar and Cosmic
        Map<String, List<String>> clinVarMap = new HashMap<>();
        List<Cosmic> cosmicList = new ArrayList<>();
        List<GeneTraitAssociation> geneTraitAssociationList = new ArrayList<>();
        if (variantSearchModel.getTraits() != null) {
            for (String trait : variantSearchModel.getTraits()) {
                String[] fields = trait.split(" -- ");
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
                            clinVarMap.put(fields[1], new ArrayList<>());
                        }
                        clinVarMap.get(fields[1]).add(fields[2]);
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
                        //TODO: Parse Keyword
                        break;
                    case "PD":
                        //TODO: Parse Protein Feature
                        break;
                    default: {
                        logger.warn("Unknown trait type: " + fields[0] + ", it should be HPO, ClinVar or Cosmic");
                        break;
                    }
                }
            }
        }
        VariantTraitAssociation variantTraitAssociation = new VariantTraitAssociation();
        List<ClinVar> clinVarList = new ArrayList<>(clinVarMap.size());
        for (String key: clinVarMap.keySet()) {
            ClinVar clinVar = new ClinVar();
            clinVar.setAccession(key);
            clinVar.setTraits(clinVarMap.get(key));
            clinVarList.add(clinVar);
        }
        if (clinVarList.size() > 0 || cosmicList.size() > 0) {
            if (clinVarList.size() > 0) {
                variantTraitAssociation.setClinvar(clinVarList);
            }
            if (cosmicList.size() > 0) {
                variantTraitAssociation.setCosmic(cosmicList);
            }
            variantAnnotation.setVariantTraitAssociation(variantTraitAssociation);
        }
        if (geneTraitAssociationList.size() > 0) {
            variantAnnotation.setGeneTraitAssociation(geneTraitAssociationList);
        }

        // set variant annotation
        variant.setAnnotation(variantAnnotation);

        return variant;
    }

    /**
     * Conversion: from data model to storage type.
     *
     * @param variant   Data model object
     * @return          Storage type object
     */
    @Override
    public VariantSearchModel convertToStorageType(Variant variant) {
        VariantSearchModel variantSearchModel = new VariantSearchModel();

        // Set general Variant attributes: id, dbSNP, chromosome, start, end, type
        variantSearchModel.setId(variant.getChromosome() + ":" + variant.getStart() + ":"
                + variant.getReference() + ":" + variant.getAlternate());
        variantSearchModel.setChromosome(variant.getChromosome());
        variantSearchModel.setStart(variant.getStart());
        variantSearchModel.setEnd(variant.getEnd());
        variantSearchModel.setVariantId(variant.getId());
        variantSearchModel.setType(variant.getType().toString());

        // This field contains all possible IDs: id, dbSNP, genes, transcripts, protein, clinvar, hpo, ...
        // This will help when searching by variant id. This is added at the end of the method after collecting all IDs
        Set<String> xrefs = new HashSet<>();
        xrefs.add(variant.getChromosome() + ":" + variant.getStart() + ":" + variant.getReference() + ":" + variant.getAlternate());
        xrefs.add(variantSearchModel.getVariantId());

        // Set Studies Alias
        if (variant.getStudies() != null && variant.getStudies().size() > 0) {
            List<String> studies = new ArrayList<>();
            Map<String, Float> stats = new HashMap<>();
//            variant.getStudies().forEach(s -> studies.add(s.getStudyId()));

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
                    for (String key: studyStats.keySet()) {
                        stats.put("stats__" + studyId + "__" + key, studyStats.get(key).getMaf());
                    }
                }
            }

            variantSearchModel.setStudies(studies);
            variantSearchModel.setStats(stats);
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

            // This object store all info and descriptions for full-text search
            Set<String> traits = new HashSet<>();

            // Set Genes and Consequence Types
            List<ConsequenceType> consequenceTypes = variantAnnotation.getConsequenceTypes();
            if (consequenceTypes != null) {
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

                    // Remove 'SO:' prefix to Store SO Accessions as integers and also store the relation
                    // between genes and SO accessions
                    for (SequenceOntologyTerm sequenceOntologyTerm : consequenceType.getSequenceOntologyTerms()) {
                        int soNumber = Integer.parseInt(sequenceOntologyTerm.getAccession().substring(3));
                        soAccessions.add(soNumber);

                        if (StringUtils.isNotEmpty(consequenceType.getGeneName())) {
                            geneToSOAccessions.add(consequenceType.getGeneName() + "_" + soNumber);
                            geneToSOAccessions.add(consequenceType.getEnsemblGeneId() + "_" + soNumber);
                            geneToSOAccessions.add(consequenceType.getEnsemblTranscriptId() + "_" + soNumber);
                        }
                    }

                    // Set Uniprot accession protein id in xrefs
                    if (consequenceType.getProteinVariantAnnotation() != null) {
                        xrefs.add(consequenceType.getProteinVariantAnnotation().getUniprotAccession());

                        if (consequenceType.getProteinVariantAnnotation().getKeywords() != null) {
                            for (String keyword : consequenceType.getProteinVariantAnnotation().getKeywords()) {
                                traits.add("KW -- " + consequenceType.getProteinVariantAnnotation().getUniprotAccession()
                                        + " -- " + keyword);
                            }
                        }

                        if (consequenceType.getProteinVariantAnnotation().getFeatures() != null) {
                            for (ProteinFeature proteinFeature : consequenceType.getProteinVariantAnnotation().getFeatures()) {
                                traits.add("PD -- " + proteinFeature.getId() + " -- " + proteinFeature.getDescription());
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
                                cv.getTraits().forEach(cvt -> traits.add("CV" + " -- " + cv.getAccession() + " -- " + cvt));
                            });
                }
                if (variantAnnotation.getVariantTraitAssociation().getCosmic() != null) {
                    variantAnnotation.getVariantTraitAssociation().getCosmic()
                            .forEach(cosm -> {
                                xrefs.add(cosm.getMutationId());
                                traits.add("CM -- " + cosm.getMutationId() + " -- "
                                        + cosm.getPrimaryHistology() + " -- " + cosm.getHistologySubtype());
                            });
                }
            }
            if (variantAnnotation.getGeneTraitAssociation() != null && variantAnnotation.getGeneTraitAssociation().size() > 0) {
                for (GeneTraitAssociation geneTraitAssociation : variantAnnotation.getGeneTraitAssociation()) {
                    switch (geneTraitAssociation.getSource().toLowerCase()) {
                        case "hpo":
//                            xrefs.add(geneTraitAssociation.getHpo());
//                            xrefs.add(geneTraitAssociation.getId());
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
        }

        variantSearchModel.setXrefs(new ArrayList<>(xrefs));
        return variantSearchModel;
    }

    public List<VariantSearchModel> convertListToStorageType(List<Variant> variants) {
        List<VariantSearchModel> variantSearchModelList = new ArrayList<>(variants.size());
        for (Variant variant: variants) {
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
     * @param consequenceTypes    List of consequence type target
     * @param variantSearchModel  Variant search model to update
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
                        if (source.equals("sift")) {
                            if (score.getScore() < sift) {
                                sift = score.getScore();
                                siftDesc = score.getDescription();
                            }
                        } else if (source.equals("polyphen")) {
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
        if (sift == 10) {
            sift = MISSING_VALUE;
        }

        // set scores
        variantSearchModel.setSift(sift);
        variantSearchModel.setPolyphen(polyphen);

        // set descriptions
        variantSearchModel.setSiftDesc(siftDesc);
        variantSearchModel.setPolyphenDesc(polyphenDesc);
    }
}

