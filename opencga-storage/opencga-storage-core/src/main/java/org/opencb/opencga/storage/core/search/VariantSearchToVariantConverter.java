package org.opencb.opencga.storage.core.search;

import org.apache.commons.lang3.StringUtils;
import org.opencb.biodata.models.variant.StudyEntry;
import org.opencb.biodata.models.variant.Variant;
import org.opencb.biodata.models.variant.avro.*;
import org.opencb.biodata.models.variant.stats.VariantStats;
import org.opencb.commons.datastore.core.ComplexTypeConverter;

import java.util.*;

/**
 * Created by wasim on 14/11/16.
 */
public class VariantSearchToVariantConverter implements ComplexTypeConverter<Variant, VariantSearchModel> {

    /**
     * Conversion: from storage type to data model.
     *
     * @param variantSearchModel    Storage type object
     * @return                 Data model object
     */
    @Override
    public Variant convertToDataModelType(VariantSearchModel variantSearchModel) {
        Variant variant = new Variant();

        // set id, chromosome, start, end, dbSNP, type
        variant.setId(variantSearchModel.getId());
        variant.setChromosome(variantSearchModel.getChromosome());
        variant.setStart(variantSearchModel.getStart());
        variant.setEnd(variantSearchModel.getEnd());
        variant.setId(variantSearchModel.getDbSNP());
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
                // key is consists of "stats" + "_" + studyId + "_" + cohort
                String[] fields = key.split("_");
                if (studyEntryMap.containsKey(fields[1])) {
                    VariantStats variantStats = new VariantStats();
                    variantStats.setMaf(variantSearchModel.getStats().get(key));
                    studyEntryMap.get(fields[1]).setStats(fields[2], variantStats);
                } else {
                    System.out.println("Something wrong happened: stats " + key + ", but there is no study for that"
                            + " stats.");
                }
            }
        }

        // process annotation
        VariantAnnotation variantAnnotation = new VariantAnnotation();

        // consequence types
        List<ConsequenceType> consequenceTypes = new ArrayList<>();
//        String[] genes = (String[]) variantSearchModel.getGenes()
//                .toArray(new String[variantSearchModel.getGenes().size()]);
        String[] genes = variantSearchModel.getGenes().toArray(new String[variantSearchModel.getGenes().size()]);
        Map<String, ConsequenceType> consequenceTypeMap = new HashMap<>();
        for (int i = 0; i < genes.length; i += 3) {
            // genes are ordered: 1) gene name, 2) ensembl gene id, 3) ensembl transcript id, and then, repeat
            ConsequenceType consequenceType = new ConsequenceType();
            consequenceType.setGeneName(genes[i]);
            consequenceType.setEnsemblGeneId(genes[i + 1]);
            consequenceType.setEnsemblTranscriptId(genes[i + 2]);

            // for that consequence type, update the protein substitution scores: sift and polyphen
            ProteinVariantAnnotation proteinAnnotation = new ProteinVariantAnnotation();
            List<Score> scores = new ArrayList<>();
            scores.add(new Score(variantSearchModel.getSift(), "sift", ""));
            scores.add(new Score(variantSearchModel.getPolyphen(), "polyhen", ""));
            proteinAnnotation.setSubstitutionScores(scores);
            consequenceType.setProteinVariantAnnotation(proteinAnnotation);
        }
        // and finally, update the SO accession for each consequence type
        for (String geneToSoAcc: variantSearchModel.getGeneToSoAcc()) {
            String[] fields = geneToSoAcc.split("_");
            if (consequenceTypeMap.containsKey(fields[0])) {
                SequenceOntologyTerm sequenceOntologyTerm = new SequenceOntologyTerm();
                sequenceOntologyTerm.setAccession("SO:" + String.format("%07d", fields[1]));
                if (consequenceTypeMap.get(fields[0]).getSequenceOntologyTerms() == null) {
                    consequenceTypeMap.get(fields[0]).setSequenceOntologyTerms(new ArrayList<>());
                }
                consequenceTypeMap.get(fields[0]).getSequenceOntologyTerms().add(sequenceOntologyTerm);
            }
        }
        // and update the variant annotation with the consequence types
        variantAnnotation.setConsequenceTypes(consequenceTypes);

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

        // set conservations
        List<Score> scores = new ArrayList<>();
        scores.add(new Score(variantSearchModel.getGerp(), "gerp", ""));
        scores.add(new Score(variantSearchModel.getPhastCons(), "phastCons", ""));
        scores.add(new Score(variantSearchModel.getPhylop(), "phylop", ""));
        variantAnnotation.setConservation(scores);

        // set cadd
        scores.clear();
        scores.add(new Score(variantSearchModel.getCaddRaw(), "cadd_raw", ""));
        scores.add(new Score(variantSearchModel.getCaddScaled(), "cadd_scaled", ""));
        variantAnnotation.setFunctionalScore(scores);

        // set clinvar, cosmic, hpo
        Map<String, List<String>> clinVarMap = new HashMap<>();
        List<Cosmic> cosmicList = new ArrayList<>();
        List<GeneTraitAssociation> geneTraitAssociationList = new ArrayList<>();
        for (String trait: variantSearchModel.getTraits()) {
            String[] fields = trait.split(" -- ");
            switch (fields[0]) {
                case "ClinVar": {
                    // variant trait
                    // ClinVar -- accession -- trait
                    if (!clinVarMap.containsKey(fields[1])) {
                        clinVarMap.put(fields[1], new ArrayList<>());
                    }
                    clinVarMap.get(fields[1]).add(fields[2]);
                    break;
                }
                case "COSMIC": {
                    // variant trait
                    // COSMIC -- mutation id -- primary histology -- histology subtype
                    Cosmic cosmic = new Cosmic();
                    cosmic.setMutationId(fields[1]);
                    cosmic.setPrimaryHistology(fields[2]);
                    cosmic.setHistologySubtype(fields[3]);
                    cosmicList.add(cosmic);
                    break;
                }
                case "HP0": {
                    // gene trait
                    // HPO -- hpo -- name
                    GeneTraitAssociation geneTraitAssociation = new GeneTraitAssociation();
                    geneTraitAssociation.setHpo(fields[1]);
                    geneTraitAssociation.setName(fields[2]);
                    geneTraitAssociationList.add(geneTraitAssociation);
                    break;
                }
                default: {
                    System.out.println("Unknown trait type: " + fields[0] + ", it should be ClinVar, COSMIC or HPO");
                    break;
                }
            }
        }
        VariantTraitAssociation variantTraitAssociation = new VariantTraitAssociation();
        List<ClinVar> clinVarList = new ArrayList<>();
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
        variantSearchModel.setId(variant.getChromosome() + "_" + variant.getStart() + "_"
                + variant.getReference() + "_" + variant.getAlternate());
        variantSearchModel.setChromosome(variant.getChromosome());
        variantSearchModel.setStart(variant.getStart());
        variantSearchModel.setEnd(variant.getEnd());
        variantSearchModel.setDbSNP(variant.getId());
        variantSearchModel.setType(variant.getType().toString());

        // This field contains all possible IDs: id, dbSNP, genes, transcripts, protein, clinvar, hpo, ...
        // This will help when searching by variant id
        Set<String> xrefs = new HashSet<>();
        xrefs.add(variant.getChromosome() + ":" + variant.getStart() + ":" + variant.getReference() + ":"
                + variant.getAlternate());
        xrefs.add(variantSearchModel.getDbSNP());

        // Set Studies Alias
        if (variant.getStudies() != null && variant.getStudies().size() > 0) {
            List<String> studies = new ArrayList<>();
            Map<String, Float> stats = new HashMap<>();
//            variant.getStudies().forEach(s -> studies.add(s.getStudyId()));

            for (StudyEntry studyEntry : variant.getStudies()) {
                studies.add(studyEntry.getStudyId());

                // We store the cohort stats with the format stats_STUDY_COHORT = value, e.g. stats_1kg_phase3_ALL=0.02
                if (studyEntry.getStats() != null && studyEntry.getStats().size() > 0) {
                    studyEntry.getStats().forEach((s, variantStats) ->
                            stats.put("stats_" + studyEntry.getStudyId() + "_" + s, variantStats.getMaf()));
                }
            }

            variantSearchModel.setStudies(studies);
            variantSearchModel.setStats(stats);
        }

        // Check for annotation
        VariantAnnotation variantAnnotation = variant.getAnnotation();
        if (variantAnnotation != null) {

            // Set Genes and Consequence Types
            List<ConsequenceType> consequenceTypes = variantAnnotation.getConsequenceTypes();
            if (consequenceTypes != null) {
                List<String> genes = new ArrayList<>();
//                Set<String> genes = new LinkedHashSet<>();
                Set<Integer> soAccessions = new HashSet<>();
                Set<String> geneToSOAccessions = new LinkedHashSet<>();

                for (ConsequenceType consequenceType : consequenceTypes) {

                    // Set genes if exists
                    if (StringUtils.isNotEmpty(consequenceType.getGeneName())) {
                        genes.add(consequenceType.getGeneName());
                        genes.add(consequenceType.getEnsemblGeneId());
                        genes.add(consequenceType.getEnsemblTranscriptId());

                        xrefs.add(consequenceType.getGeneName());
                        xrefs.add(consequenceType.getEnsemblGeneId());
                        xrefs.add(consequenceType.getEnsemblTranscriptId());
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

                    // Set sift and polyphen and also the protein id in xrefs
                    if (consequenceType.getProteinVariantAnnotation() != null) {

                        // set protein substitution scores: sift and polyphen
                        double[] proteinScores = getSubstitutionScores(consequenceType);
                        variantSearchModel.setSift(proteinScores[0]);
                        variantSearchModel.setPolyphen(proteinScores[1]);

                        xrefs.add(consequenceType.getProteinVariantAnnotation().getUniprotAccession());
                    }
                }

                // We store the accumulated data
                variantSearchModel.setGenes(genes);
                variantSearchModel.setSoAcc(soAccessions);
                variantSearchModel.setGeneToSoAcc(geneToSOAccessions);

                // We accumulate genes in xrefs
                xrefs.addAll(genes);
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
                            System.out.println("Unknown 'conservation' source: score.getSource() = "
                                    + score.getSource());
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
                            System.out.println("Unknown 'functional score' source: score.getSource() = "
                                    + score.getSource());
                            break;
                    }
                }
            }

            // Set variant traits: ClinVar, Cosmic, HPO, ...
            Set<String> traits = new HashSet<>();
            if (variantAnnotation.getVariantTraitAssociation() != null) {
                if (variantAnnotation.getVariantTraitAssociation().getClinvar() != null) {
                    variantAnnotation.getVariantTraitAssociation().getClinvar()
                            .forEach(cv -> {
                                xrefs.add(cv.getAccession());
                                cv.getTraits().forEach(cvt -> traits.add("ClinVar" + " -- " + cv.getAccession() + " -- " + cvt));
                            });
                }
                if (variantAnnotation.getVariantTraitAssociation().getCosmic() != null) {
                    variantAnnotation.getVariantTraitAssociation().getCosmic()
                            .forEach(cosm -> {
                                xrefs.add(cosm.getMutationId());
                                traits.add("COSMIC -- " + cosm.getMutationId() + " -- "
                                        + cosm.getPrimaryHistology() + " " + cosm.getHistologySubtype());
                            });
                }
            }
            if (variantAnnotation.getGeneTraitAssociation() != null && variantAnnotation.getGeneTraitAssociation().size() > 0) {
                for (GeneTraitAssociation geneTraitAssociation : variantAnnotation.getGeneTraitAssociation()) {
                    if (geneTraitAssociation.getSource().equalsIgnoreCase("hpo")) {
                        traits.add("HPO -- " + geneTraitAssociation.getHpo() + " -- " + geneTraitAssociation.getName());
                    }
                }
            }
            variantSearchModel.setTraits(traits);
        }

        variantSearchModel.setXrefs(xrefs);
        return variantSearchModel;
    }


    @Deprecated
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
     * Retrieve the protein substitution scores from a consequence type annotation: sift or polyphen.
     *
     * @param consequenceType   Consequence type target
     * @return                  Max. and min. scores
     */
    private double[] getSubstitutionScores(ConsequenceType consequenceType) {
        double sift = 10;
        double polytphen = 0;

        if (consequenceType.getProteinVariantAnnotation().getSubstitutionScores() != null) {
            for (Score score : consequenceType.getProteinVariantAnnotation().getSubstitutionScores()) {
                String source = score.getSource();
                if (source.equals("sift")) {
                    if (score.getScore() < sift) {
                        sift = score.getScore();
                    }
                } else if (source.equals("polyphen")) {
                    if (score.getScore() > polytphen) {
                        polytphen = score.getScore();
                    }
                }
            }
        }

        double[] result = new double[] {sift, polytphen};
        return result;
    }
}

