package org.opencb.opencga.storage.core.search;

import org.apache.commons.lang3.StringUtils;
import org.opencb.biodata.models.variant.*;
import org.opencb.biodata.models.variant.StudyEntry;
import org.opencb.biodata.models.variant.avro.VariantTraitAssociation;
import org.opencb.biodata.models.variant.avro.*;
import org.opencb.biodata.models.variant.avro.ConsequenceType;
import org.opencb.biodata.models.variant.avro.Score;
import org.opencb.biodata.models.variant.avro.VariantAnnotation;
import org.opencb.commons.datastore.core.ComplexTypeConverter;

import java.util.*;

/**
 * Created by wasim on 14/11/16.
 */
public class VariantSearchToVariantConverter implements ComplexTypeConverter<Variant, VariantSearch> {

    /**
     * Conversion: from storage type to data model.
     *
     * @param variantSearch    Storage type object
     * @return                 Data model object
     */
    @Override
    public Variant convertToDataModelType(VariantSearch variantSearch) {
        Variant variant = new Variant();

        // set id, chromosome, start, end, dbSNP, type
        variant.setId(variantSearch.getId());
        variant.setChromosome(variantSearch.getChromosome());
        variant.setStart(variantSearch.getStart());
        variant.setEnd(variantSearch.getEnd());
        variant.setId(variantSearch.getDbSNP());
        variant.setType(VariantType.valueOf(variantSearch.getType()));

        // set studies
        if (variantSearch.getStudies() != null && variantSearch.getStudies().size() > 0) {
            List<StudyEntry> studies = new ArrayList<>();
            variantSearch.getStudies().forEach(s -> {
                StudyEntry entry = new StudyEntry();
                entry.setStudyId(s);
                studies.add(entry);
            });
            variant.setStudies(studies);
        }

        // process annotation
        VariantAnnotation variantAnnotation = new VariantAnnotation();

        // consequence types
        List<ConsequenceType> consequenceTypes = new ArrayList<>();

        // TODO: genes, SO Accession and set consequence types
        // set genes
        // set SO Accession
//        if (variantSearch.getGeneToSOAccessions() != null && variantSearch.getGeneToSOAccessions().size() > 0) {
//
//        }

        // set protein substitution scores: sift and polyphen
        ProteinVariantAnnotation proteinAnnotation = new ProteinVariantAnnotation();
        List<Score> scores = new ArrayList<>();
        scores.add(new Score(variantSearch.getSift(), "sift", ""));
        scores.add(new Score(variantSearch.getPolyphen(), "polyhen", ""));
        proteinAnnotation.setSubstitutionScores(scores);

        // set consequence types
        variantAnnotation.setConsequenceTypes(consequenceTypes);

        // set populations
        if (variantSearch.getPopFreq() != null && variantSearch.getPopFreq().size() > 0) {
            List<PopulationFrequency> populationFrequencies = new ArrayList<>();
            for (String key : variantSearch.getPopFreq().keySet()) {
                PopulationFrequency populationFrequency = new PopulationFrequency();
                String[] fields = key.split(",");
                populationFrequency.setStudy(fields[1]);
                populationFrequency.setPopulation(fields[2]);
                populationFrequency.setAltAlleleFreq(variantSearch.getPopFreq().get(key));
                populationFrequencies.add(populationFrequency);
            }
            variantAnnotation.setPopulationFrequencies(populationFrequencies);
        }

        // set conservations
        scores.clear();
        scores.add(new Score(variantSearch.getGerp(), "gerp", ""));
        scores.add(new Score(variantSearch.getPhastCons(), "phastCons", ""));
        scores.add(new Score(variantSearch.getPhylop(), "phylop", ""));
        variantAnnotation.setConservation(scores);

        // set cadd
        scores.clear();
        scores.add(new Score(variantSearch.getCaddRaw(), "cadd_raw", ""));
        scores.add(new Score(variantSearch.getCaddScaled(), "cadd_scaled", ""));
        variantAnnotation.setFunctionalScore(scores);

        // TODO: clinvar, a clinvar accession might have several traits !!!
//        if (variantAnnotation.getVariantTraitAssociation() != null
//                && variantAnnotation.getVariantTraitAssociation().getClinvar() != null) {
//            Set<String> clinvar = new HashSet<>();
//            variantAnnotation.getVariantTraitAssociation().getClinvar()
//                    .forEach(cv -> {
//                        clinvar.add(cv.getAccession());
//                        cv.getTraits().forEach(cvt -> clinvar.add(cvt));
//                    });
//            variantSearch.setClinvar(clinvar);
//        }
        // set clinvar
        VariantTraitAssociation traitAssociation = new VariantTraitAssociation();
        List<ClinVar> clinVars = new ArrayList<>();
        traitAssociation.setClinvar(clinVars);
        variantAnnotation.setVariantTraitAssociation(traitAssociation);


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
    public VariantSearch convertToStorageType(Variant variant) {
        VariantSearch variantSearch = new VariantSearch();

        // Set general Variant attributes: id, dbSNP, chromosome, start, end, type
        variantSearch.setId(variant.getChromosome() + "_" + variant.getStart() + "_"
                + variant.getReference() + "_" + variant.getAlternate());
        variantSearch.setChromosome(variant.getChromosome());
        variantSearch.setStart(variant.getStart());
        variantSearch.setEnd(variant.getEnd());
        variantSearch.setDbSNP(variant.getId());
        variantSearch.setType(variant.getType().toString());

        // This field contains all possible IDs: id, dbSNP, genes, transcripts, protein, clinvar, hpo, ...
        // This will help when searching by variant id
        Set<String> xrefs = new HashSet<>();
        xrefs.add(variant.getChromosome() + ":" + variant.getStart() + ":" + variant.getReference() + ":" + variant.getAlternate());
        xrefs.add(variantSearch.getDbSNP());

        // Set Studies Alias
        if (variant.getStudies() != null && variant.getStudies().size() > 0) {
            List<String> studies = new ArrayList<>();
            variant.getStudies().forEach(s -> studies.add(s.getStudyId()));
            variantSearch.setStudies(studies);
        }

        // Check for annotation
        VariantAnnotation variantAnnotation = variant.getAnnotation();
        if (variantAnnotation != null) {

            // Set Genes and Consequence Types
            List<ConsequenceType> consequenceTypes = variantAnnotation.getConsequenceTypes();
            if (consequenceTypes != null) {
                Set<String> genes = new HashSet<>();
                Set<Integer> soAccessions = new HashSet<>();
                Set<String> geneToSOAccessions = new HashSet<>();

                for (ConsequenceType consequenceType : consequenceTypes) {

                    // Set genes if exists
                    if (StringUtils.isNotEmpty(consequenceType.getGeneName())) {
                        genes.add(consequenceType.getGeneName());

                        xrefs.add(consequenceType.getGeneName());
                        xrefs.add(consequenceType.getEnsemblGeneId());
                        xrefs.add(consequenceType.getEnsemblTranscriptId());
                    }

                    // Remove 'SO:' prefix to Store SO Accessions as integers and also store the relation between genes and SO accessions
                    for (SequenceOntologyTerm sequenceOntologyTerm : consequenceType.getSequenceOntologyTerms()) {
                        int soNumber = Integer.parseInt(sequenceOntologyTerm.getAccession().substring(3));
                        soAccessions.add(soNumber);

                        if (StringUtils.isNotEmpty(consequenceType.getGeneName())) {
                            geneToSOAccessions.add(consequenceType.getGeneName() + "_" + soNumber);
                        }
                    }

                    // Set sift and polyphen and also the protein id in xrefs
                    if (consequenceType.getProteinVariantAnnotation() != null) {

                        // set protein substitution scores: sift and polyphen
                        double[] proteinScores = getSubstitutionScores(consequenceType);
                        variantSearch.setSift(proteinScores[0]);
                        variantSearch.setPolyphen(proteinScores[1]);

                        xrefs.add(consequenceType.getProteinVariantAnnotation().getUniprotAccession());
                    }
                }

                // We store the accumulated data
                variantSearch.setGenes(genes);
                variantSearch.setSoAcc(soAccessions);
                variantSearch.setGeneToSoAcc(geneToSOAccessions);

                // We accumulate genes in xrefs
                xrefs.addAll(genes);
            }

            // Set Populations frequencies
            if (variantAnnotation.getPopulationFrequencies() != null) {
                Map<String, Float> populationFrequencies = new HashMap<>();
                for (PopulationFrequency populationFrequency : variantAnnotation.getPopulationFrequencies()) {
                    populationFrequencies.put("popFreq_" + populationFrequency.getStudy() + "_" + populationFrequency.getPopulation(),
                            populationFrequency.getAltAlleleFreq());
                }
                if (!populationFrequencies.isEmpty()) {
                    variantSearch.setPopFreq(populationFrequencies);
                }
            }

            // Set Conservation scores
            if (variantAnnotation.getConservation() != null) {
                for (Score score : variantAnnotation.getConservation()) {
                    switch (score.getSource()) {
                        case "phastCons":
                            variantSearch.setPhastCons(score.getScore());
                            break;
                        case "phylop":
                            variantSearch.setPhylop(score.getScore());
                            break;
                        case "gerp":
                            variantSearch.setGerp(score.getScore());
                            break;
                        default:
                            System.out.println("Unknown 'conservation' source: score.getSource() = " + score.getSource());
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
                            variantSearch.setCaddRaw(score.getScore());
                            break;
                        case "cadd_scaled":
                        case "caddScaled":
                            variantSearch.setCaddScaled(score.getScore());
                            break;
                        default:
                            System.out.println("Unknown 'functional score' source: score.getSource() = " + score.getSource());
                            break;
                    }
                }
            }

            // Set ClinVar
            if (variantAnnotation.getVariantTraitAssociation() != null
                    && variantAnnotation.getVariantTraitAssociation().getClinvar() != null) {
                Set<String> clinvar = new HashSet<>();
                variantAnnotation.getVariantTraitAssociation().getClinvar()
                        .forEach(cv -> {
                            clinvar.add(cv.getAccession());
                            cv.getTraits().forEach(cvt -> clinvar.add(cvt));
                        });
                variantSearch.setClinvar(clinvar);

                xrefs.addAll(clinvar);
            }
        }

        variantSearch.setXrefs(xrefs);
        return variantSearch;
    }


    @Deprecated
    public List<VariantSearch> convertListToStorageType(List<Variant> variants) {
        List<VariantSearch> variantSearchList = new ArrayList<>(variants.size());
        for (Variant variant: variants) {
            VariantSearch variantSearch = convertToStorageType(variant);
            if (variantSearch.getId() != null) {
                variantSearchList.add(variantSearch);
            }
        }
        return variantSearchList;
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

