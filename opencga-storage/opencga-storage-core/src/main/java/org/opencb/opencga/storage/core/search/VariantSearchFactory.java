package org.opencb.opencga.storage.core.search;

import org.opencb.biodata.models.variant.Variant;
import org.opencb.biodata.models.variant.avro.*;

import java.util.*;

/**
 * Created by wasim on 14/11/16.
 */
public class VariantSearchFactory {


    public static List<VariantSearch> create(List<Variant> variants) {
        List<VariantSearch> variantSearchList = new ArrayList<>(variants.size());
        for (Variant variant: variants) {
            VariantSearch variantSearch = create(variant);
            if (variantSearch.getId() != null) {
                variantSearchList.add(variantSearch);
            }
        }
        return variantSearchList;
    }

    public static VariantSearch create(Variant variant) {

        VariantSearch variantSearch = new VariantSearch();

        variantSearch.setId(variant.getChromosome() + "_" + variant.getStart() + "_"
                + variant.getReference() + "_" + variant.getAlternate());
        variantSearch.setChromosome(variant.getChromosome());
        variantSearch.setStart(variant.getStart());
        variantSearch.setEnd(variant.getEnd());
        variantSearch.setDbSNP(variant.getId());
        variantSearch.setType(variant.getType().toString());

        //TODO get clear with Nacho what to put in studies
//        variantSearch.setStudies(variant.getStudies());

        VariantAnnotation variantAnnotation = variant.getAnnotation();
        if (variantAnnotation != null) {
            List<ConsequenceType> consequenceTypes = variantAnnotation.getConsequenceTypes();

            if (consequenceTypes != null) {
                Set<String> genes = new HashSet<>();
                Set<String> accessipns = new HashSet<>();
//                Map<String, List<String>> geneToConsequenceType = new HashMap<>();
                for (ConsequenceType consequenceType : consequenceTypes) {

//                    variantSearch.setGenes(consequenceType.getGeneName());
                    genes.add(consequenceType.getGeneName());
//                    variantSearch.getGeneToConsequenceType().put(consequenceType.getGeneName(), new ArrayList<>());

                    //substitutionScores
                    List<Double> proteinScores = getSubstitutionScores(consequenceType);
                    variantSearch.setSift(proteinScores.get(0));
                    variantSearch.setPolyphen(proteinScores.get(1));
                    // Accession
                    for (SequenceOntologyTerm sequenceOntologyTerm : consequenceType.getSequenceOntologyTerms()) {
//                        variantSearch.setAccessions(sequenceOntologyTerm.getAccession());
                        accessipns.add(sequenceOntologyTerm.getAccession());
//                        variantSearch.getGeneToConsequenceType().get(consequenceType.getGeneName())
// .add(sequenceOntologyTerm.getAccession());

                        // TODO: gene to consequence type support !
//                        variantSearch.getGeneToConsequenceType().put("genect_" + consequenceType.getGeneName(),
//                                sequenceOntologyTerm.getAccession());
                    }
                }
                variantSearch.setGenes(genes);
                variantSearch.setAccessions(accessipns);
//                variantSearch.setGeneToConsequenceType(geneToConsequenceType);
            }

            if (variantAnnotation.getPopulationFrequencies() != null) {
                for (PopulationFrequency populationFrequency : variantAnnotation.getPopulationFrequencies()) {
                    Map<String, Float> population = new HashMap<>();
                    population.put("study_" + populationFrequency.getStudy() + "_"
                            + populationFrequency.getPopulation(),
                            populationFrequency.getAltAlleleFreq());
                    variantSearch.setPopulations(population);

                }
            }

            // conservations
            if (variantAnnotation.getConservation() != null) {
                for (Score score : variantAnnotation.getConservation()) {
                    if ("gerp".equals(score.getSource())) {
                        variantSearch.setGerp(score.getScore());
                    } else if ("phastCons".equals(score.getSource())) {
                        variantSearch.setPhastCons(score.getScore());
                    } else if ("phylop".equals(score.getSource())) {
                        variantSearch.setPhylop(score.getScore());
                    }
                }
            }

            //cadd
            if (variantAnnotation.getFunctionalScore() != null) {
                for (Score score : variantAnnotation.getFunctionalScore()) {
                    if ("cadd_raw".equals(score.getSource())) {
                        variantSearch.setCaddRaw(score.getScore());
                    } else if ("cadd_scaled".equals(score.getSource())) {
                        variantSearch.setCaddScaled(score.getScore());
                    }
                }
            }
        }
        return variantSearch;
    }

    private static List<Double> getSubstitutionScores(ConsequenceType consequenceType) {

        double min = 10;
        double max = 0;

        if (consequenceType.getProteinVariantAnnotation() != null
                && consequenceType.getProteinVariantAnnotation().getSubstitutionScores() != null) {

            for (Score score : consequenceType.getProteinVariantAnnotation().getSubstitutionScores()) {
                String s = score.getSource();
                if (s.equals("sift")) {
                    if (score.getScore() < min) {
                        min = score.getScore();
                    }
                } else if (s.equals("polyphen")) {
                    if (score.getScore() > max) {
                        max = score.getScore();
                    }
                }
            }
        }

        // Always Two values : First value min and second max
        List<Double> result = new ArrayList<>(2);
        result.add(min);
        result.add(max);

        return result;
    }
}

