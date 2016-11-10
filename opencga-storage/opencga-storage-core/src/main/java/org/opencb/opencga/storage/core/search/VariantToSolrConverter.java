package org.opencb.opencga.storage.core.search;

import org.opencb.biodata.models.variant.Variant;
import org.opencb.biodata.models.variant.avro.*;
import org.opencb.commons.datastore.core.ComplexTypeConverter;

import java.util.*;

/**
 * Created by wasim on 09/11/16.
 */
public class VariantToSolrConverter implements ComplexTypeConverter<Variant, VariantSolr> {

    @Override
    public Variant convertToDataModelType(VariantSolr variantSolr) {
        // not supported
        return null;
    }

    @Override
    public VariantSolr convertToStorageType(Variant variant) {

        VariantSolr variantSolr = new VariantSolr();

        variantSolr.setId(variant.getId());
        variantSolr.setType(variant.getType().toString());
        variantSolr.setChromosome(variant.getChromosome());
        variantSolr.setStart(variant.getStart());
        variantSolr.setEnd(variant.getEnd());

        VariantAnnotation variantAnnotation = variant.getAnnotation();

        if (variantAnnotation != null) {

            List<ConsequenceType> consequenceTypes = variantAnnotation.getConsequenceTypes();

            if (consequenceTypes != null) {
                for (ConsequenceType consequenceType : consequenceTypes) {

                    variantSolr.setGeneNames(consequenceType.getGeneName());
                    //substitutionScores
                    List<Double> proteinScores = getsubstitutionScores(consequenceType);
                    variantSolr.setSift(proteinScores.get(0));
                    variantSolr.setPolyphen(proteinScores.get(1));
                    // Accession
                    for (SequenceOntologyTerm sequenceOntologyTerm : consequenceType.getSequenceOntologyTerms()) {
                        variantSolr.setAccessions(sequenceOntologyTerm.getAccession());
                    }
                }
            }
            if (variantAnnotation.getPopulationFrequencies() != null) {
                for (PopulationFrequency populationFrequency : variantAnnotation.getPopulationFrequencies()) {
                    Map<String, Float> population = new HashMap<String, Float>();
                    population.put("study_" + populationFrequency.getStudy() + "_" + populationFrequency.getPopulation(),
                            populationFrequency.getAltAlleleFreq());
                    variantSolr.setPopulations(population);

                }
            }

            // conservations
            if (variantAnnotation.getConservation() != null) {
                for (Score score : variantAnnotation.getConservation()) {
                    if ("grep".equals(score.getSource())) {
                        variantSolr.setGerp((Double) score.getScore());
                    } else if ("phastCons".equals(score.getSource())) {
                        variantSolr.setPhastCons(score.getScore());
                    } else if ("phylop".equals(score.getSource())) {
                        variantSolr.setPhylop(score.getScore());
                    }
                }
            }

            //cadd
            if (variantAnnotation.getFunctionalScore() != null) {
                for (Score score : variantAnnotation.getFunctionalScore()) {
                    if ("cadd_raw".equals(score.getSource())) {
                        variantSolr.setCaddRaw(score.getScore());
                    } else if ("cadd_scaled".equals(score.getSource())) {
                        variantSolr.setCaddScaled(score.getScore());
                    }
                }
            }
        }
        return variantSolr;
    }

    private List<Double> getsubstitutionScores(ConsequenceType consequenceType) {

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
        List<Double> result = new ArrayList<Double>(2);
        result.add(min);
        result.add(max);

        return result;
    }
}

