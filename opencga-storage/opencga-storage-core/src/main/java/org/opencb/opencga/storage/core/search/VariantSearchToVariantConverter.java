package org.opencb.opencga.storage.core.search;

import org.opencb.biodata.models.variant.*;
import org.opencb.biodata.models.variant.StudyEntry;
import org.opencb.biodata.models.variant.avro.*;
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
        boolean annotation = false;
        VariantAnnotation variantAnnotation = new VariantAnnotation();

        // TODO: set annotation
        // consequence types
        // set genes
        // set SO Accession
        // set protein substitution scores: sift and polyphen
        // set populations
        // set conservations
        // set cadd
        // set clinvar


        if (annotation) {
            variant.setAnnotation(variantAnnotation);
        }


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

        // set id, chromosome, start, end, dbSNP, type
        variantSearch.setId(variant.getChromosome() + "_" + variant.getStart() + "_"
                + variant.getReference() + "_" + variant.getAlternate());
        variantSearch.setChromosome(variant.getChromosome());
        variantSearch.setStart(variant.getStart());
        variantSearch.setEnd(variant.getEnd());
        variantSearch.setDbSNP(variant.getId());
        variantSearch.setType(variant.getType().toString());

        // set studies
        if (variant.getStudies() != null && variant.getStudies().size() > 0) {
            List<String> studies = new ArrayList<>();
            variant.getStudies().forEach(s -> studies.add(s.getStudyId()));
            variantSearch.setStudies(studies);
        }

        // check for annotation
        VariantAnnotation variantAnnotation = variant.getAnnotation();
        if (variantAnnotation != null) {

            // consequence types
            List<ConsequenceType> consequenceTypes = variantAnnotation.getConsequenceTypes();
            if (consequenceTypes != null) {
                Set<String> genes = new HashSet<>();
                Set<Integer> soAccessions = new HashSet<>();
                Set<String> geneToSOAccessions = new HashSet<>();

                for (ConsequenceType consequenceType : consequenceTypes) {

                    // set genes
                    genes.add(consequenceType.getGeneName());

                    // set SO Accession
                    for (SequenceOntologyTerm sequenceOntologyTerm : consequenceType.getSequenceOntologyTerms()) {
                        // remove SO: from the accession
                        String soNumber = sequenceOntologyTerm.getAccession().substring(3);
                        soAccessions.add(Integer.parseInt(soNumber));
                        geneToSOAccessions.add(consequenceType.getGeneName() + "_" + soNumber);
                    }

                    // set protein substitution scores: sift and polyphen
                    List<Double> proteinScores = getSubstitutionScores(consequenceType);
                    variantSearch.setSift(proteinScores.get(0));
                    variantSearch.setPolyphen(proteinScores.get(1));
                }
                variantSearch.setGenes(genes);
                variantSearch.setSoAcc(soAccessions);
                variantSearch.setGeneToSoAcc(geneToSOAccessions);
            }

            // set populations
            if (variantAnnotation.getPopulationFrequencies() != null) {
                for (PopulationFrequency populationFrequency : variantAnnotation.getPopulationFrequencies()) {
                    Map<String, Float> population = new HashMap<>();
                    population.put("popFreq_" + populationFrequency.getStudy() + "_"
                                    + populationFrequency.getPopulation(),
                            populationFrequency.getAltAlleleFreq());
                    variantSearch.setPopFreq(population);

                }
            }

            // set conservations
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

            // set cadd
            if (variantAnnotation.getFunctionalScore() != null) {
                for (Score score : variantAnnotation.getFunctionalScore()) {
                    if ("cadd_raw".equals(score.getSource())) {
                        variantSearch.setCaddRaw(score.getScore());
                    } else if ("cadd_scaled".equals(score.getSource())) {
                        variantSearch.setCaddScaled(score.getScore());
                    }
                }
            }

            // set clinvar
            if (variantAnnotation.getVariantTraitAssociation() != null
                    && variantAnnotation.getVariantTraitAssociation().getClinvar() != null) {
                Set<String> clinvar = new HashSet<>();
                variantAnnotation.getVariantTraitAssociation().getClinvar()
                        .forEach(cv -> {
                            clinvar.add(cv.getAccession());
                            cv.getTraits().forEach(cvt -> clinvar.add(cvt));
                        });
                variantSearch.setClinvar(clinvar);
            }
        }
        return variantSearch;
    }


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
     * Retrieve the protein substitution scores from a consquence
     * type annotation: sift or polyphen.
     *
     * @param consequenceType   Consequence type target
     * @return                  Max. and min. scores
     */
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

        // return two values: first, the min. value, and second, the max. value
        List<Double> result = new ArrayList<>(2);
        result.add(min);
        result.add(max);

        return result;
    }
}

