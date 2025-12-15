package org.opencb.opencga.storage.core.variant.query.filters;

import org.apache.commons.collections4.CollectionUtils;
import org.opencb.biodata.models.core.Region;
import org.opencb.biodata.models.variant.Variant;
import org.opencb.biodata.models.variant.avro.ConsequenceType;
import org.opencb.biodata.models.variant.avro.SequenceOntologyTerm;
import org.opencb.opencga.storage.core.metadata.models.SampleMetadata;
import org.opencb.opencga.storage.core.variant.query.KeyOpValue;
import org.opencb.opencga.storage.core.variant.query.ParsedQuery;
import org.opencb.opencga.storage.core.variant.query.ParsedVariantQuery;
import org.opencb.opencga.storage.core.variant.query.VariantQueryUtils;

import java.util.*;
import java.util.function.Predicate;
import java.util.stream.Collectors;

public class VariantFilterBuilder {

    public VariantFilterBuilder() {
    }

    public Predicate<Variant> buildFilter(ParsedVariantQuery variantQuery) {

        List<Predicate<Variant>> filters = new LinkedList<>();

        addRegionFilters(variantQuery, filters);
        addStudyFilters(variantQuery, filters);
        addAnnotationFilters(variantQuery, filters);

        if (filters.isEmpty()) {
            return v -> true;
        } else {
            return mergeFilters(filters, VariantQueryUtils.QueryOperation.AND);
        }
    }

    private void addRegionFilters(ParsedVariantQuery variantQuery, List<Predicate<Variant>> filters) {
        List<Predicate<Variant>> regionFilters = new LinkedList<>();

        List<Region> regions = variantQuery.getRegions();
        if (regions != null && !regions.isEmpty()) {
            List<Region> regionsMerged = VariantQueryUtils.mergeRegions(regions);
            regionFilters.add(v -> {
                for (Region region : regionsMerged) {
                    if (region.overlaps(v.getChromosome(), v.getStart(), v.getEnd())) {
                        return true;
                    }
                }
                return false;
            });
            for (Region region : regionsMerged) {
                regionFilters.add(variant -> region.overlaps(variant.getChromosome(), variant.getStart(), variant.getEnd()));
            }
        }
        ParsedVariantQuery.VariantQueryXref variantQueryXref = variantQuery.getXrefs();
        Predicate<Variant> geneFilter = getGeneFilter(variantQuery, variantQueryXref.getGenes());
        if (!variantQueryXref.getIds().isEmpty()) {
            Set<String> ids = new HashSet<>(variantQueryXref.getIds());
            regionFilters.add(variant -> ids.contains(variant.getAnnotation().getId()));
        }
//        if (!variantQueryXref.getOtherXrefs().isEmpty()) {
//
//        }
        if (!variantQueryXref.getVariants().isEmpty()) {
            Set<String> variants = variantQueryXref.getVariants().stream().map(Variant::toString).collect(Collectors.toSet());
            regionFilters.add(variant -> variants.contains(variant.getId()));
        }

        if (!regionFilters.isEmpty()) {
            Set<String> bts;
            if (!variantQuery.getBiotypes().isEmpty()) {
                bts = new HashSet<>(variantQuery.getBiotypes());
            } else {
                bts = null;
            }
            Set<String> cts;
            if (!variantQuery.getConsequenceTypes().isEmpty()) {
                cts = new HashSet<>(variantQuery.getConsequenceTypes());
            } else {
                cts = null;
            }
            if (cts != null || bts != null) {
                filters.add(variant -> {
                    if (variant.getAnnotation() != null && variant.getAnnotation().getConsequenceTypes() != null) {
                        for (ConsequenceType ct : variant.getAnnotation().getConsequenceTypes()) {
                            if (validCt(cts, ct) && validBt(bts, ct)) {
                                return true;
                            }
                        }
                    }
                    return false;
                });
            }
        }
        regionFilters.add(geneFilter);


        Predicate<Variant> predicate = mergeFilters(regionFilters, VariantQueryUtils.QueryOperation.OR);
        if (predicate != null) {
            filters.add(predicate);
        }
    }

    private Predicate<Variant> getGeneFilter(ParsedVariantQuery variantQuery, List<String> genes) {
        if (genes.isEmpty()) {
            return null;
        }

        List<Region> geneRegions = variantQuery.getGeneRegions();
        Predicate<Variant> geneRegionFilter;
        if (CollectionUtils.isEmpty(geneRegions)) {
            geneRegionFilter = null;
        } else {
            geneRegionFilter = variant -> geneRegions.stream().anyMatch(r -> r.contains(variant.getChromosome(), variant.getStart()));
        }

        Predicate<Variant> geneFilter;

        Set<String> bts;
        if (!variantQuery.getBiotypes().isEmpty()) {
            bts = new HashSet<>(variantQuery.getBiotypes());
        } else {
            bts = null;
        }
        Set<String> cts;
        if (!variantQuery.getConsequenceTypes().isEmpty()) {
            cts = new HashSet<>(variantQuery.getConsequenceTypes());
        } else {
            cts = null;
        }
        Set<String> genesSet = new HashSet<>(genes);
        geneFilter = variant -> {
            if (variant.getAnnotation() != null && variant.getAnnotation().getConsequenceTypes() != null) {
                for (ConsequenceType ct : variant.getAnnotation().getConsequenceTypes()) {
                    if (validGene(genesSet, ct) && (validCt(cts, ct)) && (validBt(bts, ct))) {
                        return true;
                    }
                }
            }
            return false;
        };

        if (geneRegionFilter == null) {
            // No gene region filter. Use only gene filter.
            return geneFilter;
        } else if (cts == null && bts == null) {
            // No CT not BT filter. Region filter is enough.
            return geneRegionFilter;
        } else {
            // Use both geneRegion and gene filter.
            return geneRegionFilter.and(geneFilter);
        }
    }

    private void addStudyFilters(ParsedVariantQuery variantQuery, List<Predicate<Variant>> filters) {

        if (variantQuery.getStudyQuery().getStudies() != null) {
            List<Predicate<Variant>> studyFilters = new LinkedList<>();
            variantQuery.getStudyQuery().getStudies().getValues().forEach(study -> {
                if (study.isNegated()) {
                    studyFilters.add(variant -> variant.getStudies().stream()
                            .noneMatch(variantStudy -> variantStudy.getStudyId().equals(study.getValue().getName())));
                } else {
                    studyFilters.add(variant -> variant.getStudies().stream()
                            .anyMatch(variantStudy -> variantStudy.getStudyId().equals(study.getValue().getName())));
                }
            });
            filters.add(mergeFilters(studyFilters, variantQuery.getStudyQuery().getStudies().getOperation()));
        }

        if (variantQuery.getStudyQuery().getFiles() != null) {
            List<Predicate<Variant>> fileFilters = new LinkedList<>();
            variantQuery.getStudyQuery().getFiles().getValues().forEach(file -> {
                if (file.isNegated()) {
                    fileFilters.add(variant -> variant.getStudies().stream()
                            .flatMap(variantStudy -> variantStudy.getFiles().stream())
                            .noneMatch(variantFile -> variantFile.getFileId().equals(file.getValue().getName())));
                } else {
                    fileFilters.add(variant -> variant.getStudies().stream()
                            .flatMap(variantStudy -> variantStudy.getFiles().stream())
                            .anyMatch(variantFile -> variantFile.getFileId().equals(file.getValue().getName())));
                }
            });
            filters.add(mergeFilters(fileFilters, variantQuery.getStudyQuery().getFiles().getOperation()));
        }

        if (variantQuery.getStudyQuery().getGenotypes() != null) {
            List<Predicate<Variant>> genotypeFilters = new LinkedList<>();
            variantQuery.getStudyQuery().getGenotypes().getValues().forEach(genotype -> {
                SampleMetadata sample = genotype.getKey();
                Set<String> values = new HashSet<>(genotype.getValue());
                String studyName = variantQuery.getStudyQuery().getDefaultStudyOrFail().getName();

                genotypeFilters.add(variant -> {
                    String gt = variant.getStudy(studyName).getSampleData(sample.getName(), "GT");
                    return values.contains(gt);
                });
            });
            filters.add(mergeFilters(genotypeFilters, variantQuery.getStudyQuery().getGenotypes().getOperation()));
        }

    }

    private void addAnnotationFilters(ParsedVariantQuery variantQuery, List<Predicate<Variant>> filters) {
//        ParsedVariantQuery.VariantQueryXref variantQueryXref = variantQuery.getXrefs();
        addClinicalFilters(variantQuery, filters);

        ParsedQuery<KeyOpValue<String, Float>> freqQuery = variantQuery.getPopulationFrequencyAlt();
        if (!freqQuery.isEmpty()) {
            List<PopulationFrequencyVariantFilter.AltFreqFilter> freqFilters = freqQuery.mapValues(popFreq -> {
                String[] split = popFreq.getKey().split(VariantQueryUtils.STUDY_POP_FREQ_SEPARATOR);
                return new PopulationFrequencyVariantFilter.AltFreqFilter(split[0], split[1], popFreq.getOp(), popFreq.getValue());
            });
            filters.add(new PopulationFrequencyVariantFilter(freqQuery.getOperation(), freqFilters));
        }

        freqQuery = variantQuery.getPopulationFrequencyRef();
        if (!freqQuery.isEmpty()) {
            List<PopulationFrequencyVariantFilter.RefFreqFilter> freqFilters = freqQuery.mapValues(popFreq -> {
                String[] split = popFreq.getKey().split(VariantQueryUtils.STUDY_POP_FREQ_SEPARATOR);
                return new PopulationFrequencyVariantFilter.RefFreqFilter(split[0], split[1], popFreq.getOp(), popFreq.getValue());
            });
            filters.add(new PopulationFrequencyVariantFilter(freqQuery.getOperation(), freqFilters));
        }

        freqQuery = variantQuery.getPopulationFrequencyMaf();
        if (!freqQuery.isEmpty()) {
            List<PopulationFrequencyVariantFilter.MafFreqFilter> freqFilters = freqQuery.mapValues(popFreq -> {
                String[] split = popFreq.getKey().split(VariantQueryUtils.STUDY_POP_FREQ_SEPARATOR);
                return new PopulationFrequencyVariantFilter.MafFreqFilter(split[0], split[1], popFreq.getOp(), popFreq.getValue());
            });
            filters.add(new PopulationFrequencyVariantFilter(freqQuery.getOperation(), freqFilters));
        }

    }

    private void addClinicalFilters(ParsedVariantQuery variantQuery, List<Predicate<Variant>> filters) {
        if (variantQuery.getClinicalCombinations() == null) {
            return;
        }
        List<Set<String>> clinicalCombinations = variantQuery.getClinicalCombinations()
                .stream().map(HashSet::new).collect(Collectors.toList());
        if (clinicalCombinations.isEmpty()) {
            return;
        }
        if (clinicalCombinations.size() == 1) {
            Set<String> clinicalCombinationSet = clinicalCombinations.get(0);
            filters.add(variant -> {
                for (String c : VariantQueryUtils.buildClinicalCombinations(variant.getAnnotation())) {
                    if (clinicalCombinationSet.contains(c)) {
                        return true;
                    }
                }
                return false;
            });
        } else {
            filters.add(variant -> {
                for (Set<String> sourceCombinations : clinicalCombinations) {
                    boolean validSource = false;
                    for (String c : VariantQueryUtils.buildClinicalCombinations(variant.getAnnotation())) {
                        if (sourceCombinations.contains(c)) {
                            validSource = true;
                            break;
                        }
                    }
                    if (!validSource) {
                        return false;
                    }
                }
                // All sources were valid
                return true;
            });
        }

    }

    private boolean validGene(Set<String> genes, ConsequenceType ct) {
        return genes.contains(ct.getGeneId()) || genes.contains(ct.getGeneName()) || genes.contains(ct.getTranscriptId());
    }

    private boolean validCt(Set<String> acceptedCtValues, ConsequenceType ct) {
        if (acceptedCtValues == null) {
            return true;
        }
        for (SequenceOntologyTerm so : ct.getSequenceOntologyTerms()) {
            if (acceptedCtValues.contains(so.getName())) {
                return true;
            }
        }
        return false;
    }

    private boolean validBt(Set<String> acceptedBtValues, ConsequenceType ct) {
        if (acceptedBtValues == null) {
            return true;
        }
        return acceptedBtValues.contains(ct.getBiotype());
    }

    private Predicate<Variant> mergeFilters(List<Predicate<Variant>> filters, VariantQueryUtils.QueryOperation operator) {
        filters.removeIf(Objects::isNull);
        if (filters.isEmpty()) {
            return null;
        } else if (filters.size() == 1) {
            return filters.get(0);
        } else {
            Predicate<Variant> predicate = filters.get(0);
            for (int i = 1; i < filters.size(); i++) {
                if (operator == VariantQueryUtils.QueryOperation.OR) {
                    predicate = predicate.or(filters.get(i));
                } else {
                    predicate = predicate.and(filters.get(i));
                }
            }
            return predicate;
        }
    }


}
