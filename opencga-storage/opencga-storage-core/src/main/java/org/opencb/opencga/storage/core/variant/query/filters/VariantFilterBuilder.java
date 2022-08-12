package org.opencb.opencga.storage.core.variant.query.filters;

import org.apache.commons.collections4.CollectionUtils;
import org.opencb.biodata.models.core.Region;
import org.opencb.biodata.models.variant.Variant;
import org.opencb.biodata.models.variant.avro.ConsequenceType;
import org.opencb.biodata.models.variant.avro.SequenceOntologyTerm;
import org.opencb.commons.datastore.core.Query;
import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.opencga.storage.core.metadata.VariantStorageMetadataManager;
import org.opencb.opencga.storage.core.variant.adaptors.VariantQueryParam;
import org.opencb.opencga.storage.core.variant.query.*;

import java.util.*;
import java.util.function.Predicate;
import java.util.stream.Collectors;

import static org.opencb.opencga.storage.core.variant.adaptors.VariantQueryParam.*;

public class VariantFilterBuilder {

    private final VariantStorageMetadataManager metadataManager;

    public VariantFilterBuilder(VariantStorageMetadataManager metadataManager) {
        this.metadataManager = metadataManager;
    }

    public Predicate<Variant> buildFilter(Query query, QueryOptions queryOptions) {

        List<Predicate<Variant>> filters = new LinkedList<>();

        addRegionFilters(query, filters);
        addAnnotationFilters(query, filters);

        if (filters.isEmpty()) {
            return v -> true;
        } else {
            return mergeFilters(filters, VariantQueryUtils.QueryOperation.AND);
        }
    }

    private void addRegionFilters(Query query, List<Predicate<Variant>> filters) {
        List<Predicate<Variant>> regionFilters = new LinkedList<>();

        if (VariantQueryUtils.isValidParam(query, VariantQueryParam.REGION)) {
            List<Region> regions = Region.parseRegions(query.getString(REGION.key()), true);
            regions = VariantQueryUtils.mergeRegions(regions);
            for (Region region : regions) {
                regionFilters.add(variant -> region.contains(variant.getChromosome(), variant.getStart()));
            }
        }
        ParsedVariantQuery.VariantQueryXref variantQueryXref = VariantQueryParser.parseXrefs(query);
        Predicate<Variant> geneFilter = getGeneFilter(query, variantQueryXref.getGenes());
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
            if (VariantQueryUtils.isValidParam(query, VariantQueryParam.ANNOT_BIOTYPE)) {
                bts = new HashSet<>(query.getAsStringList(VariantQueryParam.ANNOT_BIOTYPE.key()));
            } else {
                bts = null;
            }
            Set<String> cts;
            if (VariantQueryUtils.isValidParam(query, VariantQueryParam.ANNOT_CONSEQUENCE_TYPE)) {
                cts = new HashSet<>(VariantQueryUtils
                        .parseConsequenceTypes(query.getAsStringList(VariantQueryParam.ANNOT_CONSEQUENCE_TYPE.key())));
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

    private Predicate<Variant> getGeneFilter(Query query, List<String> genes) {
        if (genes.isEmpty()) {
            return null;
        }

        List<Region> geneRegions = Region.parseRegions(query.getString(VariantQueryUtils.ANNOT_GENE_REGIONS.key()));
        Predicate<Variant> geneRegionFilter;
        if (CollectionUtils.isEmpty(geneRegions)) {
            geneRegionFilter = null;
        } else {
            geneRegionFilter = variant -> geneRegions.stream().anyMatch(r -> r.contains(variant.getChromosome(), variant.getStart()));
        }

        Predicate<Variant> geneFilter;

        Set<String> bts;
        if (VariantQueryUtils.isValidParam(query, VariantQueryParam.ANNOT_BIOTYPE)) {
            bts = new HashSet<>(query.getAsStringList(VariantQueryParam.ANNOT_BIOTYPE.key()));
        } else {
            bts = null;
        }
        Set<String> cts;
        if (VariantQueryUtils.isValidParam(query, VariantQueryParam.ANNOT_CONSEQUENCE_TYPE)) {
            cts = new HashSet<>(VariantQueryUtils
                    .parseConsequenceTypes(query.getAsStringList(VariantQueryParam.ANNOT_CONSEQUENCE_TYPE.key())));
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

    private void addAnnotationFilters(Query query, List<Predicate<Variant>> filters) {
//        ParsedVariantQuery.VariantQueryXref variantQueryXref = VariantQueryParser.parseXrefs(query);
        addClinicalFilters(query, filters);

        if (VariantQueryUtils.isValidParam(query, ANNOT_POPULATION_ALTERNATE_FREQUENCY)) {
            ParsedQuery<KeyOpValue<String, Float>> freqQuery = VariantQueryParser.parseFreqFilter(query, ANNOT_POPULATION_ALTERNATE_FREQUENCY);
            List<PopulationFrequencyVariantFilter.AltFreqFilter> freqFilters = freqQuery.mapValues(popFreq -> {
                String[] split = popFreq.getKey().split(VariantQueryUtils.STUDY_POP_FREQ_SEPARATOR);
                return new PopulationFrequencyVariantFilter.AltFreqFilter(split[0], split[1], popFreq.getOp(), popFreq.getValue());
            });
            filters.add(new PopulationFrequencyVariantFilter(freqQuery.getOperation(), freqFilters));
        }
        if (VariantQueryUtils.isValidParam(query, ANNOT_POPULATION_REFERENCE_FREQUENCY)) {
            ParsedQuery<KeyOpValue<String, Float>> freqQuery = VariantQueryParser.parseFreqFilter(query, ANNOT_POPULATION_REFERENCE_FREQUENCY);
            List<PopulationFrequencyVariantFilter.RefFreqFilter> freqFilters = freqQuery.mapValues(popFreq -> {
                String[] split = popFreq.getKey().split(VariantQueryUtils.STUDY_POP_FREQ_SEPARATOR);
                return new PopulationFrequencyVariantFilter.RefFreqFilter(split[0], split[1], popFreq.getOp(), popFreq.getValue());
            });
            filters.add(new PopulationFrequencyVariantFilter(freqQuery.getOperation(), freqFilters));
        }
        if (VariantQueryUtils.isValidParam(query, ANNOT_POPULATION_MINOR_ALLELE_FREQUENCY)) {
            ParsedQuery<KeyOpValue<String, Float>> freqQuery = VariantQueryParser.parseFreqFilter(query, ANNOT_POPULATION_MINOR_ALLELE_FREQUENCY);
            List<PopulationFrequencyVariantFilter.MafFreqFilter> freqFilters = freqQuery.mapValues(popFreq -> {
                String[] split = popFreq.getKey().split(VariantQueryUtils.STUDY_POP_FREQ_SEPARATOR);
                return new PopulationFrequencyVariantFilter.MafFreqFilter(split[0], split[1], popFreq.getOp(), popFreq.getValue());
            });
            filters.add(new PopulationFrequencyVariantFilter(freqQuery.getOperation(), freqFilters));
        }

    }

    private void addClinicalFilters(Query query, List<Predicate<Variant>> filters) {
        List<Set<String>> clinicalCombinations = VariantQueryParser.parseClinicalCombination(query)
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
