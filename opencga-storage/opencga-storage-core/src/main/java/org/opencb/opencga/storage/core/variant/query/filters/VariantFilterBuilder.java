package org.opencb.opencga.storage.core.variant.query.filters;

import org.opencb.biodata.models.core.Region;
import org.opencb.biodata.models.variant.Variant;
import org.opencb.biodata.models.variant.avro.ConsequenceType;
import org.opencb.biodata.models.variant.avro.SequenceOntologyTerm;
import org.opencb.commons.datastore.core.Query;
import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.opencga.storage.core.metadata.VariantStorageMetadataManager;
import org.opencb.opencga.storage.core.variant.adaptors.VariantQueryParam;
import org.opencb.opencga.storage.core.variant.query.ParsedVariantQuery;
import org.opencb.opencga.storage.core.variant.query.VariantQueryParser;
import org.opencb.opencga.storage.core.variant.query.VariantQueryUtils;

import java.util.*;
import java.util.function.Predicate;
import java.util.stream.Collectors;

import static org.opencb.opencga.storage.core.variant.adaptors.VariantQueryParam.REGION;

public class VariantFilterBuilder {

    private final VariantStorageMetadataManager metadataManager;

    public VariantFilterBuilder(VariantStorageMetadataManager metadataManager) {
        this.metadataManager = metadataManager;
    }

    public Predicate<Variant> buildFilter(Query query, QueryOptions queryOptions) {

        List<Predicate<Variant>> filters = new LinkedList<>();

        addRegionFilters(query, filters);
//        addAnnotationFilters(query, filters);

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
        Predicate<Variant> geneFilter = null;
        if (!variantQueryXref.getGenes().isEmpty()) {
            Set<String> genes = new HashSet<>(variantQueryXref.getGenes());

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
            geneFilter = variant -> {
                if (variant.getAnnotation() != null && variant.getAnnotation().getConsequenceTypes() != null) {
                    for (ConsequenceType ct : variant.getAnnotation().getConsequenceTypes()) {
                        if (validGene(genes, ct) && (validCt(cts, ct)) && (validBt(bts, ct))) {
                            return true;
                        }
                    }
                }
                return false;
            };
        }
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

//    private void addAnnotationFilters(Query query, List<Predicate<Variant>> filters) {
//        ParsedVariantQuery.VariantQueryXref variantQueryXref = VariantQueryParser.parseXrefs(query);
//
//    }

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
