package org.opencb.opencga.storage.hadoop.variant.index.query;

import org.apache.commons.collections4.CollectionUtils;
import org.opencb.biodata.models.core.Region;
import org.opencb.biodata.models.variant.Variant;
import org.opencb.biodata.models.variant.avro.VariantType;
import org.opencb.opencga.storage.core.variant.query.Values;
import org.opencb.opencga.storage.hadoop.variant.index.family.GenotypeCodec;
import org.opencb.opencga.storage.hadoop.variant.index.sample.SampleIndexSchema;

import java.util.*;
import java.util.stream.Collectors;

import static org.opencb.opencga.storage.core.variant.query.VariantQueryUtils.QueryOperation;
import static org.opencb.opencga.storage.hadoop.variant.index.IndexUtils.EMPTY_MASK;

/**
 * Created on 12/07/18.
 *
 * @author Jacobo Coll &lt;jacobo167@gmail.com&gt;
 */
public class SampleIndexQuery {
    private static final byte[] EMPTY_INDEX_MASK = {0, 0};
    static final boolean[] EMPTY_PARENT_FILTER;

    static {
        EMPTY_PARENT_FILTER = new boolean[GenotypeCodec.NUM_CODES];
        // All true values
        for (int i = 0; i < EMPTY_PARENT_FILTER.length; i++) {
            EMPTY_PARENT_FILTER[i] = true;
        }
    }

    private final SampleIndexSchema schema;
    private final Collection<LocusQuery> locusQueries;
    private final Set<VariantType> variantTypes;
    private final String study;
    private final Map<String, List<String>> samplesMap;
    private final Set<String> multiFileSamplesSet;
    /** Samples that should be subtracted from the final result. **/
    private final Set<String> negatedSamples;
    /** For each sample with father filter, indicates all the valid GTs codes. **/
    private final Map<String, boolean[]> fatherFilter;
    /** For each sample with mother filter, indicates all the valid GTs codes. **/
    private final Map<String, boolean[]> motherFilter;
    private final Map<String, Values<SampleFileIndexQuery>> fileFilterMap;
    private final SampleAnnotationIndexQuery annotationIndexQuery;
    private final Set<String> mendelianErrorSet;
    private final MendelianErrorType mendelianErrorType;
    private final boolean includeParentColumns;
    private final QueryOperation queryOperation;

    public enum MendelianErrorType {
        ALL,
        DE_NOVO,
        DE_NOVO_STRICT,
    }

    public SampleIndexQuery(Collection<LocusQuery> locusQueries, SampleIndexQuery query) {
        this.schema = query.schema;
        this.locusQueries = locusQueries;
        this.variantTypes = query.variantTypes;
        this.study = query.study;
        this.samplesMap = query.samplesMap;
        this.multiFileSamplesSet = query.multiFileSamplesSet;
        this.negatedSamples = query.negatedSamples;
        this.fatherFilter = query.fatherFilter;
        this.motherFilter = query.motherFilter;
        this.fileFilterMap = query.fileFilterMap;
        this.annotationIndexQuery = query.annotationIndexQuery;
        this.mendelianErrorSet = query.mendelianErrorSet;
        this.mendelianErrorType = query.mendelianErrorType;
        this.includeParentColumns = query.includeParentColumns;
        this.queryOperation = query.queryOperation;
    }

    public SampleIndexQuery(SampleIndexSchema schema, Collection<LocusQuery> locusQueries, String study, Map<String,
            List<String>> samplesMap, QueryOperation queryOperation) {
        this(schema, locusQueries, null, study, samplesMap, Collections.emptySet(), null, Collections.emptyMap(), Collections.emptyMap(),
                Collections.emptyMap(),
                new SampleAnnotationIndexQuery(schema), Collections.emptySet(), null, false, queryOperation);
    }

    public SampleIndexQuery(SampleIndexSchema schema, Collection<LocusQuery> locusQueries, Set<VariantType> variantTypes, String study,
                            Map<String, List<String>> samplesMap, Set<String> multiFileSamplesSet,
                            Set<String> negatedSamples, Map<String, boolean[]> fatherFilter, Map<String, boolean[]> motherFilter,
                            Map<String, Values<SampleFileIndexQuery>> fileFilterMap,
                            SampleAnnotationIndexQuery annotationIndexQuery,
                            Set<String> mendelianErrorSet, MendelianErrorType mendelianErrorType, boolean includeParentColumns,
                            QueryOperation queryOperation) {
        this.schema = schema;
        this.locusQueries = locusQueries;
        this.variantTypes = variantTypes;
        this.study = study;
        this.samplesMap = samplesMap;
        this.multiFileSamplesSet = multiFileSamplesSet;
        this.negatedSamples = negatedSamples;
        this.fatherFilter = fatherFilter;
        this.motherFilter = motherFilter;
        this.fileFilterMap = fileFilterMap;
        this.annotationIndexQuery = annotationIndexQuery;
        this.mendelianErrorSet = mendelianErrorSet;
        this.mendelianErrorType = mendelianErrorType;
        this.includeParentColumns = includeParentColumns;
        this.queryOperation = queryOperation;
    }

    public SampleIndexSchema getSchema() {
        return schema;
    }

    public Collection<LocusQuery> getLocusQueries() {
        return locusQueries;
    }

    public List<Region> getAllRegions() {
        return locusQueries.stream()
                .map(LocusQuery::getRegions)
                .flatMap(Collection::stream)
                .collect(Collectors.toList());
    }

    public List<Variant> getAllVariants() {
        return locusQueries.stream()
                .map(LocusQuery::getVariants)
                .flatMap(Collection::stream)
                .collect(Collectors.toList());
    }

    public Set<VariantType> getVariantTypes() {
        return variantTypes;
    }

    public String getStudy() {
        return study;
    }

    public Map<String, List<String>> getSamplesMap() {
        return samplesMap;
    }

    public boolean emptyOrRegionFilter() {
        if (!CollectionUtils.isEmpty(variantTypes)) {
            return false;
        }
        if (!emptyFileIndex()) {
            return false;
        }
        if (getFatherFilterMap() != null && !getFatherFilterMap().isEmpty()) {
            return false;
        }
        if (getMotherFilterMap() != null && !getMotherFilterMap().isEmpty()) {
            return false;
        }
        if (!getAnnotationIndexQuery().isEmpty()) {
            return false;
        }

        return true;
    }

    public Set<String> getNegatedSamples() {
        return negatedSamples;
    }


    public boolean isNegated(String sample) {
        return getNegatedSamples().contains(sample);
    }

    public Map<String, boolean[]> getFatherFilterMap() {
        return fatherFilter;
    }

    public boolean[] getFatherFilter(String sample) {
        return fatherFilter.getOrDefault(sample, EMPTY_PARENT_FILTER);
    }

    public Map<String, boolean[]> getMotherFilterMap() {
        return motherFilter;
    }

    public boolean[] getMotherFilter(String sample) {
        return motherFilter.getOrDefault(sample, EMPTY_PARENT_FILTER);
    }

    public Map<String, Values<SampleFileIndexQuery>> getSampleFileIndexQueryMap() {
        return fileFilterMap;
    }

    public Values<SampleFileIndexQuery> getSampleFileIndexQuery(String sample) {
        Values<SampleFileIndexQuery> sampleFileIndexQuery = fileFilterMap.get(sample);
        return sampleFileIndexQuery == null
                ? new Values<>(null, Collections.singletonList(new SampleFileIndexQuery(sample)))
                : sampleFileIndexQuery;
    }

//    public byte getFileIndex(String sample) {
//        SampleFileIndexQuery q = getSampleFileIndexQuery(sample);
//        return q == null ? EMPTY_MASK : q.getFileIndex();
//    }

    public boolean emptyFileIndex() {
        return fileFilterMap.isEmpty() || fileFilterMap.values()
                .stream()
                .flatMap(Values::stream)
                .allMatch(SampleFileIndexQuery::isEmpty);
    }

    public byte getAnnotationIndexMask() {
        return getAnnotationIndexQuery().getAnnotationIndexMask();
    }

    public byte getAnnotationIndex() {
        return getAnnotationIndexQuery().getAnnotationIndex();
    }

    public boolean emptyAnnotationIndex() {
        return getAnnotationIndexMask() == EMPTY_MASK;
    }

    public SampleAnnotationIndexQuery getAnnotationIndexQuery() {
        return annotationIndexQuery;
    }

    public boolean isIncludeParentColumns() {
        return includeParentColumns;
    }

    public QueryOperation getQueryOperation() {
        return queryOperation;
    }

    public Set<String> getMendelianErrorSet() {
        return mendelianErrorSet;
    }

    public Set<String> getMultiFileSamplesSet() {
        return multiFileSamplesSet;
    }

    public MendelianErrorType getMendelianErrorType() {
        return mendelianErrorType;
    }

    /**
     * Create a SingleSampleIndexQuery.
     *
     * @param sample Sample to query
     * @param gts    Processed list of GTs. Real GTs only.
     * @return SingleSampleIndexQuery
     */
    public SingleSampleIndexQuery forSample(String sample, List<String> gts) {
        return new SingleSampleIndexQuery(this, sample, gts);
    }

    /**
     * Create a SingleSampleIndexQuery.
     *
     * @param sample Sample to query
     * @return SingleSampleIndexQuery
     */
    public SingleSampleIndexQuery forSample(String sample) {
        return new SingleSampleIndexQuery(this, sample);
    }

}
