package org.opencb.opencga.storage.hadoop.variant.index.query;

import org.apache.commons.collections.CollectionUtils;
import org.opencb.biodata.models.core.Region;
import org.opencb.biodata.models.variant.avro.VariantType;
import org.opencb.opencga.storage.core.variant.query.Values;
import org.opencb.opencga.storage.hadoop.variant.index.family.GenotypeCodec;

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

    private final Collection<List<Region>> regionGroups;
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
    private final boolean onlyDeNovo;
    private final QueryOperation queryOperation;

    public SampleIndexQuery(Collection<List<Region>> regionGroups, SampleIndexQuery query) {
        this.regionGroups = regionGroups;
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
        this.onlyDeNovo = query.onlyDeNovo;
        this.queryOperation = query.queryOperation;
    }

    public SampleIndexQuery(Collection<List<Region>> regionGroups, String study, Map<String, List<String>> samplesMap,
                            QueryOperation queryOperation) {
        this(regionGroups, null, study, samplesMap, Collections.emptySet(), null, Collections.emptyMap(), Collections.emptyMap(),
                Collections.emptyMap(),
                new SampleAnnotationIndexQuery(), Collections.emptySet(), false, queryOperation);
    }

    public SampleIndexQuery(Collection<List<Region>> regionGroups, Set<VariantType> variantTypes, String study,
                            Map<String, List<String>> samplesMap, Set<String> multiFileSamplesSet,
                            Set<String> negatedSamples, Map<String, boolean[]> fatherFilter, Map<String, boolean[]> motherFilter,
                            Map<String, Values<SampleFileIndexQuery>> fileFilterMap,
                            SampleAnnotationIndexQuery annotationIndexQuery,
                            Set<String> mendelianErrorSet, boolean onlyDeNovo, QueryOperation queryOperation) {
        this.regionGroups = regionGroups;
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
        this.onlyDeNovo = onlyDeNovo;
        this.queryOperation = queryOperation;
    }

    public Collection<List<Region>> getRegionGroups() {
        return regionGroups;
    }

    public List<Region> getRegions() {
        return regionGroups.stream().flatMap(Collection::stream).collect(Collectors.toList());
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
                .allMatch(q -> q.getFileIndexMask() == EMPTY_MASK);
    }

    public byte getAnnotationIndexMask() {
        return annotationIndexQuery.getAnnotationIndexMask();
    }

    public byte getAnnotationIndex() {
        return annotationIndexQuery.getAnnotationIndex();
    }

    public boolean emptyAnnotationIndex() {
        return getAnnotationIndexMask() == EMPTY_MASK;
    }

    public SampleAnnotationIndexQuery getAnnotationIndexQuery() {
        return annotationIndexQuery;
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

    public boolean isOnlyDeNovo() {
        return onlyDeNovo;
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
