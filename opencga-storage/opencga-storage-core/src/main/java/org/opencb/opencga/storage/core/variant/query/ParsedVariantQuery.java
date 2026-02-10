package org.opencb.opencga.storage.core.variant.query;

import org.opencb.biodata.models.core.Region;
import org.opencb.biodata.models.variant.Variant;
import org.opencb.biodata.models.variant.avro.VariantType;
import org.opencb.commons.datastore.core.Event;
import org.opencb.commons.datastore.core.Query;
import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.opencga.storage.core.metadata.models.SampleMetadata;
import org.opencb.opencga.storage.core.metadata.models.StudyMetadata;
import org.opencb.opencga.storage.core.metadata.models.StudyResourceMetadata;
import org.opencb.opencga.storage.core.variant.adaptors.VariantQuery;
import org.opencb.opencga.storage.core.variant.adaptors.VariantQueryException;
import org.opencb.opencga.storage.core.variant.adaptors.VariantQueryParam;
import org.opencb.opencga.storage.core.variant.query.projection.VariantQueryProjection;

import java.util.*;
import java.util.stream.Collectors;

import static org.opencb.opencga.storage.core.variant.adaptors.VariantQueryParam.*;
import static org.opencb.opencga.storage.core.variant.query.VariantQueryUtils.ID_INTERSECT;

public class ParsedVariantQuery {

    private Query inputQuery;
    private QueryOptions inputOptions;
    private VariantQuery query;
    private boolean optimized = false;

    private List<Event> events = new ArrayList<>();

    private VariantQueryProjection projection;
    private final VariantStudyQuery studyQuery;
    private final VariantAnnotationQuery annotationQuery;

    private Integer limit;
    private int skip;
    private boolean count;
    private boolean sort;
    private boolean sortAscending;
    private int approximateCountSamplingSize;
    private List<Region> geneRegions;
    private List<Region> regions;
    private List<VariantType> type;
    private VariantQuerySource source;


    public ParsedVariantQuery() {
        this.inputQuery = new Query();
        this.inputOptions = new QueryOptions();
        this.query = new VariantQuery();
        this.studyQuery = new VariantStudyQuery();
        this.annotationQuery = new VariantAnnotationQuery();
    }

    public ParsedVariantQuery(Query inputQuery, QueryOptions inputOptions) {
        this.inputQuery = inputQuery;
        this.inputOptions = inputOptions;
        this.query = new VariantQuery(inputQuery);
        this.studyQuery = new VariantStudyQuery();
        this.annotationQuery = new VariantAnnotationQuery();
    }

    public ParsedVariantQuery(ParsedVariantQuery other) {
        this.inputQuery = new Query(other.inputQuery);
        this.inputOptions = new QueryOptions(other.inputOptions);
        this.query = new VariantQuery(other.query);
        this.projection = other.projection;
        this.studyQuery = new VariantStudyQuery(other.getStudyQuery());
        this.optimized = other.optimized;
        this.limit = other.limit;
        this.skip = other.skip;
        this.count = other.count;
        this.approximateCountSamplingSize = other.approximateCountSamplingSize;
        this.geneRegions = new ArrayList<>(other.geneRegions);
        this.regions = new ArrayList<>(other.regions);
        this.source = other.source;
        this.annotationQuery = new VariantAnnotationQuery(other.annotationQuery);
    }

    public Query getInputQuery() {
        return inputQuery;
    }

    public ParsedVariantQuery setInputQuery(Query inputQuery) {
        this.inputQuery = inputQuery;
        return this;
    }

    public VariantQuery getQuery() {
        return query;
    }

    public ParsedVariantQuery setQuery(VariantQuery query) {
        this.query = query;
        return this;
    }

    public boolean isOptimized() {
        return optimized;
    }

    public ParsedVariantQuery setOptimized(boolean optimized) {
        this.optimized = optimized;
        return this;
    }

    public List<Event> getEvents() {
        return events;
    }

    public ParsedVariantQuery setEvents(List<Event> events) {
        this.events = events;
        return this;
    }

    public VariantQueryProjection getProjection() {
        return projection;
    }

    public ParsedVariantQuery setProjection(VariantQueryProjection projection) {
        this.projection = projection;
        return this;
    }

    public QueryOptions getInputOptions() {
        return inputOptions;
    }

    public ParsedVariantQuery setInputOptions(QueryOptions options) {
        this.inputOptions = options;
        return this;
    }

    public VariantStudyQuery getStudyQuery() {
        return studyQuery;
    }

    public List<Variant> getVariantIdIntersect() {
        return query.getAsStringList(ID_INTERSECT.key()).stream().map(Variant::new).collect(Collectors.toList());
    }

    public VariantQueryXref getXrefs() {
        return VariantQueryParser.parseXrefs(query);
    }

    public List<Region> getRegions() {
        return regions;
    }

    public ParsedVariantQuery setRegions(List<Region> regions) {
        this.regions = regions;
        return this;
    }

    public List<Region> getGeneRegions() {
        return geneRegions;
    }

    public ParsedVariantQuery setGeneRegions(List<Region> geneRegions) {
        this.geneRegions = geneRegions;
        return this;
    }

    public List<VariantType> getType() {
        return type;
    }

    public ParsedVariantQuery setType(List<VariantType> type) {
        this.type = type;
        return this;
    }

    public List<String> getConsequenceTypes() {
        return VariantQueryUtils.parseConsequenceTypes(query.getAsStringList(VariantQueryParam.ANNOT_CONSEQUENCE_TYPE.key()));
    }

    public List<String> getBiotypes() {
        return query.getAsStringList(VariantQueryParam.ANNOT_BIOTYPE.key());
    }

    public List<String> getTranscriptFlags() {
        return query.getAsStringList(VariantQueryParam.ANNOT_TRANSCRIPT_FLAG.key());
    }

    public Integer getLimit() {
        return limit;
    }

    public int getLimitOr(int defaultValue) {
        return limit == null ? defaultValue : limit;
    }

    public ParsedVariantQuery setLimit(Integer limit) {
        this.limit = limit;
        return this;
    }

    public int getSkip() {
        return skip;
    }

    public ParsedVariantQuery setSkip(int skip) {
        this.skip = skip;
        return this;
    }

    public boolean getCount() {
        return count;
    }

    public ParsedVariantQuery setCount(boolean count) {
        this.count = count;
        return this;
    }

    public boolean isSort() {
        return sort;
    }

    public boolean isSortAscending() {
        return sort && sortAscending;
    }

    public boolean isSortDescending() {
        return sort && !sortAscending;
    }

    public ParsedVariantQuery setSort(boolean sort, boolean sortAscending) {
        this.sort = sort;
        this.sortAscending = sortAscending;
        return this;
    }


    public int getApproximateCountSamplingSize() {
        return approximateCountSamplingSize;
    }

    public ParsedVariantQuery setApproximateCountSamplingSize(int approximateCountSamplingSize) {
        this.approximateCountSamplingSize = approximateCountSamplingSize;
        return this;
    }

    public ParsedQuery<KeyOpValue<String, Float>> getPopulationFrequencyAlt() {
        return VariantQueryParser.parseFreqFilter(query, ANNOT_POPULATION_ALTERNATE_FREQUENCY);
    }

    public ParsedQuery<KeyOpValue<String, Float>> getPopulationFrequencyRef() {
        return VariantQueryParser.parseFreqFilter(query, ANNOT_POPULATION_REFERENCE_FREQUENCY);
    }

    public ParsedQuery<KeyOpValue<String, Float>> getPopulationFrequencyMaf() {
        return VariantQueryParser.parseFreqFilter(query, ANNOT_POPULATION_MINOR_ALLELE_FREQUENCY);
    }

    public List<List<String>> getClinicalCombinations() {
        return annotationQuery.clinicalCombination;
    }

    public ParsedVariantQuery setClinicalCombination(List<List<String>> clinicalCombination) {
        this.annotationQuery.setClinicalCombination(clinicalCombination);
        return this;
    }

    public List<String> getClinicalCombinationsList() {
        return annotationQuery.clinicalCombinationList;
    }

    public ParsedVariantQuery setClinicalCombinationList(List<String> clinicalCombinationList) {
        this.annotationQuery.setClinicalCombinationList(clinicalCombinationList);
        return this;
    }

    public VariantAnnotationQuery getAnnotationQuery() {
        return annotationQuery;
    }

    public VariantQuerySource getSource() {
        return source;
    }

    public ParsedVariantQuery setSource(VariantQuerySource source) {
        this.source = source;
        return this;
    }

    public static class VariantStudyQuery {
        private ParsedQuery<NegatableValue<ResourceId>> studies;
        private ParsedQuery<NegatableValue<ResourceId>> files;
        private ParsedQuery<KeyOpValue<SampleMetadata, List<String>>> genotypes;
//        // SAMPLE : [ KEY OP VALUE ] *
        private ParsedQuery<KeyValues<SampleMetadata, KeyOpValue<String, String>>> sampleDataQuery;
//        // Merged genotype and sample_data filters
//        private Values<KeyValues<SampleMetadata, KeyOpValue<String, String>>> sampleFilters;
        private StudyMetadata defaultStudy;

        private boolean includeSampleId = false;

        public VariantStudyQuery() {
        }

        public VariantStudyQuery(ParsedQuery<NegatableValue<ResourceId>> studies,
                                 ParsedQuery<NegatableValue<ResourceId>> files,
                                 ParsedQuery<KeyOpValue<SampleMetadata, List<String>>> genotypes,
                                 ParsedQuery<KeyValues<SampleMetadata, KeyOpValue<String, String>>> sampleDataQuery,
                                 StudyMetadata defaultStudy) {
            this.studies = studies;
            this.files = files;
            this.genotypes = genotypes;
            this.sampleDataQuery = sampleDataQuery;
            this.defaultStudy = defaultStudy;
        }

        public VariantStudyQuery(VariantStudyQuery other) {
            this(
                    other.studies,
                    other.files,
                    other.genotypes,
                    other.sampleDataQuery,
                    other.defaultStudy
            );
        }

        /**
         * Value of {@link VariantQueryParam#STUDY}.
         * @return List of studies to be used in the query
         */
        public ParsedQuery<NegatableValue<ResourceId>> getStudies() {
            return studies;
        }

        public VariantStudyQuery setStudies(ParsedQuery<NegatableValue<ResourceId>> studies) {
            this.studies = studies;
            return this;
        }

        public ParsedQuery<NegatableValue<ResourceId>> getFiles() {
            return files;
        }

        public VariantStudyQuery setFiles(ParsedQuery<NegatableValue<ResourceId>> files) {
            this.files = files;
            return this;
        }

        public ParsedQuery<KeyOpValue<SampleMetadata, List<String>>> getGenotypes() {
            return genotypes;
        }

        public VariantStudyQuery setGenotypes(ParsedQuery<KeyOpValue<SampleMetadata, List<String>>> genotypes) {
            this.genotypes = genotypes;
            return this;
        }

        public ParsedQuery<KeyValues<SampleMetadata, KeyOpValue<String, String>>> getSampleDataQuery() {
            return sampleDataQuery;
        }

        public VariantStudyQuery setSampleDataQuery(ParsedQuery<KeyValues<SampleMetadata, KeyOpValue<String, String>>> sampleDataQuery) {
            this.sampleDataQuery = sampleDataQuery;
            return this;
        }

        public int countSamplesInFilter() {
            Set<String> samples = new HashSet<>();
            if (sampleDataQuery != null) {
                sampleDataQuery.stream().map(KeyValues::getKey).map(StudyResourceMetadata::getName).forEach(samples::add);
            }
            if (genotypes != null) {
                genotypes.stream().map(KeyOpValue::getKey).map(StudyResourceMetadata::getName).forEach(samples::add);
            }
            return samples.size();
        }

        public void setDefaultStudy(StudyMetadata defaultStudy) {
            this.defaultStudy = defaultStudy;
        }

        public StudyMetadata getDefaultStudy() {
            return defaultStudy;
        }

        public StudyMetadata getDefaultStudyOrFail() {
            if (defaultStudy == null) {
                if (studies.size() != 1) {
                    throw new VariantQueryException("Only one study is allowed. Found " + studies.size() + " studies");
                } else {
                    throw new VariantQueryException("One study required. None provided");
                }
            } else {
                return defaultStudy;
            }
        }

        public boolean isIncludeSampleId() {
            return includeSampleId;
        }

        public VariantStudyQuery setIncludeSampleId(boolean includeSampleId) {
            this.includeSampleId = includeSampleId;
            return this;
        }
    }

    public static class VariantAnnotationQuery {

        private List<List<String>> clinicalCombination;
        private List<String> clinicalCombinationList;
        private GeneCombinations geneCombinations;

        public VariantAnnotationQuery() {
        }

        public VariantAnnotationQuery(VariantAnnotationQuery other) {
            this.clinicalCombination = new ArrayList<>(other.clinicalCombination);
            this.clinicalCombinationList = new ArrayList<>(other.clinicalCombinationList);
            this.geneCombinations = new GeneCombinations(other.geneCombinations);
        }

        public List<List<String>> getClinicalCombination() {
            return clinicalCombination;
        }

        public VariantAnnotationQuery setClinicalCombination(List<List<String>> clinicalCombination) {
            this.clinicalCombination = clinicalCombination;
            return this;
        }

        public List<String> getClinicalCombinationList() {
            return clinicalCombinationList;
        }

        public VariantAnnotationQuery setClinicalCombinationList(List<String> clinicalCombinationList) {
            this.clinicalCombinationList = clinicalCombinationList;
            return this;
        }

        public GeneCombinations getGeneCombinations() {
            return geneCombinations;
        }

        public VariantAnnotationQuery setGeneCombinations(GeneCombinations geneCombinations) {
            this.geneCombinations = geneCombinations;
            return this;
        }
    }

    public static class VariantQueryXref {
        private final List<String> genes = new LinkedList<>();
        private final List<Variant> variants = new LinkedList<>();
        private final List<String> ids = new LinkedList<>();
        private final List<String> otherXrefs = new LinkedList<>();

        /**
         * @return List of genes found at {@link VariantQueryParam#GENE} and {@link VariantQueryParam#ANNOT_XREF}
         */
        public List<String> getGenes() {
            return genes;
        }

        /**
         * @return List of variants found at {@link VariantQueryParam#ANNOT_XREF} and {@link VariantQueryParam#ID}
         */
        public List<Variant> getVariants() {
            return variants;
        }

        /**
         * @return List of ids found at {@link VariantQueryParam#ID}
         */
        public List<String> getIds() {
            return ids;
        }

        /**
         * @return List of other xrefs found at
         * {@link VariantQueryParam#ANNOT_XREF},
         * {@link VariantQueryParam#ID},
         * {@link VariantQueryParam#ANNOT_CLINVAR},
         * {@link VariantQueryParam#ANNOT_COSMIC}
         */
        public List<String> getOtherXrefs() {
            return otherXrefs;
        }

        public List<String> getIDsAndXrefs() {
            List<String> all = new ArrayList<>(ids.size() + otherXrefs.size());
            all.addAll(ids);
            all.addAll(otherXrefs);
            return all;
        }

        public boolean isEmpty() {
            return genes.isEmpty() && variants.isEmpty() && ids.isEmpty() && otherXrefs.isEmpty();
        }

        @Override
        public String toString() {
            final StringBuilder sb = new StringBuilder("VariantQueryXref{");
            sb.append("genes=").append(genes);
            sb.append(", variants=").append(variants);
            sb.append(", ids=").append(ids);
            sb.append(", otherXrefs=").append(otherXrefs);
            sb.append('}');
            return sb.toString();
        }
    }

    public static class GeneCombinations {
        private final List<GeneCombination> combinations;
        private final Type type;

        public GeneCombinations(List<GeneCombination> combinations, Type type) {
            this.combinations = combinations;
            this.type = type;
        }

        public GeneCombinations(List<GeneCombination> combinations) {
            this.combinations = combinations;
            this.type = getType(combinations.get(0));
        }

        public GeneCombinations(GeneCombinations other) {
            this.combinations = new ArrayList<>(other.combinations);
            this.type = other.type;
        }

        public List<GeneCombination> getCombinations() {
            return combinations;
        }

        public Type getType() {
            return type;
        }

        public static Type getType(GeneCombination c) {
            String type = "";
            if (c.gene != null) {
                type += "GENE";
            }
            if (c.biotype != null) {
                type += (type.isEmpty() ? "" : "_") + "BIOTYPE";
            }
            if (c.so != null) {
                type += (type.isEmpty() ? "" : "_") + "SO";
            }
            if (c.flag != null) {
                type += (type.isEmpty() ? "" : "_") + "FLAG";
            }
            return GeneCombinations.Type.valueOf(type);
        }

        public enum Type {
            GENE_BIOTYPE_SO_FLAG,
            GENE_BIOTYPE_SO,
            GENE_BIOTYPE_FLAG,
            GENE_BIOTYPE,
            GENE_SO_FLAG,
            GENE_SO,
            GENE_FLAG,
//            GENE,
            BIOTYPE_SO_FLAG,
            BIOTYPE_SO,
            BIOTYPE_FLAG,
//            BIOTYPE,
            SO_FLAG,
//            SO,
//            FLAG
        }
    }

    public static class GeneCombination {

        private final String gene;
        private final String biotype;
        private final String so;
        private final String flag;

        public GeneCombination(String gene, String biotype, String so, String flag) {
            this.gene = gene;
            this.biotype = biotype;
            this.so = so;
            this.flag = flag;
        }

        public String getGene() {
            return gene;
        }

        public String getBiotype() {
            return biotype;
        }

        public String getSo() {
            return so;
        }

        public String getFlag() {
            return flag;
        }

    }
}
