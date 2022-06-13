package org.opencb.opencga.storage.core.variant.query;

import org.opencb.biodata.models.variant.Variant;
import org.opencb.commons.datastore.core.Query;
import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.opencga.storage.core.metadata.models.SampleMetadata;
import org.opencb.opencga.storage.core.metadata.models.StudyMetadata;
import org.opencb.opencga.storage.core.metadata.models.StudyResourceMetadata;
import org.opencb.opencga.storage.core.variant.adaptors.VariantQueryParam;
import org.opencb.opencga.storage.core.variant.query.projection.VariantQueryProjection;

import java.util.*;
import java.util.stream.Collectors;

import static org.opencb.opencga.storage.core.variant.query.VariantQueryUtils.ID_INTERSECT;

public class ParsedVariantQuery {

    private Query inputQuery;
    private QueryOptions inputOptions;
    private Query query;
    private boolean optimized = false;

    private VariantQueryProjection projection;

    private final VariantStudyQuery studyQuery;
//    private VariantAnnotationQuery annotationQuery;


    public ParsedVariantQuery() {
        this.inputQuery = new Query();
        this.inputOptions = new QueryOptions();
        this.query = new Query();
        studyQuery = new VariantStudyQuery();
    }

    public ParsedVariantQuery(Query inputQuery, QueryOptions inputOptions) {
        this.inputQuery = inputQuery;
        this.inputOptions = inputOptions;
        this.query = inputQuery;
        studyQuery = new VariantStudyQuery();
    }

    public ParsedVariantQuery(ParsedVariantQuery other) {
        this.inputQuery = new Query(other.inputQuery);
        this.inputOptions = new QueryOptions(other.inputOptions);
        this.query = new Query(other.query);
        this.projection = other.projection;
        this.studyQuery = other.studyQuery;
        this.optimized = other.optimized;
    }

    public Query getInputQuery() {
        return inputQuery;
    }

    public ParsedVariantQuery setInputQuery(Query inputQuery) {
        this.inputQuery = inputQuery;
        return this;
    }

    public Query getQuery() {
        return query;
    }

    public ParsedVariantQuery setQuery(Query query) {
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

    public List<String> getConsequenceTypes() {
        return VariantQueryUtils.parseConsequenceTypes(query.getAsStringList(VariantQueryParam.ANNOT_CONSEQUENCE_TYPE.key()));
    }

    public List<String> getBiotypes() {
        return query.getAsStringList(VariantQueryParam.ANNOT_BIOTYPE.key());
    }

    public static class VariantStudyQuery {
        private ParsedQuery<String> studies;
        private ParsedQuery<KeyOpValue<SampleMetadata, List<String>>> genotypes;
//        // SAMPLE : [ KEY OP VALUE ] *
        private ParsedQuery<KeyValues<SampleMetadata, KeyOpValue<String, String>>> sampleDataQuery;
//        // Merged genotype and sample_data filters
//        private Values<KeyValues<SampleMetadata, KeyOpValue<String, String>>> sampleFilters;
        private StudyMetadata defaultStudy;

        public VariantStudyQuery() {
        }

        public ParsedQuery<String> getStudies() {
            return studies;
        }

        public VariantStudyQuery setStudies(ParsedQuery<String> studies) {
            this.studies = studies;
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
    }
}
