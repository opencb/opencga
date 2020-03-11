package org.opencb.opencga.storage.core.variant.query;

import org.opencb.biodata.models.variant.Variant;
import org.opencb.commons.datastore.core.Query;
import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.opencga.storage.core.metadata.models.SampleMetadata;
import org.opencb.opencga.storage.core.metadata.models.StudyMetadata;
import org.opencb.opencga.storage.core.variant.adaptors.VariantQueryParam;
import org.opencb.opencga.storage.core.variant.query.projection.VariantQueryProjection;

import java.util.LinkedList;
import java.util.List;

public class VariantQuery {

    private Query inputQuery;
    private QueryOptions inputOptions;
    private Query query;

    private VariantQueryProjection projection;

    private VariantStudyQuery studyQuery;
//    private VariantAnnotationQuery annotationQuery;


    public Query getInputQuery() {
        return inputQuery;
    }

    public VariantQuery setInputQuery(Query inputQuery) {
        this.inputQuery = inputQuery;
        return this;
    }

    public Query getQuery() {
        return query;
    }

    public VariantQuery setQuery(Query query) {
        this.query = query;
        return this;
    }

    public VariantQueryProjection getProjection() {
        return projection;
    }

    public VariantQuery setProjection(VariantQueryProjection projection) {
        this.projection = projection;
        return this;
    }

    public QueryOptions getInputOptions() {
        return inputOptions;
    }

    public VariantQuery setInputOptions(QueryOptions options) {
        this.inputOptions = options;
        return this;
    }

    public VariantStudyQuery getStudyQuery() {
        return studyQuery;
    }

    public VariantQuery setStudyQuery(VariantStudyQuery studyQuery) {
        this.studyQuery = studyQuery;
        return this;
    }

    public static class VariantStudyQuery {
        private ParsedQuery<String> studies;
        private ParsedQuery<KeyOpValue<SampleMetadata, List<String>>> genotypes;
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
    }
}
