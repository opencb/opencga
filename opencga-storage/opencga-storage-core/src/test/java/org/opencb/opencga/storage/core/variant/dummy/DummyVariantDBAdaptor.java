package org.opencb.opencga.storage.core.variant.dummy;

import com.google.common.collect.BiMap;
import org.opencb.biodata.models.core.Region;
import org.opencb.biodata.models.feature.Genotype;
import org.opencb.biodata.models.variant.StudyEntry;
import org.opencb.biodata.models.variant.Variant;
import org.opencb.biodata.models.variant.VariantSource;
import org.opencb.biodata.models.variant.avro.AdditionalAttribute;
import org.opencb.biodata.models.variant.avro.FileEntry;
import org.opencb.biodata.models.variant.avro.VariantAnnotation;
import org.opencb.biodata.models.variant.stats.VariantSourceStats;
import org.opencb.biodata.models.variant.stats.VariantStats;
import org.opencb.cellbase.client.rest.CellBaseClient;
import org.opencb.commons.datastore.core.Query;
import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.commons.datastore.core.QueryResult;
import org.opencb.opencga.storage.core.exceptions.StorageManagerException;
import org.opencb.opencga.storage.core.metadata.StudyConfiguration;
import org.opencb.opencga.storage.core.metadata.StudyConfigurationManager;
import org.opencb.opencga.storage.core.variant.adaptors.VariantDBAdaptor;
import org.opencb.opencga.storage.core.variant.adaptors.VariantDBAdaptorUtils;
import org.opencb.opencga.storage.core.variant.adaptors.VariantDBIterator;
import org.opencb.opencga.storage.core.variant.adaptors.VariantSourceDBAdaptor;
import org.opencb.opencga.storage.core.variant.stats.VariantStatsWrapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.*;
import java.util.function.Consumer;

/**
 * Created on 28/11/16.
 *
 * @author Jacobo Coll &lt;jacobo167@gmail.com&gt;
 */
public class DummyVariantDBAdaptor implements VariantDBAdaptor {


    private final String dbName;
    private boolean closed = false;
    private Logger logger = LoggerFactory.getLogger(DummyVariantDBAdaptor.class);
    private static final List<String> TEMPLATES;

    static {
        TEMPLATES = new ArrayList<>();
        for (int chr = 22; chr > 0; chr--) {
            TEMPLATES.add(chr + ":1000:A:C");
        }
        TEMPLATES.add("X:1000:A:C");
        TEMPLATES.add("Y:1000:A:C");
        TEMPLATES.add("MT:1000:A:C");
    }

    public DummyVariantDBAdaptor(String dbName) {
        this.dbName = dbName;
    }

    @Override
    public QueryResult<Variant> get(Query query, QueryOptions options) {
        return null;
    }

    @Override
    public List<QueryResult<Variant>> get(List<Query> queries, QueryOptions options) {
        return null;
    }

    @Override
    public QueryResult<Variant> getPhased(String variant, String studyName, String sampleName, QueryOptions options, int windowsSize) {
        return null;
    }

    @Override
    public QueryResult<Long> count(Query query) {
        return null;
    }

    @Override
    public QueryResult distinct(Query query, String field) {
        return null;
    }

    @Override
    public VariantDBIterator iterator() {
        return iterator(new Query(), new QueryOptions());
    }

    @Override
    public VariantDBIterator iterator(Query query, QueryOptions options) {
        List<Variant> variants = new ArrayList<>(TEMPLATES.size());
        for (String template : TEMPLATES) {
            Variant variant = new Variant(template);

            Map<Integer, List<Integer>> returnedSamples = getReturnedSamples(query, options);
            returnedSamples.forEach((study, samples) -> {
                StudyConfiguration sc = getStudyConfigurationManager().getStudyConfiguration(study, null).first();
                if (sc.getIndexedFiles().isEmpty()) {
                    // Ignore non indexed studies
                    return; // continue
                }
                StudyEntry st = new StudyEntry(sc.getStudyName(), Collections.emptyList(), Collections.singletonList("GT"));
                BiMap<Integer, String> samplesMap = sc.getSampleIds().inverse();
                for (Integer sampleId : samples) {
                    st.addSampleData(samplesMap.get(sampleId), Collections.singletonList("0/0"));
                }
                variant.addStudyEntry(st);
                for (Integer cid : sc.getCalculatedStats()) {
                    VariantStats stats = new VariantStats();
                    stats.addGenotype(new Genotype("0/0"), sc.getCohorts().get(cid).size());
                    st.setStats(sc.getCohortIds().inverse().get(cid), stats);
                }
                List<FileEntry> files = new ArrayList<>();
                for (Integer id : sc.getIndexedFiles()) {
                    files.add(new FileEntry(id.toString(), "", Collections.emptyMap()));
                }
                st.setFiles(files);
            });
            variants.add(variant);
        }

        return toVariantDBIterator(variants);
    }

    VariantDBIterator toVariantDBIterator(final List<Variant> variants) {
        return new VariantDBIterator() {
            Iterator<Variant> iterator = variants.iterator();

            @Override
            public boolean hasNext() {
                return iterator.hasNext();
            }

            @Override
            public Variant next() {
                return iterator.next();
            }
        };
    }

    @Override
    public void forEach(Consumer<? super Variant> action) {

    }

    @Override
    public void forEach(Query query, Consumer<? super Variant> action, QueryOptions options) {

    }

    @Override
    public QueryResult getFrequency(Query query, Region region, int regionIntervalSize) {
        return null;
    }

    @Override
    public QueryResult rank(Query query, String field, int numResults, boolean asc) {
        return null;
    }

    @Override
    public QueryResult groupBy(Query query, String field, QueryOptions options) {
        return null;
    }

    @Override
    public QueryResult groupBy(Query query, List<String> fields, QueryOptions options) {
        return null;
    }

    // Update methods

    @Override
    public QueryResult updateStats(List<VariantStatsWrapper> variantStatsWrappers, StudyConfiguration studyConfiguration, QueryOptions options) {
        QueryResult queryResult = new QueryResult();
        logger.info("Writing " + variantStatsWrappers.size() + " statistics");
        queryResult.setNumResults(variantStatsWrappers.size());
        queryResult.setNumTotalResults(variantStatsWrappers.size());
        return queryResult;
    }

    @Override
    public QueryResult updateAnnotations(List<VariantAnnotation> variantAnnotations, QueryOptions queryOptions) {
        return new QueryResult();
    }

    @Override
    public QueryResult updateCustomAnnotations(Query query, String name, AdditionalAttribute attribute, QueryOptions options) {
        System.out.println("Update custom annotation : " + name);
        return new QueryResult();
    }

    // Unsupported methods

    @Override
    public QueryResult insert(List<Variant> variants, String studyName, QueryOptions options) {
        throw new UnsupportedOperationException();
    }

    @Override
    public QueryResult delete(Query query, QueryOptions options) {
        throw new UnsupportedOperationException();
    }

    @Override
    public QueryResult deleteSamples(String studyName, List<String> sampleNames, QueryOptions options) {
        throw new UnsupportedOperationException();
    }

    @Override
    public QueryResult deleteFile(String studyName, String fileName, QueryOptions options) {
        throw new UnsupportedOperationException();
    }

    @Override
    public QueryResult deleteStudy(String studyName, QueryOptions options) {
        throw new UnsupportedOperationException();
    }


    @Override
    public QueryResult addStats(List<VariantStatsWrapper> variantStatsWrappers, String studyName, QueryOptions queryOptions) {
        throw new UnsupportedOperationException();
    }

    @Override
    public QueryResult updateStats(List<VariantStatsWrapper> variantStatsWrappers, String studyName, QueryOptions queryOptions) {
        throw new UnsupportedOperationException();
    }

    @Override
    public QueryResult deleteStats(String studyName, String cohortName, QueryOptions options) {
        throw new UnsupportedOperationException();
    }

    @Override
    @Deprecated
    public QueryResult addAnnotations(List<VariantAnnotation> variantAnnotations, QueryOptions queryOptions) {
        throw new UnsupportedOperationException();
    }

    @Override
    public QueryResult deleteAnnotation(String annotationId, Query query, QueryOptions queryOptions) {
        throw new UnsupportedOperationException();
    }

    @Override
    public VariantSourceDBAdaptor getVariantSourceDBAdaptor() {
        return new VariantSourceDBAdaptor() {
            @Override
            public QueryResult<Long> count() {
                return new QueryResult<>();
            }

            @Override
            public void updateVariantSource(VariantSource variantSource) throws StorageManagerException {

            }

            @Override
            public Iterator<VariantSource> iterator(Query query, QueryOptions options) throws IOException {
                return Collections.emptyIterator();
            }

            @Override
            public QueryResult updateSourceStats(VariantSourceStats variantSourceStats, StudyConfiguration studyConfiguration, QueryOptions queryOptions) {
                return new QueryResult();
            }

            @Override
            public void close() throws IOException {

            }
        };
    }

    @Override
    public StudyConfigurationManager getStudyConfigurationManager() {
        return new DummyStudyConfigurationManager();
    }

    @Override
    public void setStudyConfigurationManager(StudyConfigurationManager studyConfigurationManager) {

    }

    @Override
    public CellBaseClient getCellBaseClient() {
        return null;
    }

    @Override
    public VariantDBAdaptorUtils getDBAdaptorUtils() {
        return new VariantDBAdaptorUtils(this);
    }

    @Override
    public void close() throws IOException {
        closed = true;
    }

}
