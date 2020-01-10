package org.opencb.opencga.storage.core.variant.adaptors;

import org.hamcrest.CoreMatchers;
import org.junit.*;
import org.opencb.biodata.models.variant.Variant;
import org.opencb.commons.datastore.core.Query;
import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.opencga.core.response.VariantQueryResult;
import org.opencb.opencga.storage.core.exceptions.StorageEngineException;
import org.opencb.opencga.storage.core.exceptions.VariantSearchException;
import org.opencb.opencga.storage.core.metadata.VariantStorageMetadataManager;
import org.opencb.opencga.storage.core.metadata.models.StudyMetadata;
import org.opencb.opencga.storage.core.variant.search.VariantSearchUtils;
import org.opencb.opencga.storage.core.variant.solr.VariantSolrExternalResource;

import java.io.IOException;
import java.util.ArrayList;

import static org.hamcrest.CoreMatchers.*;
import static org.junit.Assert.assertThat;
import static org.opencb.biodata.models.variant.StudyEntry.FILTER;
import static org.opencb.opencga.storage.core.variant.adaptors.VariantMatchers.*;

/**
 * Created on 23/07/18.
 *
 * @author Jacobo Coll &lt;jacobo167@gmail.com&gt;
 */
public abstract class VariantDBAdaptorMultiFileSpecificSamplesCollectionTest extends VariantDBAdaptorMultiFileTest {

    @ClassRule
    public static VariantSolrExternalResource solr = new VariantSolrExternalResource();

    @Before
    public void before() throws Exception {
        solr.configure(variantStorageEngine);
        super.before();
        options.append(QueryOptions.EXCLUDE, VariantField.ANNOTATION);
    }

    @Override
    protected void load() throws Exception {
        super.load();

        VariantStorageMetadataManager scm = dbAdaptor.getMetadataManager();
        for (String studyName : scm.getStudyNames()) {
            StudyMetadata sc = scm.getStudyMetadata(studyName);
            ArrayList<String> samples = new ArrayList<>(metadataManager.getIndexedSamplesMap(sc.getId()).keySet());
            samples.sort(String::compareTo);
            variantStorageEngine.secondaryIndexSamples(sc.getName(), samples.subList(0, samples.size() / 2));
            variantStorageEngine.secondaryIndexSamples(sc.getName(), samples.subList(samples.size() / 2, samples.size()));
        }
    }


    protected VariantQueryResult<Variant> query(Query query, QueryOptions options) {
        try {
            query = variantStorageEngine.preProcessQuery(query, options);
            VariantStorageMetadataManager scm = dbAdaptor.getMetadataManager();
            String collection = VariantSearchUtils.inferSpecificSearchIndexSamplesCollection(query, options, scm, DB_NAME);

            // Do not execute this test if the query is not covered by the specific search index collection
            Assume.assumeThat(query.toJson(), collection, CoreMatchers.notNullValue());

            if (options.getInt(QueryOptions.LIMIT, 0) <= 0) {
                options = new QueryOptions(options);
                options.put(QueryOptions.LIMIT, 100000);
            }

            return variantStorageEngine.getVariantSearchManager().query(collection, query, options);
        } catch (StorageEngineException | VariantSearchException | IOException e) {
            e.printStackTrace();
            Assert.fail(e.getMessage());
        }
        return null;
    }


    @Test
    public void testGetByFilterAndSample() {
        VariantQueryResult<Variant> allVariants = dbAdaptor.get(new Query()
                .append(VariantQueryParam.INCLUDE_SAMPLE.key(), "NA12877")
                .append(VariantQueryParam.INCLUDE_FILE.key(), file12877)
                .append(VariantQueryParam.INCLUDE_STUDY.key(), "S_1"), options);

        query = new Query()
                .append(VariantQueryParam.FILTER.key(), "LowGQX;LowMQ;LowQD;TruthSensitivityTranche99.90to100.00")
                .append(VariantQueryParam.SAMPLE.key(), "NA12877")
                .append(VariantQueryParam.FILE.key(), file12877)
                .append(VariantQueryParam.STUDY.key(), "S_1");
        queryResult = query(query, options);
        System.out.println("queryResult.getNumResults() = " + queryResult.getNumResults());
        assertThat(queryResult, everyResult(allVariants, withStudy("S_1", allOf(
                withSampleData(sampleNA12877, "GT", anyOf(is("0/1"), is("1/1"))),
                withFileId(file12877,
                        with(FILTER, fileEntry -> fileEntry.getAttributes().get(FILTER), allOf(
                                containsString("LowGQX"),
                                containsString("LowMQ"),
                                containsString("LowQD"),
                                containsString("TruthSensitivityTranche99.90to100.00")
                        )))))));

        query = new Query()
                .append(VariantQueryParam.FILTER.key(), "MaxDepth")
                .append(VariantQueryParam.FILE.key(), file12877)
                .append(VariantQueryParam.SAMPLE.key(), "NA12877")
                .append(VariantQueryParam.STUDY.key(), "S_1");
        queryResult = query(query, options);
        System.out.println("queryResult.getNumResults() = " + queryResult.getNumResults());
        assertThat(queryResult, everyResult(allVariants, withStudy("S_1", allOf(
                withSampleData(sampleNA12877, "GT", anyOf(is("0/1"), is("1/1"))),
                withFileId(file12877,
                        with(FILTER, fileEntry -> fileEntry.getAttributes().get(FILTER), anyOf(
                                containsString("MaxDepth")
                        )))
        ))));

        query = new Query()
                .append(VariantQueryParam.FILTER.key(), "LowGQX,LowMQ")
                .append(VariantQueryParam.FILE.key(), file12877)
                .append(VariantQueryParam.SAMPLE.key(), "NA12877")
                .append(VariantQueryParam.STUDY.key(), "S_1");
        queryResult = query(query, options);
        System.out.println("queryResult.getNumResults() = " + queryResult.getNumResults());
        assertThat(queryResult, everyResult(allVariants, withStudy("S_1", allOf(
                withSampleData(sampleNA12877, "GT", anyOf(is("0/1"), is("1/1"))),
                withFileId(file12877,
                        with(FILTER, fileEntry -> fileEntry.getAttributes().get(FILTER), anyOf(
                                containsString("LowGQX"),
                                containsString("LowMQ")
                        )))))));

        query = new Query()
                .append(VariantQueryParam.FILTER.key(), "\"LowGQX;LowMQ;LowQD;TruthSensitivityTranche99.90to100.00\"")
                .append(VariantQueryParam.FILE.key(), file12877)
                .append(VariantQueryParam.SAMPLE.key(), "NA12877")
                .append(VariantQueryParam.STUDY.key(), "S_1");
        queryResult = query(query, options);
        System.out.println("queryResult.getNumResults() = " + queryResult.getNumResults());
        assertThat(queryResult, everyResult(allVariants, withStudy("S_1", allOf(
                withSampleData(sampleNA12877, "GT", anyOf(is("0/1"), is("1/1"))),
                withFileId(file12877,
                        with(FILTER, fileEntry -> fileEntry.getAttributes().get(FILTER), is("LowGQX;LowMQ;LowQD;TruthSensitivityTranche99.90to100.00")))))));

        query = new Query()
                .append(VariantQueryParam.FILTER.key(), "\"LowGQX;LowMQ;LowQD;TruthSensitivityTranche99.90to100.00\",\"LowGQX;LowQD;SiteConflict\"")
                .append(VariantQueryParam.FILE.key(), file12877)
                .append(VariantQueryParam.SAMPLE.key(), "NA12877")
                .append(VariantQueryParam.STUDY.key(), "S_1");
        queryResult = query(query, options);
        System.out.println("queryResult.getNumResults() = " + queryResult.getNumResults());
        assertThat(queryResult, everyResult(allVariants, withStudy("S_1", allOf(
                withSampleData(sampleNA12877, "GT", anyOf(is("0/1"), is("1/1"))),
                withFileId(file12877,
                        with(FILTER, fileEntry -> fileEntry.getAttributes().get(FILTER), anyOf(
                                is("LowGQX;LowMQ;LowQD;TruthSensitivityTranche99.90to100.00"),
                                is("LowGQX;LowQD;SiteConflict")
                        )))))));
    }

}

