/*
 * Copyright 2015-2017 OpenCB
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.opencb.opencga.storage.hadoop.variant.adaptors;

import org.junit.*;
import org.junit.rules.ExternalResource;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.opencb.biodata.models.feature.Genotype;
import org.opencb.biodata.models.variant.Variant;
import org.opencb.biodata.models.variant.avro.VariantType;
import org.opencb.commons.datastore.core.ObjectMap;
import org.opencb.commons.datastore.core.Query;
import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.opencga.core.common.UriUtils;
import org.opencb.opencga.core.results.VariantQueryResult;
import org.opencb.opencga.storage.core.exceptions.StorageEngineException;
import org.opencb.opencga.storage.core.utils.CellBaseUtils;
import org.opencb.opencga.storage.core.variant.VariantStorageEngine;
import org.opencb.opencga.storage.core.variant.VariantStorageOptions;
import org.opencb.opencga.storage.core.variant.adaptors.*;
import org.opencb.opencga.storage.core.variant.adaptors.iterators.VariantDBIterator;
import org.opencb.opencga.storage.hadoop.variant.HadoopVariantStorageEngine;
import org.opencb.opencga.storage.hadoop.variant.HadoopVariantStorageTest;
import org.opencb.opencga.storage.hadoop.variant.VariantHbaseTestUtils;
import org.opencb.opencga.storage.hadoop.variant.index.IndexUtils;
import org.opencb.opencga.storage.hadoop.variant.index.sample.SampleIndexQuery;
import org.opencb.opencga.storage.hadoop.variant.index.sample.SampleIndexQueryParser;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

import static org.junit.Assert.*;
import static org.junit.runners.Parameterized.Parameter;
import static org.junit.runners.Parameterized.Parameters;
import static org.opencb.opencga.storage.core.variant.adaptors.VariantMatchers.*;
import static org.opencb.opencga.storage.core.variant.adaptors.VariantQueryParam.*;
import static org.opencb.opencga.storage.hadoop.variant.HadoopVariantStorageEngine.MISSING_GENOTYPES_UPDATED;


/**
 * Created on 20/05/16
 *
 * @author Jacobo Coll &lt;jacobo167@gmail.com&gt;
 */
@RunWith(Parameterized.class)
public class HadoopVariantDBAdaptorTest extends VariantDBAdaptorTest implements HadoopVariantStorageTest {

    private static final boolean FILES = true;
    private static final boolean GROUP_BY = false;
    protected static final boolean MISSING_ALLELE = false;

    @ClassRule
    public static ExternalResource externalResource = new HadoopExternalResource();

    @Parameter
    public ObjectMap indexParams;

    public static ObjectMap previousIndexParams = null;
    protected CellBaseUtils cellBaseUtils;

    @Parameters
    public static List<Object[]> data() {
        List<Object[]> parameters = new ArrayList<>();
        parameters.add(new Object[]{
                new ObjectMap()
                        .append(VariantStorageOptions.TRANSFORM_FORMAT.key(), "avro")
                        .append(VariantStorageOptions.MERGE_MODE.key(), VariantStorageEngine.MergeMode.BASIC)
//                        .append(VariantStorageEngine.Options.EXTRA_FORMAT_FIELDS.key(), VariantMerger.GENOTYPE_FILTER_KEY + ",DS,GL")
                        .append(VariantStorageOptions.STATS_CALCULATE.key(), true)
                        .append(VariantStorageOptions.GVCF.key(), false)
        });
//        parameters.add(new Object[]{
//                new ObjectMap()
//                        .append(VariantStorageEngine.Options.TRANSFORM_FORMAT.key(), "proto")
//                        .append(VariantStorageEngine.Options.MERGE_MODE.key(), VariantStorageEngine.MergeMode.ADVANCED)
////                        .append(VariantStorageEngine.Options.EXTRA_FORMAT_FIELDS.key(), VariantMerger.GENOTYPE_FILTER_KEY + ",DS,GL")
//                        .append(VariantStorageEngine.Options.STATS_CALCULATE.key(), true)
//                        .append(VariantStorageEngine.Options.GVCF.key(), false)
//        });
        return parameters;
    }

    @Before
    @Override
    public void before() throws Exception {
        boolean fileIndexed = VariantDBAdaptorTest.fileIndexed;
        try {
            VariantStorageEngine.MergeMode mergeMode = VariantStorageEngine.MergeMode.from(indexParams);
            if (!indexParams.equals(previousIndexParams)) {
                fileIndexed = false;
                VariantDBAdaptorTest.fileIndexed = false;
                clearDB(getVariantStorageEngine().getVariantTableName());
                clearDB(getVariantStorageEngine().getArchiveTableName(STUDY_ID));
                clearDB(getVariantStorageEngine().getDBAdaptor().getTableNameGenerator().getMetaTableName());
            }
            previousIndexParams = indexParams;
            System.out.println("Loading with MergeMode : " + mergeMode);
            super.before();
        } finally {
            try {
                if (!fileIndexed) {
                    VariantHadoopDBAdaptor dbAdaptor = getVariantStorageEngine().getDBAdaptor();
                    studyMetadata = dbAdaptor.getMetadataManager().getStudyMetadata(studyMetadata.getId());
                    studyMetadata.getAttributes().put(MISSING_GENOTYPES_UPDATED, true);
                    dbAdaptor.getMetadataManager().unsecureUpdateStudyMetadata(studyMetadata);
                    VariantHbaseTestUtils.printVariants(studyMetadata, dbAdaptor, newOutputUri());
                }
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
        cellBaseUtils = variantStorageEngine.getCellBaseUtils();
    }
//
    //    @Override
//    public Map<String, ?> getOtherStorageConfigurationOptions() {
//        return new ObjectMap(AbstractHadoopVariantStoragePipeline.SKIP_CREATE_PHOENIX_INDEXES, true);
//    }

//    @Override
//    protected String getHetGT() {
//        return Genotype.HET_REF;
//    }
//
    @Override
    protected String getHomRefGT() {
        return Genotype.HOM_REF;
    }
//
//    @Override
//    protected String getHomAltGT() {
//        return Genotype.HOM_VAR;
//    }

    @Override
    protected ObjectMap getOtherParams() {
        return indexParams;
    }


    @Override
    public void rank_gene() throws Exception {
        Assume.assumeTrue(GROUP_BY);
        super.rank_gene();
    }

    @Override
    public void testExcludeFiles() {
        Assume.assumeTrue(FILES);
        super.testExcludeFiles();
    }

    @Override
    public void testReturnNoneFiles() {
        Assume.assumeTrue(FILES);
        super.testReturnNoneFiles();
    }

    @Override
    public void testGetAllVariants_missingAllele() throws Exception {
        Assume.assumeTrue(MISSING_ALLELE);
        super.testGetAllVariants_missingAllele();
    }

    @Override
    @Ignore
    public void groupBy_gene_limit_0() throws Exception {
        Assume.assumeTrue(GROUP_BY);
        super.groupBy_gene_limit_0();
    }

    @Override
    public void groupBy_gene() throws Exception {
        Assume.assumeTrue(GROUP_BY);
        super.groupBy_gene();
    }

    @Override
    public void testGetAllVariants_files() {
        Assume.assumeTrue(FILES);
        super.testGetAllVariants_files();
    }

    @Override
    public void testGetAllVariants_filterNoFile() {
        thrown.expect(VariantQueryException.class);
        super.testGetAllVariants_filterNoFile();
    }

    @Override
    public void rank_ct() throws Exception {
        Assume.assumeTrue(GROUP_BY);
        super.rank_ct();
    }

    @Override
    public void limitSkip(Query query, QueryOptions options) {
        Assume.assumeTrue("Unable to paginate queries without sorting", options.getBoolean(QueryOptions.SORT, false));
        super.limitSkip(query, options);
    }

    @Override
    public void testInclude() {
        Assume.assumeTrue(FILES);
        super.testInclude();
    }

    @Test
    public void testNativeQuery() {
        int count = 0;
        for (VariantDBIterator iterator = dbAdaptor.iterator(new Query(), new QueryOptions(VariantHadoopDBAdaptor.NATIVE, true)); iterator.hasNext();) {
            Variant variant = iterator.next();
//            System.out.println(variant.toJson());
            count++;
        }
        assertEquals(dbAdaptor.count(new Query()).first().intValue(), count);
    }

    @Test
    public void testArchiveIterator() {
        int count = 0;
        Query query = new Query(VariantQueryParam.STUDY.key(), studyMetadata.getId())
                .append(VariantQueryParam.FILE.key(), UriUtils.fileName(smallInputUri));

        for (VariantDBIterator iterator = dbAdaptor.iterator(query, new QueryOptions("archive", true)); iterator.hasNext(); ) {
            Variant variant = iterator.next();
//            System.out.println(variant.toJson());
            count++;
        }
        assertEquals(fileMetadata.getStats().getNumVariants(), count);
    }


    @Test
    public void testQueryFileIndex() throws Exception {
        testQueryFileIndex(new Query(TYPE.key(), "SNV"));
        testQueryFileIndex(new Query(TYPE.key(), "INDEL"));
    }

    @Test
    public void testQueryAnnotationIndex() throws Exception {
        testQueryAnnotationIndex(new Query(ANNOT_BIOTYPE.key(), "protein_coding"));
        testQueryAnnotationIndex(new Query(ANNOT_CONSEQUENCE_TYPE.key(), "missense_variant"));
        testQueryAnnotationIndex(new Query(ANNOT_CONSEQUENCE_TYPE.key(), "stop_lost"));
        testQueryAnnotationIndex(new Query(ANNOT_CONSEQUENCE_TYPE.key(), "stop_lost").append(ANNOT_TRANSCRIPT_FLAG.key(), "basic"));
        testQueryAnnotationIndex(new Query(ANNOT_CONSEQUENCE_TYPE.key(), "missense_variant,stop_lost").append(ANNOT_TRANSCRIPT_FLAG.key(), "basic"));
        testQueryAnnotationIndex(new Query(ANNOT_CONSEQUENCE_TYPE.key(), "stop_gained"));
        testQueryAnnotationIndex(new Query(ANNOT_PROTEIN_SUBSTITUTION.key(), "sift=tolerated"));
        testQueryAnnotationIndex(new Query(ANNOT_POPULATION_ALTERNATE_FREQUENCY.key(), "1kG_phase3:ALL<0.001"));
    }

    public void testQueryAnnotationIndex(Query annotationQuery) throws Exception {
        assertFalse(testQueryIndex(annotationQuery).emptyAnnotationIndex());
    }

    public void testQueryFileIndex(Query annotationQuery) throws Exception {
        testQueryIndex(annotationQuery);
    }

    public SampleIndexQuery testQueryIndex(Query annotationQuery) throws Exception {
        return testQueryIndex(annotationQuery, new Query()
                .append(STUDY.key(), STUDY_NAME)
                .append(SAMPLE.key(), "NA19600"));
    }

    public SampleIndexQuery testQueryIndex(Query annotationQuery, Query query) throws Exception {
//        System.out.println("----------------------------------------------------------");
//        queryResult = query(query, new QueryOptions());
//        int numResultsSample = queryResult.getNumResults();
//        System.out.println("Sample query: " + numResultsSample);

        // Query DBAdaptor
        System.out.println("Query DBAdaptor");
        query.putAll(annotationQuery);
        queryResult = query(new Query(query), new QueryOptions());
        int onlyDBAdaptor = queryResult.getNumResults();

        // Query SampleIndex
        System.out.println("Query SampleIndex");
        SampleIndexQuery indexQuery = SampleIndexQueryParser.parseSampleIndexQuery(new Query(query), variantStorageEngine.getMetadataManager());
//        int onlyIndex = (int) ((HadoopVariantStorageEngine) variantStorageEngine).getSampleIndexDBAdaptor()
//                .count(indexQuery, "NA19600");
        int onlyIndex = ((HadoopVariantStorageEngine) variantStorageEngine).getSampleIndexDBAdaptor()
                .iterator(indexQuery).toDataResult().getNumResults();

        // Query SampleIndex+DBAdaptor
        System.out.println("Query SampleIndex+DBAdaptor");
        VariantQueryResult<Variant> queryResult = variantStorageEngine.get(query, new QueryOptions());
        int indexAndDBAdaptor = queryResult.getNumResults();
        System.out.println("queryResult.source = " + queryResult.getSource());

        System.out.println("----------------------------------------------------------");
        System.out.println("query = " + annotationQuery.toJson());
        System.out.println("annotationIndex = " + IndexUtils.byteToString(indexQuery.getAnnotationIndexMask()));
        for (String sample : indexQuery.getSamplesMap().keySet()) {
            System.out.println("fileIndex("+sample+") = " + IndexUtils.maskToString(indexQuery.getFileIndexMask(sample), indexQuery.getFileIndex(sample)));
        }
        System.out.println("Query ONLY_INDEX = " + onlyIndex);
        System.out.println("Query NO_INDEX = " + onlyDBAdaptor);
        System.out.println("Query INDEX = " + indexAndDBAdaptor);

        if (onlyDBAdaptor != indexAndDBAdaptor) {
            queryResult = variantStorageEngine.get(query, new QueryOptions());
            List<String> indexAndDB = queryResult.getResults().stream().map(Variant::toString).sorted().collect(Collectors.toList());
            queryResult = query(query, new QueryOptions());
            List<String> noIndex = queryResult.getResults().stream().map(Variant::toString).sorted().collect(Collectors.toList());

            for (String s : indexAndDB) {
                if (!noIndex.contains(s)) {
                    System.out.println("From IndexAndDB, not in NoIndex = " + s);
                }
            }

            for (String s : noIndex) {
                if (!indexAndDB.contains(s)) {
                    System.out.println("From NoIndex, not in IndexAndDB = " + s);
                }
            }
        }
        assertEquals(onlyDBAdaptor, indexAndDBAdaptor);
        assertThat(queryResult, numResults(lte(onlyIndex)));
        assertThat(queryResult, numResults(gt(0)));
        return indexQuery;
    }


    @Test
    public void testSampleIndexSkipIntersect() throws StorageEngineException {
        Query query = new Query(VariantQueryParam.SAMPLE.key(), sampleNames.get(0)).append(VariantQueryParam.STUDY.key(), studyMetadata.getName());
        VariantQueryResult<Variant> result = variantStorageEngine.get(query, new QueryOptions(QueryOptions.INCLUDE, VariantField.ID).append(QueryOptions.LIMIT, 1));
        assertEquals("sample_index_table", result.getSource());

        query.append(VariantQueryParam.ANNOT_CONSEQUENCE_TYPE.key(), String.join(",", new ArrayList<>(VariantQueryUtils.LOF_SET)));
        result = variantStorageEngine.get(query, new QueryOptions(QueryOptions.INCLUDE, VariantField.ID).append(QueryOptions.LIMIT, 1));
        assertEquals("sample_index_table", result.getSource());

        query.append(VariantQueryParam.ANNOT_CONSEQUENCE_TYPE.key(), String.join(",", new ArrayList<>(VariantQueryUtils.LOF_EXTENDED_SET))).append(TYPE.key(), VariantType.INDEL);
        result = variantStorageEngine.get(query, new QueryOptions(QueryOptions.INCLUDE, VariantField.ID).append(QueryOptions.LIMIT, 1));
        assertEquals("sample_index_table", result.getSource());

        query.append(VariantQueryParam.ANNOT_CONSEQUENCE_TYPE.key(), new ArrayList<>(VariantQueryUtils.LOF_EXTENDED_SET)
                .subList(2, 4)
                .stream()
                .collect(Collectors.joining(",")));
        result = variantStorageEngine.get(query, new QueryOptions(QueryOptions.INCLUDE, VariantField.ID).append(QueryOptions.LIMIT, 1));
        assertNotEquals("sample_index_table", result.getSource());
    }

}
