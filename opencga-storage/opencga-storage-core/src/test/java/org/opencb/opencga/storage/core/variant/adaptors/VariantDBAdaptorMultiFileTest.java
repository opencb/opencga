package org.opencb.opencga.storage.core.variant.adaptors;

import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;
import org.opencb.biodata.models.variant.StudyEntry;
import org.opencb.biodata.models.variant.Variant;
import org.opencb.biodata.models.variant.avro.FileEntry;
import org.opencb.biodata.models.variant.stats.VariantStats;
import org.opencb.commons.datastore.core.*;
import org.opencb.opencga.core.response.VariantQueryResult;
import org.opencb.opencga.storage.core.exceptions.StorageEngineException;
import org.opencb.opencga.storage.core.metadata.VariantStorageMetadataManager;
import org.opencb.opencga.storage.core.metadata.models.FileMetadata;
import org.opencb.opencga.storage.core.metadata.models.StudyMetadata;
import org.opencb.opencga.storage.core.variant.VariantStorageBaseTest;
import org.opencb.opencga.storage.core.variant.VariantStorageEngine;
import org.opencb.opencga.storage.core.variant.VariantStorageOptions;
import org.opencb.opencga.storage.core.variant.query.VariantQueryParser;

import java.io.IOException;
import java.net.URI;
import java.util.*;
import java.util.stream.Collectors;

import static java.util.Arrays.asList;
import static org.hamcrest.CoreMatchers.*;
import static org.junit.Assert.*;
import static org.opencb.biodata.models.variant.StudyEntry.FILTER;
import static org.opencb.biodata.models.variant.StudyEntry.QUAL;
import static org.opencb.biodata.models.variant.StudyEntry.*;
import static org.opencb.opencga.storage.core.variant.adaptors.VariantMatchers.*;
import static org.opencb.opencga.storage.core.variant.adaptors.VariantQueryParam.*;
import static org.opencb.opencga.storage.core.variant.adaptors.VariantQueryUtils.*;

/**
 * Created on 24/10/17.
 *
 * @author Jacobo Coll &lt;jacobo167@gmail.com&gt;
 */
@Ignore
public abstract class VariantDBAdaptorMultiFileTest extends VariantStorageBaseTest {

    protected static final String file12877 = "1K.end.platinum-genomes-vcf-NA12877_S1.genome.vcf.gz";
    protected static final String file12878 = "1K.end.platinum-genomes-vcf-NA12878_S1.genome.vcf.gz";
    protected static final String file12882 = "1K.end.platinum-genomes-vcf-NA12882_S1.genome.vcf.gz";
    protected static final String file12879 = "1K.end.platinum-genomes-vcf-NA12879_S1.genome.vcf.gz";
    protected static final String file12880 = "1K.end.platinum-genomes-vcf-NA12880_S1.genome.vcf.gz";
    protected static final String sampleNA12877 = "NA12877";
    protected static final String sampleNA12878 = "NA12878";
    protected static final String sampleNA12882 = "NA12882";
    protected static final String sampleNA12879 = "NA12879";
    protected static final String sampleNA12880 = "NA12880";
    public static final String study1 = "S_1";
    public static final String study2 = "S_2";
    protected static boolean loaded = false;
    protected VariantDBAdaptor dbAdaptor;
    protected Query query;
    protected QueryOptions options = new QueryOptions();
    protected VariantQueryResult<Variant> queryResult;

    @Before
    public void before() throws Exception {
        dbAdaptor = variantStorageEngine.getDBAdaptor();
        if (!loaded) {
            super.before();

            load();
            loaded = true;
        }
    }

    protected void load() throws Exception {
        VariantStorageEngine storageEngine = getVariantStorageEngine();
        ObjectMap options = getOptions();
        options.put(VariantStorageOptions.STATS_CALCULATE.key(), true);

        int maxStudies = 2;
        int studyId = 1;
        int release = 1;
        List<URI> inputFiles = new ArrayList<>();
        VariantStorageMetadataManager metadataManager = dbAdaptor.getMetadataManager();
        StudyMetadata studyMetadata = metadataManager.createStudy("S_" + studyId);
        for (int fileId = 12877; fileId <= 12893; fileId++) {
            String fileName = "1K.end.platinum-genomes-vcf-NA" + fileId + "_S1.genome.vcf.gz";
            URI inputFile = getResourceUri("platinum/" + fileName);
            inputFiles.add(inputFile);
            metadataManager.unsecureUpdateFileMetadata(studyMetadata.getId(), new FileMetadata(studyMetadata.getId(), fileId, fileName));
            metadataManager.registerFileSamples(studyMetadata.getId(), fileId, Collections.singletonList("NA" + fileId));

            if (inputFiles.size() == 4) {

                options.put(VariantStorageOptions.STUDY.key(), "S_" + studyId);
                storageEngine.getOptions().putAll(options);
                storageEngine.getOptions().put(VariantStorageOptions.RELEASE.key(), release++);
                storageEngine.index(inputFiles.subList(0, 2), outputUri, true, true, true);
                storageEngine.getOptions().put(VariantStorageOptions.RELEASE.key(), release++);
                storageEngine.index(inputFiles.subList(2, 4), outputUri, true, true, true);

                studyId++;
                if (studyId > maxStudies) {
                    break;
                }
                studyMetadata = metadataManager.createStudy("S_" + studyId);
                inputFiles.clear();
            }
        }
    }

    protected VariantQueryResult<Variant> query(Query query, QueryOptions options) {
        options = options == null ? QueryOptions.empty() : options;
        query = variantStorageEngine.preProcessQuery(query, options);
        return dbAdaptor.get(query, options);
    }

    protected ObjectMap getOptions() {
        return new ObjectMap();
    }

    @Test
    public void testIncludeStudies() throws Exception {
        query = new Query()
                .append(VariantQueryParam.INCLUDE_STUDY.key(), study1);
        this.queryResult = query(query, options);
        assertEquals(dbAdaptor.count(null).first().intValue(), this.queryResult.getNumResults());
        assertThat(this.queryResult, everyResult(allOf(withStudy(study2, nullValue()), withStudy("S_3", nullValue()), withStudy("S_4", nullValue()))));
    }

    @Test
    public void testIncludeStudiesAll() throws Exception {
        query = new Query(VariantQueryParam.INCLUDE_STUDY.key(), ALL);
        queryResult = query(query, options);
        VariantQueryResult<Variant> allVariants = dbAdaptor.get(new Query(), options);

        assertThat(queryResult, everyResult(allVariants, notNullValue(Variant.class)));
    }

    @Test
    public void testRelease() throws Exception {
        List<Variant> variants = query(new Query(), new QueryOptions()).getResults();
        for (Variant variant : variants) {
            Integer minFileId = variant.getStudies().stream()
                    .flatMap(s -> s.getFiles().stream())
                    .map(FileEntry::getFileId)
                    .map(s -> s.substring(30, 35))
                    .map(Integer::valueOf)
                    .min(Integer::compareTo)
                    .orElse(0);
            assertTrue(minFileId > 0);
            int expectedRelease = (minFileId - 12877/*first file loaded*/) / 2/*each release contains 2 files*/ + 1/*base-1*/;
            int release = Integer.valueOf(variant.getAnnotation().getAdditionalAttributes().get("opencga").getAttribute().get("release"));
            assertEquals(expectedRelease, release);
        }
    }

    @Test
    public void testIncludeStudiesNone() throws Exception {
        query = new Query(VariantQueryParam.INCLUDE_STUDY.key(), NONE);
        queryResult = query(query, options);

        assertEquals(dbAdaptor.count(null).first().intValue(), queryResult.getNumResults());
        assertThat(queryResult, everyResult(firstStudy(nullValue())));
    }

    @Test
    public void testIncludeSampleIdFileIdx() throws Exception {
        for (Variant variant : query(new Query(INCLUDE_FORMAT.key(),
                "all," + VariantQueryParser.SAMPLE_ID
                        + "," + VariantQueryParser.FILE_IDX
                        + "," + VariantQueryParser.FILE_ID), new QueryOptions(QueryOptions.LIMIT, 1)).getResults()) {

            for (StudyEntry study : variant.getStudies()) {
                assertEquals(Arrays.asList("GT", "GQX", "AD", "DP", "GQ", "MQ", "PL", "VF",
                        VariantQueryParser.SAMPLE_ID, VariantQueryParser.FILE_IDX, VariantQueryParser.FILE_ID), study.getFormat());
                List<String> sampleIds = study.getSamplesData()
                        .stream()
                        .map(l -> l.get(study.getFormatPositions().get(VariantQueryParser.SAMPLE_ID)))
                        .collect(Collectors.toList());
                List<String> fileIdxs = study.getSamplesData()
                        .stream()
                        .map(l -> l.get(study.getFormatPositions().get(VariantQueryParser.FILE_IDX)))
                        .collect(Collectors.toList());
                List<String> fileIds = study.getSamplesData()
                        .stream()
                        .map(l -> l.get(study.getFormatPositions().get(VariantQueryParser.FILE_ID)))
                        .collect(Collectors.toList());

                assertEquals(variant.toString(), study.getOrderedSamplesName(), sampleIds);
                for (int i = 0; i < fileIds.size(); i++) {
                    if (!fileIds.get(i).equals(".")) {
                        String expected = "1K.end.platinum-genomes-vcf-" + sampleIds.get(i) + "_S1.genome.vcf.gz";
                        assertEquals(expected, fileIds.get(i));
                        assertEquals(study.getFiles().stream().map(FileEntry::getFileId).collect(Collectors.toList()).indexOf(expected),
                                Integer.parseInt(fileIdxs.get(i)));
                    }
                }
            }
        }
    }

    @Test
    public void testIncludeSampleIdFileIdxExcludeFiles() throws Exception {
        for (Variant variant : query(new Query(INCLUDE_FORMAT.key(),
                "all," + VariantQueryParser.SAMPLE_ID
                + "," + VariantQueryParser.FILE_IDX
                + "," + VariantQueryParser.FILE_ID)
                .append(INCLUDE_FILE.key(), NONE), new QueryOptions(QueryOptions.LIMIT, 1)).getResults()) {

            for (StudyEntry study : variant.getStudies()) {
                assertEquals(Arrays.asList("GT", "GQX", "AD", "DP", "GQ", "MQ", "PL", "VF",
                        VariantQueryParser.SAMPLE_ID, VariantQueryParser.FILE_IDX, VariantQueryParser.FILE_ID), study.getFormat());
                List<String> sampleIds = study.getSamplesData()
                        .stream()
                        .map(l -> l.get(study.getFormatPositions().get(VariantQueryParser.SAMPLE_ID)))
                        .collect(Collectors.toList());

                assertEquals(variant.toString(), study.getOrderedSamplesName(), sampleIds);
                for (List<String> samplesDatum : study.getSamplesData()) {
                    assertEquals(".", samplesDatum.get(study.getFormatPositions().get(VariantQueryParser.FILE_ID)));
                    assertEquals(".", samplesDatum.get(study.getFormatPositions().get(VariantQueryParser.FILE_IDX)));
                }
            }
        }
    }

    @Test
    public void testIncludeFiles() throws Exception {
        query = new Query()
                .append(VariantQueryParam.INCLUDE_STUDY.key(), study1)
                .append(VariantQueryParam.INCLUDE_FILE.key(), file12877);
        queryResult = query(query, options);
        assertEquals(dbAdaptor.count(null).first().intValue(), queryResult.getNumResults());
        for (Variant variant : queryResult.getResults()) {
            assertTrue(variant.getStudies().size() <= 1);
            StudyEntry s_1 = variant.getStudy(study1);
            if (s_1 != null) {
                assertTrue(s_1.getFiles().size() <= 1);
                if (s_1.getFiles().size() == 1) {
                    assertNotNull(s_1.getFile(file12877));
                }
            }
            assertTrue(variant.getStudies().size() <= 1);
        }
        assertThat(queryResult, everyResult(allOf(not(withStudy(study2)), not(withStudy("S_3")), not(withStudy("S_4")))));
    }

    @Test
    public void testGetByStudies() throws Exception {
        query = new Query()
                .append(VariantQueryParam.STUDY.key(), study1);
        queryResult = query(query, options);
        VariantQueryResult<Variant> allVariants = dbAdaptor.get(new Query()
                .append(VariantQueryParam.INCLUDE_STUDY.key(), study1), options);
        assertThat(queryResult, everyResult(allVariants, withStudy(study1)));


        allVariants = dbAdaptor.get(new Query(), options);
        query = new Query().append(VariantQueryParam.STUDY.key(), study1 + AND + study2);
        queryResult = query(query, options);
        assertThat(queryResult, everyResult(allVariants, allOf(withStudy(study1), withStudy(study2))));

        query = new Query().append(VariantQueryParam.STUDY.key(), study1 + OR + study2);
        queryResult = query(query, options);
        assertThat(queryResult, everyResult(allVariants, anyOf(withStudy(study1), withStudy(study2))));
    }

    @Test
    public void testGetByStudiesNegated() throws Exception {
        query = new Query()
                .append(VariantQueryParam.STUDY.key(), study1 + AND + NOT + study2);
        queryResult = query(query, options);
        VariantQueryResult<Variant> allVariants = dbAdaptor.get(new Query()
                .append(VariantQueryParam.INCLUDE_STUDY.key(), study1 + "," + study2), options);
        assertThat(queryResult, everyResult(allVariants, allOf(withStudy(study1), not(withStudy(study2)))));
    }

    @Test
    public void testGetBySampleName() throws Exception {
        query = new Query()
                .append(VariantQueryParam.STUDY.key(), study1)
                .append(VariantQueryParam.SAMPLE.key(), "NA12877");
        queryResult = query(query, options);
        VariantQueryResult<Variant> allVariants = dbAdaptor.get(new Query()
                .append(VariantQueryParam.INCLUDE_STUDY.key(), study1)
                .append(VariantQueryParam.INCLUDE_SAMPLE.key(), "NA12877")
                .append(VariantQueryParam.INCLUDE_FILE.key(), file12877), options);
        assertThat(queryResult, everyResult(allVariants, withStudy(study1, allOf(withFileId(file12877), withSampleData("NA12877", "GT", containsString("1"))))));
    }

    @Test
    public void testGetBySampleNamesOR() throws Exception {
        query = new Query()
                .append(VariantQueryParam.STUDY.key(), study1)
                .append(VariantQueryParam.SAMPLE.key(), "NA12877,NA12878");
        queryResult = query(query, options);
        VariantQueryResult<Variant> allVariants = dbAdaptor.get(new Query()
                .append(VariantQueryParam.INCLUDE_STUDY.key(), study1)
                .append(VariantQueryParam.INCLUDE_SAMPLE.key(), "NA12877,NA12878")
                .append(VariantQueryParam.INCLUDE_FILE.key(), file12877 + "," + file12878), options);
        assertThat(queryResult, everyResult(allVariants, withStudy(study1, anyOf(
                allOf(withFileId(file12877), withSampleData("NA12877", "GT", containsString("1"))),
                allOf(withFileId(file12878), withSampleData("NA12878", "GT", containsString("1")))
        ))));
    }

    @Test
    public void testGetBySampleNamesAND() throws Exception {
        query = new Query()
                .append(VariantQueryParam.STUDY.key(), study1)
                .append(VariantQueryParam.SAMPLE.key(), "NA12877;NA12878");
        queryResult = query(query, options);
        VariantQueryResult<Variant> allVariants = dbAdaptor.get(new Query()
                .append(VariantQueryParam.INCLUDE_STUDY.key(), study1)
                .append(VariantQueryParam.INCLUDE_SAMPLE.key(), "NA12877,NA12878")
                .append(VariantQueryParam.INCLUDE_FILE.key(), file12877 + "," + file12878), options);
        assertThat(queryResult, everyResult(allVariants, withStudy(study1, allOf(
                withFileId(file12877), withSampleData("NA12877", "GT", containsString("1")),
                withFileId(file12878), withSampleData("NA12878", "GT", containsString("1"))
        ))));
    }

    @Test
    public void testGetByGenotype() throws Exception {
        VariantQueryResult<Variant> allVariants = dbAdaptor.get(new Query()
                .append(VariantQueryParam.INCLUDE_STUDY.key(), study1)
                .append(VariantQueryParam.INCLUDE_SAMPLE.key(), "NA12877")
                .append(VariantQueryParam.INCLUDE_FILE.key(), file12877), options);

        query = new Query()
                .append(VariantQueryParam.STUDY.key(), study1)
                .append(VariantQueryParam.GENOTYPE.key(), "NA12877:" + GenotypeClass.HOM_ALT);
        queryResult = query(query, options);
        assertThat(queryResult, everyResult(allVariants, withStudy(study1, allOf(withFileId(file12877), withSampleData("NA12877", "GT", anyOf(is("1/1"), is("2/2")))))));

        query = new Query()
                .append(VariantQueryParam.STUDY.key(), study1)
                .append(VariantQueryParam.GENOTYPE.key(), "NA12877:" + GenotypeClass.HET_REF);
        queryResult = query(query, options);
        assertThat(queryResult, everyResult(allVariants, withStudy(study1, allOf(withFileId(file12877), withSampleData("NA12877", "GT", anyOf(is("0/1"), is("0/2")))))));

        query = new Query()
                .append(VariantQueryParam.STUDY.key(), study1)
                .append(VariantQueryParam.GENOTYPE.key(), "NA12877:" + GenotypeClass.HET_ALT);
        queryResult = query(query, options);
        assertThat(queryResult, everyResult(allVariants, withStudy(study1, allOf(withFileId(file12877), withSampleData("NA12877", "GT", anyOf(is("1/2"), is("2/3")))))));
    }

    @Test
    public void testSampleLimitSkip() throws Exception {
        VariantQueryResult<Variant> result = query(new Query(SAMPLE_METADATA.key(), true), options);
        System.out.println("samples(ALL) = " + result.getSamples());

        for (int i : new int[]{1, 3, 6, 8, 10}) {
            result = query(new Query(VariantQueryParam.SAMPLE_SKIP.key(), i).append(SAMPLE_METADATA.key(), true), options);
//            System.out.println("samples(SKIP=" + i + ") = " + result.getSamples());
            assertEquals(Math.max(0, 8 - i), result.getSamples().values().stream().mapToInt(List::size).sum());
            assertEquals(Math.max(0, 8 - i), result.getNumSamples().intValue());
            assertEquals(8, result.getNumTotalSamples().intValue());

            result = query(new Query(VariantQueryParam.SAMPLE_LIMIT.key(), i).append(SAMPLE_METADATA.key(), true), options);
//            System.out.println("samples(LIMIT=" + i + ") = " + result.getSamples());
            assertEquals(Math.min(8, i), result.getSamples().values().stream().mapToInt(List::size).sum());
            assertEquals(Math.min(8, i), result.getNumSamples().intValue());
            assertEquals(8, result.getNumTotalSamples().intValue());
        }
    }

    @Test
    public void testSampleLimitFail() throws Exception {
        variantStorageEngine.getOptions().put(VariantStorageOptions.QUERY_SAMPLE_LIMIT_MAX.key(), 2);
        VariantQueryException e = VariantQueryException.maxLimitReached("samples", 10, 2);
        thrown.expect(e.getClass());
        thrown.expectMessage(e.getMessage());
        variantStorageEngine.get(new Query(SAMPLE_LIMIT.key(), 10), options);
    }

    @Test
    public void testLimitFail() throws Exception {
        variantStorageEngine.getOptions().put(VariantStorageOptions.QUERY_LIMIT_MAX.key(), 2);
        VariantQueryException e = VariantQueryException.maxLimitReached("variants", 10, 2);
        thrown.expect(e.getClass());
        thrown.expectMessage(e.getMessage());
        variantStorageEngine.get(new Query(), new QueryOptions(QueryOptions.LIMIT, 10));
    }

    @Test
    public void testGetByFileName() throws Exception {
        query = new Query()
                .append(VariantQueryParam.STUDY.key(), study1)
                .append(VariantQueryParam.FILE.key(), file12877);
        queryResult = query(query, options);
        VariantQueryResult<Variant> allVariants = dbAdaptor.get(new Query()
                .append(VariantQueryParam.INCLUDE_STUDY.key(), "all")
                .append(VariantQueryParam.INCLUDE_SAMPLE.key(), "NA12877")
                .append(VariantQueryParam.INCLUDE_FILE.key(), file12877), options);
        assertThat(queryResult, everyResult(allVariants, withStudy(study1, withFileId(file12877))));
    }

    @Test
    public void testGetByFileNamesOr() {
        query = new Query()
                .append(VariantQueryParam.STUDY.key(), study1)
//                .append(VariantQueryParam.SAMPLE.key(), sampleNA12877)
                .append(VariantQueryParam.FILE.key(),
                        file12877
                                + VariantQueryUtils.OR +
                                file12878);
        queryResult = query(query, options);
        VariantQueryResult<Variant> allVariants = dbAdaptor.get(new Query()
                .append(VariantQueryParam.INCLUDE_STUDY.key(), study1)
                .append(VariantQueryParam.INCLUDE_SAMPLE.key(), "NA12877,NA12878")
                .append(VariantQueryParam.INCLUDE_FILE.key(), file12877+","+file12878), options);
        assertThat(queryResult, everyResult(allVariants, withStudy(study1, anyOf(withFileId(file12877), withFileId(file12878)))));
    }

    @Test
    public void testGetByFileNamesAnd() {
        query = new Query()
                .append(VariantQueryParam.STUDY.key(), study1)
                .append(VariantQueryParam.FILE.key(),
                        file12877
                                + AND +
                                file12878);
        queryResult = query(query, options);
        VariantQueryResult<Variant> allVariants = dbAdaptor.get(new Query()
                .append(VariantQueryParam.INCLUDE_STUDY.key(), study1)
                .append(VariantQueryParam.INCLUDE_SAMPLE.key(), "NA12877,NA12878")
                .append(VariantQueryParam.INCLUDE_FILE.key(), asList(file12877 , file12878)), options);
        assertThat(queryResult, everyResult(allVariants, withStudy(study1, allOf(withFileId(file12877), withFileId(file12878)))));
    }

    @Test
    public void testGetByFileNamesAndNegated() {
        query = new Query()
                .append(VariantQueryParam.STUDY.key(), study1)
                .append(VariantQueryParam.FILE.key(),
                        file12877
                                + AND + NOT +
                                file12878)
                .append(VariantQueryParam.INCLUDE_SAMPLE.key(), "NA12877,NA12878")
                .append(VariantQueryParam.INCLUDE_FILE.key(), file12877);
        queryResult = query(query, options);
        VariantQueryResult<Variant> allVariants = dbAdaptor.get(new Query()
                .append(VariantQueryParam.INCLUDE_STUDY.key(), study1)
                .append(VariantQueryParam.INCLUDE_SAMPLE.key(), "NA12877,NA12878")
                // Return file NA12878 to determine which variants must be discarded
                .append(VariantQueryParam.INCLUDE_FILE.key(), asList(file12877 , file12878)), options);
        assertThat(queryResult, everyResult(allVariants, withStudy(study1, allOf(withFileId(file12877), not(withFileId(file12878))))));
    }

    @Test
    public void testGetByFileNamesMultiStudiesAnd() {
        query = new Query()
                .append(VariantQueryParam.STUDY.key(), study1 + "," + study2)
                .append(VariantQueryParam.FILE.key(),
                        file12877
                                + AND +
                                file12882);
        queryResult = query(query, options);
        VariantQueryResult<Variant> allVariants = dbAdaptor.get(new Query()
                .append(VariantQueryParam.INCLUDE_STUDY.key(), study1 + "," + study2)
                .append(VariantQueryParam.INCLUDE_SAMPLE.key(), "NA12877,NA12882")
                .append(VariantQueryParam.INCLUDE_FILE.key(), asList(file12877 , file12882)), options);
        assertThat(queryResult, everyResult(allVariants, allOf(withStudy(study1, withFileId(file12877)), withStudy(study2, withFileId(file12882)))));
    }

    @Test
    public void testGetAllVariants_format() {
        VariantQueryResult<Variant> allVariants = dbAdaptor.get(new Query()
                .append(VariantQueryParam.INCLUDE_STUDY.key(), study1)
                .append(VariantQueryParam.INCLUDE_SAMPLE.key(), "NA12877,NA12878")
                .append(VariantQueryParam.INCLUDE_FILE.key(), "1K.end.platinum-genomes-vcf-NA12877_S1.genome.vcf.gz,1K.end.platinum-genomes-vcf-NA12878_S1.genome.vcf.gz"), options);

        Query query = new Query(STUDY.key(), study1)
                .append(SAMPLE.key(), "NA12877,NA12878")
                .append(FORMAT.key(), "NA12877:DP<100");
        queryResult = query(query, new QueryOptions());
        System.out.println("queryResult.getNumResults() = " + queryResult.getNumResults());
        assertThat(queryResult, everyResult(allVariants, withStudy(study1, allOf(
                withSamples("NA12877", "NA12878"),
                anyOf(
                        withSampleData("NA12877", "GT", containsString("1")),
                        withSampleData("NA12878", "GT", containsString("1"))
                ),
                withSampleData("NA12877", "DP", asNumber(lt(100)))

        ))));

        query = new Query(STUDY.key(), study1)
                .append(INCLUDE_SAMPLE.key(), "NA12877,NA12878")
                .append(FORMAT.key(), "NA12877:DP<100;GT=1/1");
        queryResult = query(query, new QueryOptions());
        System.out.println("queryResult.getNumResults() = " + queryResult.getNumResults());
        assertThat(queryResult, everyResult(allVariants, withStudy(study1, allOf(
                withSamples("NA12877", "NA12878"),
                withSampleData("NA12877", "GT", is("1/1")),
                withSampleData("NA12877", "DP", asNumber(lt(100)))

        ))));

        query = new Query(STUDY.key(), study1)
                .append(INCLUDE_SAMPLE.key(), "NA12877,NA12878")
                .append(FORMAT.key(), "NA12877:DP<100;GT=1/1,0/1");
        queryResult = query(query, new QueryOptions());
        System.out.println("queryResult.getNumResults() = " + queryResult.getNumResults());
        assertThat(queryResult, everyResult(allVariants, withStudy(study1, allOf(
                withSamples("NA12877", "NA12878"),
                withSampleData("NA12877", "GT", anyOf(is("1/1"), is("0/1"))),
                withSampleData("NA12877", "DP", asNumber(lt(100)))

        ))));

        query = new Query(STUDY.key(), study1)
                .append(FORMAT.key(), "NA12877:DP<100" + OR + "NA12878:DP<50");
        queryResult = query(query, new QueryOptions());
        System.out.println("queryResult.getNumResults() = " + queryResult.getNumResults());
        assertThat(queryResult, everyResult(allVariants, withStudy(study1, allOf(
                withSamples("NA12877", "NA12878"),
                anyOf(
                        withSampleData("NA12877", "DP", asNumber(lt(100))),
                        withSampleData("NA12878", "DP", asNumber(lt(50)))
                )
        ))));

        query = new Query(STUDY.key(), study1)
                .append(FORMAT.key(), "NA12877:DP<100" + AND + "NA12878:DP<50");
        queryResult = query(query, new QueryOptions());
        System.out.println("queryResult.getNumResults() = " + queryResult.getNumResults());
        assertThat(queryResult, everyResult(allVariants, withStudy(study1, allOf(
                withSamples("NA12877", "NA12878"),
                allOf(
                        withSampleData("NA12877", "DP", asNumber(lt(100))),
                        withSampleData("NA12878", "DP", asNumber(lt(50)))
                )
        ))));

    }


    @Test
    public void testGetAllVariants_formatFail() {
        thrown.expect(VariantQueryException.class);
        thrown.expectMessage("FORMAT field \"JJ\" not found.");
        Query query = new Query(STUDY.key(), study1).append(FORMAT.key(), "NA12877:JJ<100");
        queryResult = query(query, new QueryOptions());
    }

    @Test
    public void testGetAllVariants_Info() {
        VariantQueryResult<Variant> allVariants = dbAdaptor.get(new Query()
                .append(VariantQueryParam.INCLUDE_STUDY.key(), study1)
                .append(VariantQueryParam.INCLUDE_SAMPLE.key(), "NA12877,NA12878")
                .append(VariantQueryParam.INCLUDE_FILE.key(), file12877 + "," + file12878), options);


        Query query = new Query(STUDY.key(), study1)
//                .append(INCLUDE_FILE.key(), "1K.end.platinum-genomes-vcf-NA12877_S1.genome.vcf.gz,1K.end.platinum-genomes-vcf-NA12878_S1.genome.vcf.gz")
                .append(INFO.key(), "1K.end.platinum-genomes-vcf-NA12877_S1.genome.vcf.gz:HaplotypeScore<10"
                        + ",1K.end.platinum-genomes-vcf-NA12878_S1.genome.vcf.gz:DP>100");
        queryResult = query(query, new QueryOptions());
        System.out.println("queryResult.getNumResults() = " + queryResult.getNumResults());
        assertThat(queryResult, everyResult(allVariants, withStudy(study1, allOf(
                anyOf(
                        withFileId(file12877,
                                withAttribute("HaplotypeScore", asNumber(lt(10)))
                        ),
                        withFileId(file12878,
                                withAttribute("DP", asNumber(gt(100)))
                        )
                )
        ))));

        query = new Query(STUDY.key(), study1)
//                .append(FILE.key(), "1K.end.platinum-genomes-vcf-NA12877_S1.genome.vcf.gz,1K.end.platinum-genomes-vcf-NA12878_S1.genome.vcf.gz")
                .append(INFO.key(), "1K.end.platinum-genomes-vcf-NA12877_S1.genome.vcf.gz:HaplotypeScore<10"
                        + ",1K.end.platinum-genomes-vcf-NA12878_S1.genome.vcf.gz:DP>100");
        queryResult = query(query, new QueryOptions());
        System.out.println("queryResult.getNumResults() = " + queryResult.getNumResults());
        assertThat(queryResult, everyResult(allVariants, withStudy(study1, allOf(
                anyOf(
                        withFileId(file12877,
                                withAttribute("HaplotypeScore", asNumber(lt(10)))
                        ),
                        withFileId(file12878,
                                withAttribute("DP", asNumber(gt(100)))
                        )
                )
        ))));
    }

    @Test
    public void testGetAllVariants_mixInfoFileOperators() {
        VariantQueryResult<Variant> allVariants = dbAdaptor.get(new Query()
                .append(VariantQueryParam.INCLUDE_STUDY.key(), study1)
                .append(VariantQueryParam.INCLUDE_SAMPLE.key(), "NA12877,NA12878")
                .append(VariantQueryParam.INCLUDE_FILE.key(), file12877 + "," + file12878), options);


        thrown.expect(VariantQueryException.class);
        thrown.expectMessage(VariantQueryException.mixedAndOrOperators(FILE, INFO).getMessage());

        query = new Query(STUDY.key(), study1)
                .append(FILE.key(), file12877 + OR + file12878)
                .append(INFO.key(),
                        "1K.end.platinum-genomes-vcf-NA12877_S1.genome.vcf.gz:HaplotypeScore<10"
                                + AND
                                + "1K.end.platinum-genomes-vcf-NA12878_S1.genome.vcf.gz:DP>100");
        queryResult = query(query, new QueryOptions());
        System.out.println("queryResult.getNumResults() = " + queryResult.getNumResults());
        assertThat(queryResult, everyResult(allVariants, withStudy(study1, allOf(
                withFileId(file12877,
                        withAttribute("HaplotypeScore", asNumber(lt(10)))
                ),
                withFileId(file12878,
                        withAttribute("DP", asNumber(gt(100)))
                )
        ))));

    }

    @Test
    public void testGetAllVariants_mixInfoFileOperators2() {
        VariantQueryResult<Variant> allVariants = dbAdaptor.get(new Query()
                .append(VariantQueryParam.INCLUDE_STUDY.key(), study1)
                .append(VariantQueryParam.INCLUDE_SAMPLE.key(), "NA12877,NA12878")
                .append(VariantQueryParam.INCLUDE_FILE.key(), file12877 + "," + file12878), options);

        thrown.expect(VariantQueryException.class);
        thrown.expectMessage(VariantQueryException.mixedAndOrOperators(FILE, INFO).getMessage());

        query = new Query(STUDY.key(), study1)
                .append(FILE.key(), file12877 + AND + file12878)
                .append(INFO.key(),
                        "1K.end.platinum-genomes-vcf-NA12877_S1.genome.vcf.gz:HaplotypeScore<10"
                                + OR
                                + "1K.end.platinum-genomes-vcf-NA12878_S1.genome.vcf.gz:DP>100");
        queryResult = query(query, new QueryOptions());
        System.out.println("queryResult.getNumResults() = " + queryResult.getNumResults());
        assertThat(queryResult, everyResult(allVariants, withStudy(study1, allOf(
                withFileId(file12877),
                withFileId(file12878),
                anyOf(
                        withFileId(file12877,
                                withAttribute("HaplotypeScore", asNumber(lt(10)))
                        ),
                        withFileId(file12878,
                                withAttribute("DP", asNumber(gt(100)))
                        )
                )
        ))));

    }

    @Test
    public void testGetAllVariants_infoFail() {
        thrown.expect(VariantQueryException.class);
        thrown.expectMessage("INFO field \"JJ\" not found.");
        Query query = new Query(STUDY.key(), study1).append(INFO.key(), "1K.end.platinum-genomes-vcf-NA12877_S1.genome.vcf.gz:JJ<100");
        queryResult = query(query, new QueryOptions());
    }

    @Test
    public void testGetByFileNamesMultiStudiesOr() {
        query = new Query()
                .append(VariantQueryParam.STUDY.key(), study1 + "," + study2)
                .append(VariantQueryParam.FILE.key(),
                        file12877
                                + VariantQueryUtils.OR +
                                file12882);
        queryResult = query(query, options);
        VariantQueryResult<Variant> allVariants = dbAdaptor.get(new Query()
                .append(VariantQueryParam.INCLUDE_STUDY.key(), study1 + "," + study2)
                .append(VariantQueryParam.INCLUDE_SAMPLE.key(), "NA12877,NA12882")
                .append(VariantQueryParam.INCLUDE_FILE.key(), asList(file12877 , file12882)), options);
        assertThat(queryResult, everyResult(allVariants, anyOf(withStudy(study1, withFileId(file12877)), withStudy(study2, withFileId(file12882)))));
    }

    @Test
    public void testGetByFileNamesMultiStudiesImplicitAnd() {
        query = new Query()
                .append(VariantQueryParam.FILE.key(),
                        file12877
                                + AND +
                                file12882);
        queryResult = query(query, options);
        VariantQueryResult<Variant> allVariants = dbAdaptor.get(new Query()
                .append(VariantQueryParam.INCLUDE_SAMPLE.key(), "NA12877,NA12882")
                .append(VariantQueryParam.INCLUDE_FILE.key(), asList(file12877 , file12882)), options);
        assertThat(queryResult, everyResult(allVariants, allOf(withStudy(study1, withFileId(file12877)), withStudy(study2, withFileId(file12882)))));
    }

    @Test
    public void testGetByFileNamesMultiStudiesImplicitOr() {
        query = new Query()
                .append(VariantQueryParam.FILE.key(),
                        file12877
                                + OR +
                                file12882);
        queryResult = query(query, options);
        VariantQueryResult<Variant> allVariants = dbAdaptor.get(new Query()
                .append(VariantQueryParam.INCLUDE_SAMPLE.key(), "NA12877,NA12882")
                .append(VariantQueryParam.INCLUDE_FILE.key(), asList(file12877 , file12882)), options);
        assertThat(queryResult, everyResult(allVariants, anyOf(withStudy(study1, withFileId(file12877)), withStudy(study2, withFileId(file12882)))));
    }

    @Test
    public void testGetByFilter() {
        VariantQueryResult<Variant> allVariants = dbAdaptor.get(new Query()
                .append(VariantQueryParam.INCLUDE_SAMPLE.key(), "NA12877")
                .append(VariantQueryParam.INCLUDE_FILE.key(), file12877)
                .append(VariantQueryParam.INCLUDE_STUDY.key(), study1), options);

        query = new Query()
                .append(VariantQueryParam.FILTER.key(), "LowGQX;LowMQ;LowQD;TruthSensitivityTranche99.90to100.00")
                .append(VariantQueryParam.FILE.key(), file12877)
                .append(VariantQueryParam.STUDY.key(), study1);
        queryResult = query(query, options);
        System.out.println("queryResult.getNumResults() = " + queryResult.getNumResults());
        assertThat(queryResult, everyResult(allVariants, withStudy(study1, withFileId(file12877,
                with(FILTER, fileEntry -> fileEntry.getAttributes().get(FILTER), allOf(
                        containsString("LowGQX"),
                        containsString("LowMQ"),
                        containsString("LowQD"),
                        containsString("TruthSensitivityTranche99.90to100.00")
                ))))));

        query = new Query()
                .append(VariantQueryParam.FILTER.key(), "MaxDepth")
                .append(VariantQueryParam.FILE.key(), file12877)
                .append(VariantQueryParam.STUDY.key(), study1);
        queryResult = query(query, options);
        System.out.println("queryResult.getNumResults() = " + queryResult.getNumResults());
        assertThat(queryResult, everyResult(allVariants, withStudy(study1, withFileId(file12877,
                with(FILTER, fileEntry -> fileEntry.getAttributes().get(FILTER), anyOf(
                        containsString("MaxDepth")
                )))
        )));

        query = new Query()
                .append(VariantQueryParam.FILTER.key(), "LowGQX,LowMQ")
                .append(VariantQueryParam.FILE.key(), file12877)
                .append(VariantQueryParam.STUDY.key(), study1);
        queryResult = query(query, options);
        System.out.println("queryResult.getNumResults() = " + queryResult.getNumResults());
        assertThat(queryResult, everyResult(allVariants, withStudy(study1, withFileId(file12877,
                with(FILTER, fileEntry -> fileEntry.getAttributes().get(FILTER), anyOf(
                        containsString("LowGQX"),
                        containsString("LowMQ")
                ))))));

        query = new Query()
                .append(VariantQueryParam.FILTER.key(), "\"LowGQX;LowMQ;LowQD;TruthSensitivityTranche99.90to100.00\"")
                .append(VariantQueryParam.FILE.key(), file12877)
                .append(VariantQueryParam.STUDY.key(), study1);
        queryResult = query(query, options);
        System.out.println("queryResult.getNumResults() = " + queryResult.getNumResults());
        assertThat(queryResult, everyResult(allVariants, withStudy(study1, withFileId(file12877,
                with(FILTER, fileEntry -> fileEntry.getAttributes().get(FILTER), is("LowGQX;LowMQ;LowQD;TruthSensitivityTranche99.90to100.00"))))));

        query = new Query()
                .append(VariantQueryParam.FILTER.key(), "\"LowGQX;LowMQ;LowQD;TruthSensitivityTranche99.90to100.00\",\"LowGQX;LowQD;SiteConflict\"")
                .append(VariantQueryParam.FILE.key(), file12877)
                .append(VariantQueryParam.STUDY.key(), study1);
        queryResult = query(query, options);
        System.out.println("queryResult.getNumResults() = " + queryResult.getNumResults());
        assertThat(queryResult, everyResult(allVariants, withStudy(study1, withFileId(file12877,
                with(FILTER, fileEntry -> fileEntry.getAttributes().get(FILTER), anyOf(
                        is("LowGQX;LowMQ;LowQD;TruthSensitivityTranche99.90to100.00"),
                        is("LowGQX;LowQD;SiteConflict")
                ))))));
    }

    @Test
    public void testGetByNegatedFilter() {
        VariantQueryResult<Variant> allVariants = dbAdaptor.get(new Query()
                .append(VariantQueryParam.INCLUDE_SAMPLE.key(), "NA12877")
                .append(VariantQueryParam.INCLUDE_FILE.key(), file12877), options);

        query = new Query()
                .append(VariantQueryParam.FILTER.key(), "LowGQX" + AND + "LowMQ" + AND + NOT + "SiteConflict")
                .append(VariantQueryParam.FILE.key(), file12877);
        queryResult = query(query, options);
        System.out.println("queryResult.getNumResults() = " + queryResult.getNumResults());
        assertThat(queryResult, everyResult(allVariants, withStudy(study1, withFileId(file12877,
                with(FILTER, fileEntry -> fileEntry.getAttributes().get(FILTER), allOf(
                        containsString("LowGQX"),
                        containsString("LowMQ"),
                        not(containsString("SiteConflict"))
                ))))));

        query = new Query()
                .append(VariantQueryParam.FILTER.key(), "LowGQX" + AND + "LowQD" + AND + NOT + "\"LowGQX;LowQD;SiteConflict\"")
                .append(VariantQueryParam.FILE.key(), file12877);
        queryResult = query(query, options);
        System.out.println("queryResult.getNumResults() = " + queryResult.getNumResults());
        assertThat(queryResult, everyResult(allVariants, withStudy(study1, withFileId(file12877,
                with(FILTER, fileEntry -> fileEntry.getAttributes().get(FILTER), allOf(
                        containsString("LowGQX"),
                        containsString("LowQD"),
                        not(is("LowGQX;LowQD;SiteConflict"))
                ))))));

    }

    @Test
    public void testGetByFilterMultiFile() {
        VariantQueryResult<Variant> allVariants = dbAdaptor.get(new Query()
                .append(VariantQueryParam.INCLUDE_SAMPLE.key(), "NA12877,NA12878")
                .append(VariantQueryParam.INCLUDE_FILE.key(), asList(file12877, file12878)), options);


        query = new Query()
                .append(VariantQueryParam.FILTER.key(), "LowGQX;LowMQ")
                .append(VariantQueryParam.FILE.key(),
                        file12877
                                + OR +
                                file12878);
        queryResult = query(query, options);

        assertThat(queryResult, everyResult(allVariants, withStudy(study1, anyOf(
                withFileId(file12877, with(FILTER, fileEntry -> fileEntry.getAttributes().get(FILTER), allOf(containsString("LowGQX"), containsString("LowMQ")))),
                withFileId(file12878, with(FILTER, fileEntry -> fileEntry.getAttributes().get(FILTER), allOf(containsString("LowGQX"), containsString("LowMQ"))))
        ))));

        query = new Query()
                .append(VariantQueryParam.FILTER.key(), "LowGQX;LowMQ")
                .append(VariantQueryParam.FILE.key(),
                        file12877
                                + AND +
                                file12878);
        queryResult = query(query, options);

        assertThat(queryResult, everyResult(allVariants, withStudy(study1, allOf(
                withFileId(file12877, with(FILTER, fileEntry -> fileEntry.getAttributes().get(FILTER), allOf(containsString("LowGQX"), containsString("LowMQ")))),
                withFileId(file12878, with(FILTER, fileEntry -> fileEntry.getAttributes().get(FILTER), allOf(containsString("LowGQX"), containsString("LowMQ"))))
        ))));

        query = new Query()
                .append(VariantQueryParam.FILTER.key(), "LowGQX")
                .append(VariantQueryParam.FILE.key(),
                        file12877
                                + AND +
                                file12878);
        queryResult = query(query, options);

        assertThat(queryResult, everyResult(allVariants, withStudy(study1, allOf(
                withFileId(file12877, with(FILTER, fileEntry -> fileEntry.getAttributes().get(FILTER), containsString("LowGQX"))),
                withFileId(file12878, with(FILTER, fileEntry -> fileEntry.getAttributes().get(FILTER), containsString("LowGQX")))
        ))));
    }

    @Test
    public void testGetByFilterMultiFileNegatedFiles() {
        VariantQueryResult<Variant> allVariants = dbAdaptor.get(new Query()
                .append(VariantQueryParam.INCLUDE_STUDY.key(), study1)
                .append(VariantQueryParam.INCLUDE_SAMPLE.key(), ALL)
                .append(VariantQueryParam.INCLUDE_FILE.key(), asList(file12877, file12878)), options);

        query = new Query()
                .append(VariantQueryParam.INCLUDE_STUDY.key(), study1)
                .append(VariantQueryParam.INCLUDE_SAMPLE.key(), ALL)
                .append(VariantQueryParam.FILE.key(),
                        file12877
                                + AND + NOT +
                                file12878);
        queryResult = query(query, options);

        assertThat(queryResult, everyResult(allVariants, withStudy(study1, allOf(
                withFileId(file12877),
                not(withFileId(file12878))
        ))));

        query = new Query()
                .append(VariantQueryParam.INCLUDE_SAMPLE.key(), ALL)
                .append(VariantQueryParam.STUDY.key(), 1)
                .append(VariantQueryParam.SAMPLE.key(), sampleNA12879+OR+sampleNA12880)
                .append(VariantQueryParam.FILE.key(),
                        file12877
                                + AND + NOT +
                                file12878);
        queryResult = query(query, options);

        assertThat(queryResult, everyResult(allVariants, withStudy(study1, allOf(
                anyOf(withSampleData(sampleNA12879, "GT", not("?/?")), withSampleData(sampleNA12880, "GT", not("?/?"))),
                withFileId(file12877),
                not(withFileId(file12878))
        ))));

        query = new Query()
                .append(VariantQueryParam.INCLUDE_SAMPLE.key(), ALL)
                .append(VariantQueryParam.STUDY.key(), 1)
                .append(VariantQueryParam.SAMPLE.key(), sampleNA12879 + AND + sampleNA12880)
                .append(VariantQueryParam.FILE.key(),
                        file12877
                                + AND + NOT +
                                file12878);
        queryResult = query(query, options);

        assertThat(queryResult, everyResult(allVariants, withStudy(study1, allOf(
                withSampleData(sampleNA12879, "GT", not("?/?")),
                withSampleData(sampleNA12880, "GT", not("?/?")),
                withFileId(file12877),
                not(withFileId(file12878))
        ))));

        query = new Query()
                .append(VariantQueryParam.INCLUDE_STUDY.key(), study1)
                .append(VariantQueryParam.INCLUDE_SAMPLE.key(), ALL)
                .append(VariantQueryParam.FILTER.key(), "LowGQX;LowMQ")
                .append(VariantQueryParam.FILE.key(),
                        file12877
                                + AND + NOT +
                                file12878);
        queryResult = query(query, options);

        assertThat(queryResult, everyResult(allVariants, withStudy(study1, allOf(
                withFileId(file12877, with(FILTER, fileEntry -> fileEntry.getAttributes().get(FILTER), allOf(containsString("LowGQX"), containsString("LowMQ")))),
                not(withFileId(file12878))
        ))));
    }

    @Test
    public void testGetByFilterBySample() {
        VariantQueryResult<Variant> allVariants = dbAdaptor.get(new Query()
                .append(VariantQueryParam.INCLUDE_STUDY.key(), study1)
                .append(VariantQueryParam.INCLUDE_SAMPLE.key(), sampleNA12877)
                .append(VariantQueryParam.INCLUDE_FILE.key(), asList(file12877)), options);

        query = new Query()
                .append(VariantQueryParam.STUDY.key(), study1)
                .append(VariantQueryParam.FILTER.key(), "LowGQX;LowMQ")
                .append(VariantQueryParam.SAMPLE.key(), sampleNA12877);
        queryResult = query(query, options);
        assertThat(queryResult, everyResult(allVariants, withStudy(study1, allOf(
                withFileId(file12877, withAttribute(FILTER, allOf(containsString("LowGQX"), containsString("LowMQ")))),
                withSampleData(sampleNA12877, "GT", containsString("1"))
        ))));
    }

    @Test
    public void testGetByFilterByIncludeSample() {
        VariantQueryResult<Variant> allVariants = dbAdaptor.get(new Query()
                .append(VariantQueryParam.INCLUDE_STUDY.key(), study1)
                .append(VariantQueryParam.INCLUDE_SAMPLE.key(), sampleNA12877)
                .append(VariantQueryParam.INCLUDE_FILE.key(), asList(file12877)), options);

        query = new Query()
                .append(VariantQueryParam.STUDY.key(), study1)
                .append(VariantQueryParam.FILTER.key(), "LowGQX;LowMQ")
                .append(VariantQueryParam.INCLUDE_SAMPLE.key(), sampleNA12877);
        queryResult = query(query, options);
        assertThat(queryResult, everyResult(allVariants, withStudy(study1,
                withFileId(file12877, withAttribute(FILTER, allOf(containsString("LowGQX"), containsString("LowMQ")))))));
    }

    @Test
    public void testGetByFilterMultiStudy() {
        query = new Query()
                .append(VariantQueryParam.FILTER.key(), "LowGQX;LowMQ")
                .append(VariantQueryParam.FILE.key(),
                        file12877
                                + VariantQueryUtils.OR +
                                file12882);
        queryResult = query(query, options);
        VariantQueryResult<Variant> allVariants = dbAdaptor.get(new Query()
                .append(VariantQueryParam.INCLUDE_SAMPLE.key(), "NA12877,NA12882")
                .append(VariantQueryParam.INCLUDE_FILE.key(), asList(file12877, file12882)), options);
        assertThat(queryResult, everyResult(allVariants, anyOf(
                withStudy(study1, withFileId(file12877, with(FILTER, fileEntry -> fileEntry.getAttributes().get(FILTER), allOf(containsString("LowGQX"), containsString("LowMQ"))))),
                withStudy(study2, withFileId(file12882, with(FILTER, fileEntry -> fileEntry.getAttributes().get(FILTER), allOf(containsString("LowGQX"), containsString("LowMQ")))))
        )));
    }

    @Test
    public void testGetByQual() {
        VariantQueryResult<Variant> allVariants = dbAdaptor.get(new Query()
                .append(VariantQueryParam.INCLUDE_SAMPLE.key(), "NA12877")
                .append(VariantQueryParam.INCLUDE_FILE.key(), file12877), options);

        query = new Query()
                .append(VariantQueryParam.STUDY.key(), study1)
                .append(VariantQueryParam.QUAL.key(), ">50")
                .append(VariantQueryParam.FILE.key(), file12877);
        queryResult = query(query, options);
        System.out.println("queryResult.getNumResults() = " + queryResult.getNumResults());
        System.out.println(allVariants.first().toJson());
        assertThat(queryResult, everyResult(allVariants, withStudy(study1, withFileId(file12877,
                with(QUAL, fileEntry -> fileEntry.getAttributes().get(QUAL), allOf(notNullValue(), with("", Double::valueOf, gt(50))))))));

        query = new Query()
                .append(VariantQueryParam.STUDY.key(), study1)
                .append(VariantQueryParam.QUAL.key(), "<50")
                .append(VariantQueryParam.FILE.key(), file12877);
        queryResult = query(query, options);
        System.out.println("queryResult.getNumResults() = " + queryResult.getNumResults());
        assertThat(queryResult, everyResult(allVariants, withStudy(study1, withFileId(file12877,
                with(QUAL, fileEntry -> fileEntry.getAttributes().get(QUAL), allOf(notNullValue(), with("", Double::valueOf, lt(50))))))));

        query = new Query()
                .append(VariantQueryParam.STUDY.key(), study1)
                .append(VariantQueryParam.QUAL.key(), "<<5")
                .append(VariantQueryParam.FILE.key(), file12877);
        queryResult = query(query, options);
        System.out.println("queryResult.getNumResults() = " + queryResult.getNumResults());
        assertThat(queryResult, everyResult(allVariants, withStudy(study1, withFileId(file12877,
                with(QUAL, fileEntry -> fileEntry.getAttributes().get(QUAL), anyOf(with("", Double::valueOf, lt(5)), nullValue()))))));

        query = new Query()
                .append(VariantQueryParam.STUDY.key(), study1)
                .append(VariantQueryParam.QUAL.key(), "<50")
                .append(VariantQueryParam.FILTER.key(), "LowGQX,LowMQ")
                .append(VariantQueryParam.FILE.key(), file12877);
        queryResult = query(query, options);
        System.out.println("queryResult.getNumResults() = " + queryResult.getNumResults());
        assertThat(queryResult, everyResult(allVariants, withStudy(study1, withFileId(file12877,
                allOf(
                        with(QUAL, fileEntry -> fileEntry.getAttributes().get(QUAL), allOf(notNullValue(), with("", Double::valueOf, lt(50)))),
                        with(FILTER, fileEntry -> fileEntry.getAttributes().get(FILTER), anyOf(
                                containsString("LowGQX"),
                                containsString("LowMQ")
                        )))))));

    }

    @Test
    public void testGetByStats() {
        VariantQueryResult<Variant> allVariants = query(new Query(), new QueryOptions());

        // Single study query
        queryResult = query(new Query()
                .append(STATS_ALT.key(), study1 + ":" + StudyEntry.DEFAULT_COHORT + "<0.3")
                .append(STUDY.key(), study1)
                .append(INCLUDE_STUDY.key(), ALL), null);
        assertThat(queryResult, everyResult(allVariants, withStudy(study1, withStats(StudyEntry.DEFAULT_COHORT,
                with("af", VariantStats::getAltAlleleFreq, lt(0.3))))));

        // Multi-study AND
        queryResult = query(new Query()
                .append(STATS_ALT.key(), study1 + ":" + StudyEntry.DEFAULT_COHORT + "<0.3")
                .append(STUDY.key(), study1 + ";" + study2)
                .append(INCLUDE_STUDY.key(), ALL), null);
        assertThat(queryResult, everyResult(allVariants, allOf(
                withStudy(study1,
                        withStats(StudyEntry.DEFAULT_COHORT,
                                with("af", VariantStats::getAltAlleleFreq, lt(0.3)))),
                withStudy(study2))));

        queryResult = query(new Query()
                .append(STATS_ALT.key(), study1 + ":" + StudyEntry.DEFAULT_COHORT + "<0.3;" + study2 + ":" + StudyEntry.DEFAULT_COHORT + "<0.3")
                .append(STUDY.key(), study1 + ";" + study2)
                .append(INCLUDE_STUDY.key(), ALL), null);
        assertThat(queryResult, everyResult(allVariants, allOf(
                withStudy(study1,
                        withStats(StudyEntry.DEFAULT_COHORT,
                                with("af", VariantStats::getAltAlleleFreq, lt(0.3)))),
                withStudy(study2,
                        withStats(StudyEntry.DEFAULT_COHORT,
                                with("af", VariantStats::getAltAlleleFreq, lt(0.3)))))));

        // Multi-study OR
        queryResult = query(new Query()
                .append(STATS_ALT.key(), study1 + ":" + StudyEntry.DEFAULT_COHORT + "<0.3")
                .append(STUDY.key(), study1 + "," + study2)
                .append(INCLUDE_STUDY.key(), ALL), null);
        assertThat(queryResult, everyResult(allVariants, allOf(
                anyOf(withStudy(study1), withStudy(study2)),
                anyOf(
                        not(withStudy(study1)), // Either the study is not present, or the study is present with stats
                        withStudy(study1,
                                withStats(StudyEntry.DEFAULT_COHORT,
                                        with("af", VariantStats::getAltAlleleFreq, lt(0.3))))))));

        queryResult = query(new Query()
                .append(STATS_ALT.key(), study1 + ":" + StudyEntry.DEFAULT_COHORT + "<0.3," + study2 + ":" + StudyEntry.DEFAULT_COHORT + "<0.3")
                .append(STUDY.key(), study1 + "," + study2)
                .append(INCLUDE_STUDY.key(), ALL), null);
        assertThat(queryResult, everyResult(allVariants, allOf(
                anyOf(withStudy(study1), withStudy(study2)),
                anyOf(
                        anyOf(
                                not(withStudy(study1)), // Either the study is not present, or the study is present with stats
                                withStudy(study1,
                                        withStats(StudyEntry.DEFAULT_COHORT,
                                                with("af", VariantStats::getAltAlleleFreq, lt(0.3))))),
                        anyOf(
                                not(withStudy(study2)), // Either the study is not present, or the study is present with stats
                                withStudy(study2,
                                        withStats(StudyEntry.DEFAULT_COHORT,
                                                with("af", VariantStats::getAltAlleleFreq, lt(0.3))))
                        )
                ))));
    }

    @Test
    public void testGetByRelease() {
        query = new Query().append(VariantQueryParam.RELEASE.key(), 1);
        queryResult = query(query, options);
        VariantQueryResult<Variant> allVariants = dbAdaptor.get(new Query()
                .append(VariantQueryParam.STUDY.key(), study1)
                .append(VariantQueryParam.FILE.key(), file12877 + OR + file12878)
                .append(VariantQueryParam.INCLUDE_STUDY.key(), ALL)
                .append(VariantQueryParam.INCLUDE_SAMPLE.key(), ALL)
                .append(VariantQueryParam.INCLUDE_FILE.key(), ALL), options);
        assertThat(queryResult, everyResult(allVariants, withStudy(study1, anyOf(withFileId(file12877), withFileId(file12878)))));

        query = new Query().append(VariantQueryParam.RELEASE.key(), 2);
        queryResult = query(query, options);
        allVariants = dbAdaptor.get(new Query()
                .append(VariantQueryParam.STUDY.key(), study1)
                .append(VariantQueryParam.INCLUDE_STUDY.key(), ALL)
                .append(VariantQueryParam.INCLUDE_SAMPLE.key(), ALL)
                .append(VariantQueryParam.INCLUDE_FILE.key(), ALL), options);
        assertThat(queryResult, everyResult(allVariants, withStudy(study1, anyOf(withFileId(file12877), withFileId(file12878), withFileId(file12879), withFileId(file12880)))));

    }

    @Test
    public void testFacet() throws IOException, StorageEngineException {
        Query query = new Query(STUDY.key(), study1).append(SAMPLE.key(), sampleNA12877);

        DataResult<FacetField> facet = variantStorageEngine.facet(query, new QueryOptions(QueryOptions.FACET, "chromDensity[1:10109-17539]"));
        assertEquals(variantStorageEngine.count(new Query(query).append(REGION.key(), "1:10109-17539")).first().longValue(), facet.getNumMatches());
//        System.out.println(JacksonUtils.getDefaultObjectMapper().writerWithDefaultPrettyPrinter().writeValueAsString(facet));

        facet = variantStorageEngine.facet(query, new QueryOptions(QueryOptions.FACET, "chromDensity[1:10109-17539]:500"));
//        System.out.println(JacksonUtils.getDefaultObjectMapper().writerWithDefaultPrettyPrinter().writeValueAsString(facet));

        facet = variantStorageEngine.facet(query, new QueryOptions(QueryOptions.FACET, "chromDensity[1]"));
        assertEquals(variantStorageEngine.count(new Query(query).append(REGION.key(), "1")).first().longValue(), facet.getNumMatches());
//        System.out.println(JacksonUtils.getDefaultObjectMapper().writerWithDefaultPrettyPrinter().writeValueAsString(facet));

        facet = variantStorageEngine.facet(query, new QueryOptions(QueryOptions.FACET, "chromDensity[1:10109-17539]:500>>type"));
        assertEquals(variantStorageEngine.count(new Query(query).append(REGION.key(), "1")).first().longValue(), facet.getNumMatches());
//        System.out.println(JacksonUtils.getDefaultObjectMapper().writerWithDefaultPrettyPrinter().writeValueAsString(facet));
    }

    @Test
    public void testSampleData() throws Exception {
        Variant variant;

        // 0/1 : 77, 79, 80
        // 1/1 : 78
        variant = variantStorageEngine.getSampleData("1:14907:A:G", study1, new QueryOptions()).first();
        System.out.println("variant = " + variant.toJson());
        Map<String, Integer> formats = variant.getStudies().get(0).getFormatPositions();
        assertNotNull(variant.getStudies().get(0).getStats().get(DEFAULT_COHORT));
        assertEquals(4, variant.getStudies().get(0).getSamplesData().size());
        assertEquals(4, variant.getStudies().get(0).getFiles().size());
//
        variant = variantStorageEngine.getSampleData("1:14907:A:G", study1, new QueryOptions(QueryOptions.LIMIT, 1)).first();
        System.out.println("variant = " + variant.toJson());
        assertNotNull(variant.getStudies().get(0).getStats().get(DEFAULT_COHORT));
        assertEquals(1, variant.getStudies().get(0).getSamplesData().size());
        assertEquals(sampleNA12877, variant.getStudies().get(0).getSamplesData().get(0).get(formats.get(VariantQueryParser.SAMPLE_ID)));
        assertEquals("0/1", variant.getStudies().get(0).getSamplesData().get(0).get(formats.get("GT")));
        assertEquals("0", variant.getStudies().get(0).getSamplesData().get(0).get(formats.get(VariantQueryParser.FILE_IDX)));
//        assertEquals(file12877, variant.getStudies().get(0).getSamplesData().get("0/1").get(0).getFileId());
//        assertEquals(sampleNA12878, variant.getStudies().get(0).getSamplesData().get("1/1").get(0).getId());
//        assertEquals(file12878, variant.getStudies().get(0).getSamplesData().get("1/1").get(0).getFileId());
        assertEquals(1, variant.getStudies().get(0).getFiles().size());
//
        variant = variantStorageEngine.getSampleData("1:14907:A:G", study1, new QueryOptions(QueryOptions.LIMIT, 1).append(QueryOptions.SKIP, 1)).first();
        System.out.println("sampleData = " + variant);
        assertNotNull(variant.getStudies().get(0).getStats().get(DEFAULT_COHORT));
        assertEquals(sampleNA12878, variant.getStudies().get(0).getSamplesData().get(0).get(formats.get(VariantQueryParser.SAMPLE_ID)));
        assertEquals("1/1", variant.getStudies().get(0).getSamplesData().get(0).get(formats.get("GT")));
        assertEquals("0", variant.getStudies().get(0).getSamplesData().get(0).get(formats.get(VariantQueryParser.FILE_IDX)));
        assertEquals(1, variant.getStudies().get(0).getFiles().size());

        variant = variantStorageEngine.getSampleData("1:14907:A:G", study1, new QueryOptions(VariantQueryParam.INCLUDE_SAMPLE.key(), sampleNA12878 + "," + sampleNA12879)).first();
        assertNotNull(variant.getStudies().get(0).getStats().get(DEFAULT_COHORT));
        assertEquals(2, variant.getStudies().get(0).getSamplesData().size());
        assertEquals(2, variant.getStudies().get(0).getFiles().size());
        assertEquals(sampleNA12878, variant.getStudies().get(0).getSamplesData().get(0).get(formats.get(VariantQueryParser.SAMPLE_ID)));
        assertEquals("0", variant.getStudies().get(0).getSamplesData().get(0).get(formats.get(VariantQueryParser.FILE_IDX)));
        assertEquals(sampleNA12879, variant.getStudies().get(0).getSamplesData().get(1).get(formats.get(VariantQueryParser.SAMPLE_ID)));
        assertEquals("1", variant.getStudies().get(0).getSamplesData().get(1).get(formats.get(VariantQueryParser.FILE_IDX)));

        // 0/1 : 77
        variant = variantStorageEngine.getSampleData("MT:16184:C:A", study1, new QueryOptions()).first();
        assertNotNull(variant.getStudies().get(0).getStats().get(DEFAULT_COHORT));
        assertEquals(1, variant.getStudies().get(0).getSamplesData().size());
        assertEquals(sampleNA12877, variant.getStudies().get(0).getSamplesData().get(0).get(formats.get(VariantQueryParser.SAMPLE_ID)));
        assertEquals("0/1", variant.getStudies().get(0).getSamplesData().get(0).get(formats.get("GT")));
        assertEquals("0", variant.getStudies().get(0).getSamplesData().get(0).get(formats.get(VariantQueryParser.FILE_IDX)));
        assertEquals(1, variant.getStudies().get(0).getFiles().size());

        variant = variantStorageEngine.getSampleData("MT:16184:C:A", study1, new QueryOptions(QueryOptions.LIMIT, 1)).first();
        assertNotNull(variant.getStudies().get(0).getStats().get(DEFAULT_COHORT));
        assertEquals(1, variant.getStudies().get(0).getSamplesData().size());
        assertEquals(sampleNA12877, variant.getStudies().get(0).getSamplesData().get(0).get(formats.get(VariantQueryParser.SAMPLE_ID)));
        assertEquals("0/1", variant.getStudies().get(0).getSamplesData().get(0).get(formats.get("GT")));
        assertEquals("0", variant.getStudies().get(0).getSamplesData().get(0).get(formats.get(VariantQueryParser.FILE_IDX)));
        assertEquals(1, variant.getStudies().get(0).getFiles().size());

        variant = variantStorageEngine.getSampleData("MT:16184:C:A", study1, new QueryOptions(QueryOptions.LIMIT, 1).append(QueryOptions.SKIP, 1)).first();
        assertNotNull(variant.getStudies().get(0).getStats().get(DEFAULT_COHORT));
        assertEquals(0, variant.getStudies().get(0).getSamplesData().size());
        assertEquals(0, variant.getStudies().get(0).getFiles().size());
    }


    @Test
    public void testCount() throws StorageEngineException {
        checkCount(new Query(STUDY.key(), study1));
        checkCount(new Query(STUDY.key(), study1).append(SAMPLE.key(), sampleNA12877));
    }

    @Test
    public void testCount1() throws StorageEngineException {
        checkCount(new Query(STUDY.key(), study1));
    }

    @Test
    public void testCount2() throws StorageEngineException {
        checkCount(new Query(STUDY.key(), study1).append(SAMPLE.key(), sampleNA12877));
    }

    public void checkCount(Query query) throws StorageEngineException {
        long expected = variantStorageEngine.count(query).first();

        VariantQueryResult<Variant> result;
        result = variantStorageEngine.get(query, new QueryOptions(QueryOptions.COUNT, false).append(QueryOptions.LIMIT, 1));
        System.out.println(query.toJson());
        System.out.println("source = " + result.getSource());
        assertEquals(1, result.getNumResults());
        assertEquals(1, result.getResults().size());
        assertEquals(-1, result.getNumMatches());

        result = variantStorageEngine.get(query, new QueryOptions(QueryOptions.COUNT, true).append(QueryOptions.LIMIT, 1));
        System.out.println(query.toJson());
        System.out.println("source = " + result.getSource());
        assertEquals(1, result.getNumResults());
        assertEquals(1, result.getResults().size());
        assertEquals(expected, result.getNumMatches());

        result = variantStorageEngine.get(query, new QueryOptions(QueryOptions.COUNT, true).append(QueryOptions.LIMIT, 0));
        System.out.println(query.toJson());
        System.out.println("source = " + result.getSource());
        assertEquals(0, result.getNumResults());
        assertEquals(0, result.getResults().size());
        assertEquals(expected, result.getNumMatches());

        result = variantStorageEngine.get(query, new QueryOptions(QueryOptions.COUNT, true)
                .append(VariantStorageOptions.APPROXIMATE_COUNT.key(), true)
                .append(VariantStorageOptions.APPROXIMATE_COUNT_SAMPLING_SIZE.key(), 1000)
                .append(QueryOptions.LIMIT, 1));
        System.out.println(query.toJson());
        System.out.println("source = " + result.getSource());
        assertEquals(1, result.getNumResults());
        assertEquals(1, result.getResults().size());
        assertEquals(expected, result.getNumMatches());

        result = variantStorageEngine.get(query, new QueryOptions(QueryOptions.COUNT, true)
                .append(VariantStorageOptions.APPROXIMATE_COUNT.key(), true)
                .append(VariantStorageOptions.APPROXIMATE_COUNT_SAMPLING_SIZE.key(), 1000)
                .append(QueryOptions.LIMIT, 0));
        System.out.println(query.toJson());
        System.out.println("source = " + result.getSource());
        assertEquals(0, result.getNumResults());
        assertEquals(0, result.getResults().size());
        assertEquals(expected, result.getNumMatches());
    }

}
