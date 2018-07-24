package org.opencb.opencga.storage.core.variant.adaptors;

import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;
import org.opencb.biodata.models.variant.StudyEntry;
import org.opencb.biodata.models.variant.Variant;
import org.opencb.biodata.models.variant.avro.FileEntry;
import org.opencb.commons.datastore.core.ObjectMap;
import org.opencb.commons.datastore.core.Query;
import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.opencga.core.results.VariantQueryResult;
import org.opencb.opencga.storage.core.metadata.StudyConfiguration;
import org.opencb.opencga.storage.core.variant.VariantStorageBaseTest;
import org.opencb.opencga.storage.core.variant.VariantStorageEngine;

import java.net.URI;
import java.util.ArrayList;
import java.util.List;

import static java.util.Arrays.asList;
import static org.hamcrest.CoreMatchers.*;
import static org.junit.Assert.*;
import static org.opencb.biodata.models.variant.StudyEntry.FILTER;
import static org.opencb.biodata.models.variant.StudyEntry.QUAL;
import static org.opencb.opencga.storage.core.variant.adaptors.VariantMatchers.*;
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

            VariantStorageEngine storageEngine = getVariantStorageEngine();
            ObjectMap options = getOptions();

            int maxStudies = 2;
            int studyId = 1;
            int release = 1;
            List<URI> inputFiles = new ArrayList<>();
            StudyConfiguration studyConfiguration = new StudyConfiguration(studyId, "S_" + studyId);
            for (int fileId = 12877; fileId <= 12893; fileId++) {
                String fileName = "1K.end.platinum-genomes-vcf-NA" + fileId + "_S1.genome.vcf.gz";
                URI inputFile = getResourceUri("platinum/" + fileName);
                inputFiles.add(inputFile);
                studyConfiguration.getFileIds().put(fileName, fileId);
                studyConfiguration.getSampleIds().put("NA" + fileId, fileId);
                if (inputFiles.size() == 4) {
                    dbAdaptor.getStudyConfigurationManager().updateStudyConfiguration(studyConfiguration, null);
                    options.put(VariantStorageEngine.Options.STUDY.key(), "S_" + studyId);
                    storageEngine.getOptions().putAll(options);
                    storageEngine.getOptions().put(VariantStorageEngine.Options.RELEASE.key(), release++);
                    storageEngine.index(inputFiles.subList(0, 2), outputUri, true, true, true);
                    storageEngine.getOptions().put(VariantStorageEngine.Options.RELEASE.key(), release++);
                    storageEngine.index(inputFiles.subList(2, 4), outputUri, true, true, true);

                    studyId++;
                    studyConfiguration = new StudyConfiguration(studyId, "S_" + studyId);
                    inputFiles.clear();
                    if (studyId > maxStudies) {
                        break;
                    }
                }
            }
            loaded = true;
        }
    }

    protected ObjectMap getOptions() {
        return new ObjectMap();
    }

    @Test
    public void testIncludeStudies() throws Exception {
        query = new Query()
                .append(VariantQueryParam.INCLUDE_STUDY.key(), "S_1");
        queryResult = dbAdaptor.get(query, options);
        assertEquals(dbAdaptor.count(null).first().intValue(), queryResult.getNumResults());
        assertThat(queryResult, everyResult(allOf(withStudy("S_2", nullValue()), withStudy("S_3", nullValue()), withStudy("S_4", nullValue()))));
    }

    @Test
    public void testIncludeStudiesAll() throws Exception {
        query = new Query(VariantQueryParam.INCLUDE_STUDY.key(), ALL);
        queryResult = dbAdaptor.get(query, options);
        VariantQueryResult<Variant> allVariants = dbAdaptor.get(new Query(), options);

        assertThat(queryResult, everyResult(allVariants, notNullValue(Variant.class)));
    }

    @Test
    public void testRelease() throws Exception {
        for (Variant variant : dbAdaptor) {
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
        queryResult = dbAdaptor.get(query, options);

        assertEquals(dbAdaptor.count(null).first().intValue(), queryResult.getNumResults());
        assertThat(queryResult, everyResult(firstStudy(nullValue())));
    }

    @Test
    public void testIncludeFiles() throws Exception {
        query = new Query()
                .append(VariantQueryParam.INCLUDE_STUDY.key(), "S_1")
                .append(VariantQueryParam.INCLUDE_FILE.key(), file12877);
        queryResult = dbAdaptor.get(query, options);
        assertEquals(dbAdaptor.count(null).first().intValue(), queryResult.getNumResults());
        for (Variant variant : queryResult.getResult()) {
            assertTrue(variant.getStudies().size() <= 1);
            StudyEntry s_1 = variant.getStudy("S_1");
            if (s_1 != null) {
                assertTrue(s_1.getFiles().size() <= 1);
                if (s_1.getFiles().size() == 1) {
                    assertNotNull(s_1.getFile(file12877));
                }
            }
            assertTrue(variant.getStudies().size() <= 1);
        }
        assertThat(queryResult, everyResult(allOf(not(withStudy("S_2")), not(withStudy("S_3")), not(withStudy("S_4")))));
    }

    @Test
    public void testGetByStudies() throws Exception {
        query = new Query()
                .append(VariantQueryParam.STUDY.key(), "S_1");
        queryResult = dbAdaptor.get(query, options);
        VariantQueryResult<Variant> allVariants = dbAdaptor.get(new Query()
                .append(VariantQueryParam.INCLUDE_STUDY.key(), "S_1"), options);
        assertThat(queryResult, everyResult(allVariants, withStudy("S_1")));
    }

    @Test
    public void testGetByStudiesNegated() throws Exception {
        query = new Query()
                .append(VariantQueryParam.STUDY.key(), "S_1" + AND + NOT + "S_2");
        queryResult = dbAdaptor.get(query, options);
        VariantQueryResult<Variant> allVariants = dbAdaptor.get(new Query()
                .append(VariantQueryParam.INCLUDE_STUDY.key(), "S_1,S_2"), options);
        assertThat(queryResult, everyResult(allVariants, allOf(withStudy("S_1"), not(withStudy("S_2")))));
    }

    @Test
    public void testGetBySampleName() throws Exception {
        query = new Query()
                .append(VariantQueryParam.STUDY.key(), "S_1")
                .append(VariantQueryParam.SAMPLE.key(), "NA12877");
        queryResult = dbAdaptor.get(query, options);
        VariantQueryResult<Variant> allVariants = dbAdaptor.get(new Query()
                .append(VariantQueryParam.INCLUDE_STUDY.key(), "S_1")
                .append(VariantQueryParam.INCLUDE_SAMPLE.key(), "NA12877")
                .append(VariantQueryParam.INCLUDE_FILE.key(), file12877), options);
        assertThat(queryResult, everyResult(allVariants, withStudy("S_1", allOf(withFileId(file12877), withSampleData("NA12877", "GT", containsString("1"))))));
    }

    @Test
    public void testGetByGenotype() throws Exception {
        VariantQueryResult<Variant> allVariants = dbAdaptor.get(new Query()
                .append(VariantQueryParam.INCLUDE_STUDY.key(), "S_1")
                .append(VariantQueryParam.INCLUDE_SAMPLE.key(), "NA12877")
                .append(VariantQueryParam.INCLUDE_FILE.key(), file12877), options);

        query = new Query()
                .append(VariantQueryParam.STUDY.key(), "S_1")
                .append(VariantQueryParam.GENOTYPE.key(), "NA12877:" + GenotypeClass.HOM_ALT);
        queryResult = dbAdaptor.get(query, options);
        assertThat(queryResult, everyResult(allVariants, withStudy("S_1", allOf(withFileId(file12877), withSampleData("NA12877", "GT", anyOf(is("1/1"), is("2/2")))))));

        query = new Query()
                .append(VariantQueryParam.STUDY.key(), "S_1")
                .append(VariantQueryParam.GENOTYPE.key(), "NA12877:" + GenotypeClass.HET_REF);
        queryResult = dbAdaptor.get(query, options);
        assertThat(queryResult, everyResult(allVariants, withStudy("S_1", allOf(withFileId(file12877), withSampleData("NA12877", "GT", anyOf(is("0/1"), is("0/2")))))));

        query = new Query()
                .append(VariantQueryParam.STUDY.key(), "S_1")
                .append(VariantQueryParam.GENOTYPE.key(), "NA12877:" + GenotypeClass.HET_ALT);
        queryResult = dbAdaptor.get(query, options);
        assertThat(queryResult, everyResult(allVariants, withStudy("S_1", allOf(withFileId(file12877), withSampleData("NA12877", "GT", anyOf(is("1/2"), is("2/3")))))));
    }

    @Test
    public void testGetByFileName() throws Exception {
        query = new Query()
//                .append(VariantQueryParam.STUDY.key(), "S_1")
                .append(VariantQueryParam.FILE.key(), file12877);
        queryResult = dbAdaptor.get(query, options);
        VariantQueryResult<Variant> allVariants = dbAdaptor.get(new Query()
                .append(VariantQueryParam.INCLUDE_STUDY.key(), "all")
                .append(VariantQueryParam.INCLUDE_SAMPLE.key(), "NA12877")
                .append(VariantQueryParam.INCLUDE_FILE.key(), file12877), options);
        assertThat(queryResult, everyResult(allVariants, withStudy("S_1", withFileId(file12877))));
    }

    @Test
    public void testGetByFileNamesOr() {
        query = new Query()
                .append(VariantQueryParam.STUDY.key(), "S_1")
                .append(VariantQueryParam.FILE.key(),
                        file12877
                                + VariantQueryUtils.OR +
                                file12878);
        queryResult = dbAdaptor.get(query, options);
        VariantQueryResult<Variant> allVariants = dbAdaptor.get(new Query()
                .append(VariantQueryParam.INCLUDE_STUDY.key(), "S_1")
                .append(VariantQueryParam.INCLUDE_SAMPLE.key(), "NA12877,NA12878")
                .append(VariantQueryParam.INCLUDE_FILE.key(), file12877+","+file12878), options);
        assertThat(queryResult, everyResult(allVariants, withStudy("S_1", anyOf(withFileId(file12877), withFileId(file12878)))));
    }

    @Test
    public void testGetByFileNamesAnd() {
        query = new Query()
                .append(VariantQueryParam.STUDY.key(), "S_1")
                .append(VariantQueryParam.FILE.key(),
                        file12877
                                + AND +
                                file12878);
        queryResult = dbAdaptor.get(query, options);
        VariantQueryResult<Variant> allVariants = dbAdaptor.get(new Query()
                .append(VariantQueryParam.INCLUDE_STUDY.key(), "S_1")
                .append(VariantQueryParam.INCLUDE_SAMPLE.key(), "NA12877,NA12878")
                .append(VariantQueryParam.INCLUDE_FILE.key(), asList(file12877 , file12878)), options);
        assertThat(queryResult, everyResult(allVariants, withStudy("S_1", allOf(withFileId(file12877), withFileId(file12878)))));
    }

    @Test
    public void testGetByFileNamesAndNegated() {
        query = new Query()
                .append(VariantQueryParam.STUDY.key(), "S_1")
                .append(VariantQueryParam.FILE.key(),
                        file12877
                                + AND + NOT +
                                file12878)
                .append(VariantQueryParam.INCLUDE_SAMPLE.key(), "NA12877,NA12878")
                .append(VariantQueryParam.INCLUDE_FILE.key(), file12877);
        queryResult = dbAdaptor.get(query, options);
        VariantQueryResult<Variant> allVariants = dbAdaptor.get(new Query()
                .append(VariantQueryParam.INCLUDE_STUDY.key(), "S_1")
                .append(VariantQueryParam.INCLUDE_SAMPLE.key(), "NA12877,NA12878")
                // Return file NA12878 to determine which variants must be discarded
                .append(VariantQueryParam.INCLUDE_FILE.key(), asList(file12877 , file12878)), options);
        assertThat(queryResult, everyResult(allVariants, withStudy("S_1", allOf(withFileId(file12877), not(withFileId(file12878))))));
    }

    @Test
    public void testGetByFileNamesMultiStudiesAnd() {
        query = new Query()
                .append(VariantQueryParam.STUDY.key(), "S_1,S_2")
                .append(VariantQueryParam.FILE.key(),
                        file12877
                                + AND +
                                file12882);
        queryResult = dbAdaptor.get(query, options);
        VariantQueryResult<Variant> allVariants = dbAdaptor.get(new Query()
                .append(VariantQueryParam.INCLUDE_STUDY.key(), "S_1,S_2")
                .append(VariantQueryParam.INCLUDE_SAMPLE.key(), "NA12877,NA12882")
                .append(VariantQueryParam.INCLUDE_FILE.key(), asList(file12877 , file12882)), options);
        assertThat(queryResult, everyResult(allVariants, allOf(withStudy("S_1", withFileId(file12877)), withStudy("S_2", withFileId(file12882)))));
    }

    @Test
    public void testGetByFileNamesMultiStudiesOr() {
        query = new Query()
                .append(VariantQueryParam.STUDY.key(), "S_1,S_2")
                .append(VariantQueryParam.FILE.key(),
                        file12877
                                + VariantQueryUtils.OR +
                                file12882);
        queryResult = dbAdaptor.get(query, options);
        VariantQueryResult<Variant> allVariants = dbAdaptor.get(new Query()
                .append(VariantQueryParam.INCLUDE_STUDY.key(), "S_1,S_2")
                .append(VariantQueryParam.INCLUDE_SAMPLE.key(), "NA12877,NA12882")
                .append(VariantQueryParam.INCLUDE_FILE.key(), asList(file12877 , file12882)), options);
        assertThat(queryResult, everyResult(allVariants, anyOf(withStudy("S_1", withFileId(file12877)), withStudy("S_2", withFileId(file12882)))));
    }

    @Test
    public void testGetByFileNamesMultiStudiesImplicitAnd() {
        query = new Query()
                .append(VariantQueryParam.FILE.key(),
                        file12877
                                + AND +
                                file12882);
        queryResult = dbAdaptor.get(query, options);
        VariantQueryResult<Variant> allVariants = dbAdaptor.get(new Query()
                .append(VariantQueryParam.INCLUDE_SAMPLE.key(), "NA12877,NA12882")
                .append(VariantQueryParam.INCLUDE_FILE.key(), asList(file12877 , file12882)), options);
        assertThat(queryResult, everyResult(allVariants, allOf(withStudy("S_1", withFileId(file12877)), withStudy("S_2", withFileId(file12882)))));
    }

    @Test
    public void testGetByFileNamesMultiStudiesImplicitOr() {
        query = new Query()
                .append(VariantQueryParam.FILE.key(),
                        file12877
                                + OR +
                                file12882);
        queryResult = dbAdaptor.get(query, options);
        VariantQueryResult<Variant> allVariants = dbAdaptor.get(new Query()
                .append(VariantQueryParam.INCLUDE_SAMPLE.key(), "NA12877,NA12882")
                .append(VariantQueryParam.INCLUDE_FILE.key(), asList(file12877 , file12882)), options);
        assertThat(queryResult, everyResult(allVariants, anyOf(withStudy("S_1", withFileId(file12877)), withStudy("S_2", withFileId(file12882)))));
    }

    @Test
    public void testGetByFilter() {
        VariantQueryResult<Variant> allVariants = dbAdaptor.get(new Query()
                .append(VariantQueryParam.INCLUDE_SAMPLE.key(), "NA12877")
                .append(VariantQueryParam.INCLUDE_FILE.key(), file12877), options);

        query = new Query()
                .append(VariantQueryParam.FILTER.key(), "LowGQX;LowMQ;LowQD;TruthSensitivityTranche99.90to100.00")
                .append(VariantQueryParam.FILE.key(), file12877);
        queryResult = dbAdaptor.get(query, options);
        System.out.println("queryResult.getNumResults() = " + queryResult.getNumResults());
        assertThat(queryResult, everyResult(allVariants, withStudy("S_1", withFileId(file12877,
                with(FILTER, fileEntry -> fileEntry.getAttributes().get(FILTER), allOf(
                        containsString("LowGQX"),
                        containsString("LowMQ"),
                        containsString("LowQD"),
                        containsString("TruthSensitivityTranche99.90to100.00")
                ))))));

        query = new Query()
                .append(VariantQueryParam.FILTER.key(), "LowGQX,LowMQ")
                .append(VariantQueryParam.FILE.key(), file12877);
        queryResult = dbAdaptor.get(query, options);
        System.out.println("queryResult.getNumResults() = " + queryResult.getNumResults());
        assertThat(queryResult, everyResult(allVariants, withStudy("S_1", withFileId(file12877,
                with(FILTER, fileEntry -> fileEntry.getAttributes().get(FILTER), anyOf(
                        containsString("LowGQX"),
                        containsString("LowMQ")
                ))))));

        query = new Query()
                .append(VariantQueryParam.FILTER.key(), "\"LowGQX;LowMQ;LowQD;TruthSensitivityTranche99.90to100.00\"")
                .append(VariantQueryParam.FILE.key(), file12877);
        queryResult = dbAdaptor.get(query, options);
        System.out.println("queryResult.getNumResults() = " + queryResult.getNumResults());
        assertThat(queryResult, everyResult(allVariants, withStudy("S_1", withFileId(file12877,
                with(FILTER, fileEntry -> fileEntry.getAttributes().get(FILTER), is("LowGQX;LowMQ;LowQD;TruthSensitivityTranche99.90to100.00"))))));

        query = new Query()
                .append(VariantQueryParam.FILTER.key(), "\"LowGQX;LowMQ;LowQD;TruthSensitivityTranche99.90to100.00\",\"LowGQX;LowQD;SiteConflict\"")
                .append(VariantQueryParam.FILE.key(), file12877);
        queryResult = dbAdaptor.get(query, options);
        System.out.println("queryResult.getNumResults() = " + queryResult.getNumResults());
        assertThat(queryResult, everyResult(allVariants, withStudy("S_1", withFileId(file12877,
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
        queryResult = dbAdaptor.get(query, options);
        System.out.println("queryResult.getNumResults() = " + queryResult.getNumResults());
        assertThat(queryResult, everyResult(allVariants, withStudy("S_1", withFileId(file12877,
                with(FILTER, fileEntry -> fileEntry.getAttributes().get(FILTER), allOf(
                        containsString("LowGQX"),
                        containsString("LowMQ"),
                        not(containsString("SiteConflict"))
                ))))));

        query = new Query()
                .append(VariantQueryParam.FILTER.key(), "LowGQX" + AND + "LowQD" + AND + NOT + "\"LowGQX;LowQD;SiteConflict\"")
                .append(VariantQueryParam.FILE.key(), file12877);
        queryResult = dbAdaptor.get(query, options);
        System.out.println("queryResult.getNumResults() = " + queryResult.getNumResults());
        assertThat(queryResult, everyResult(allVariants, withStudy("S_1", withFileId(file12877,
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
        queryResult = dbAdaptor.get(query, options);

        assertThat(queryResult, everyResult(allVariants, withStudy("S_1", anyOf(
                withFileId(file12877, with(FILTER, fileEntry -> fileEntry.getAttributes().get(FILTER), allOf(containsString("LowGQX"), containsString("LowMQ")))),
                withFileId(file12878, with(FILTER, fileEntry -> fileEntry.getAttributes().get(FILTER), allOf(containsString("LowGQX"), containsString("LowMQ"))))
        ))));

        query = new Query()
                .append(VariantQueryParam.FILTER.key(), "LowGQX;LowMQ")
                .append(VariantQueryParam.FILE.key(),
                        file12877
                                + AND +
                                file12878);
        queryResult = dbAdaptor.get(query, options);

        assertThat(queryResult, everyResult(allVariants, withStudy("S_1", allOf(
                withFileId(file12877, with(FILTER, fileEntry -> fileEntry.getAttributes().get(FILTER), allOf(containsString("LowGQX"), containsString("LowMQ")))),
                withFileId(file12878, with(FILTER, fileEntry -> fileEntry.getAttributes().get(FILTER), allOf(containsString("LowGQX"), containsString("LowMQ"))))
        ))));

        query = new Query()
                .append(VariantQueryParam.FILTER.key(), "LowGQX")
                .append(VariantQueryParam.FILE.key(),
                        file12877
                                + AND +
                                file12878);
        queryResult = dbAdaptor.get(query, options);

        assertThat(queryResult, everyResult(allVariants, withStudy("S_1", allOf(
                withFileId(file12877, with(FILTER, fileEntry -> fileEntry.getAttributes().get(FILTER), containsString("LowGQX"))),
                withFileId(file12878, with(FILTER, fileEntry -> fileEntry.getAttributes().get(FILTER), containsString("LowGQX")))
        ))));
    }

    @Test
    public void testGetByFilterMultiFileNegatedFiles() {
        VariantQueryResult<Variant> allVariants = dbAdaptor.get(new Query()
                .append(VariantQueryParam.INCLUDE_SAMPLE.key(), "NA12877")
                .append(VariantQueryParam.INCLUDE_FILE.key(), asList(file12877, file12878)), options);


        query = new Query()
                .append(VariantQueryParam.FILTER.key(), "LowGQX;LowMQ")
                .append(VariantQueryParam.FILE.key(),
                        file12877
                                + AND + NOT +
                                file12878);
        queryResult = dbAdaptor.get(query, options);

        assertThat(queryResult, everyResult(allVariants, withStudy("S_1", allOf(
                withFileId(file12877, with(FILTER, fileEntry -> fileEntry.getAttributes().get(FILTER), allOf(containsString("LowGQX"), containsString("LowMQ")))),
                not(withFileId(file12878))
        ))));
    }

    @Test
    public void testGetByFilterMultiStudy() {
        query = new Query()
                .append(VariantQueryParam.FILTER.key(), "LowGQX;LowMQ")
                .append(VariantQueryParam.FILE.key(),
                        file12877
                                + VariantQueryUtils.OR +
                                file12882);
        queryResult = dbAdaptor.get(query, options);
        VariantQueryResult<Variant> allVariants = dbAdaptor.get(new Query()
                .append(VariantQueryParam.INCLUDE_SAMPLE.key(), "NA12877,NA12882")
                .append(VariantQueryParam.INCLUDE_FILE.key(), asList(file12877, file12882)), options);
        assertThat(queryResult, everyResult(allVariants, anyOf(
                withStudy("S_1", withFileId(file12877, with(FILTER, fileEntry -> fileEntry.getAttributes().get(FILTER), allOf(containsString("LowGQX"), containsString("LowMQ"))))),
                withStudy("S_2", withFileId(file12882, with(FILTER, fileEntry -> fileEntry.getAttributes().get(FILTER), allOf(containsString("LowGQX"), containsString("LowMQ")))))
        )));
    }

    @Test
    public void testGetByQual() {
        VariantQueryResult<Variant> allVariants = dbAdaptor.get(new Query()
                .append(VariantQueryParam.INCLUDE_SAMPLE.key(), "NA12877")
                .append(VariantQueryParam.INCLUDE_FILE.key(), file12877), options);

        query = new Query()
                .append(VariantQueryParam.QUAL.key(), ">50")
                .append(VariantQueryParam.FILE.key(), file12877);
        queryResult = dbAdaptor.get(query, options);
        System.out.println("queryResult.getNumResults() = " + queryResult.getNumResults());
        System.out.println(allVariants.first().toJson());
        assertThat(queryResult, everyResult(allVariants, withStudy("S_1", withFileId(file12877,
                with(QUAL, fileEntry -> fileEntry.getAttributes().get(QUAL), allOf(notNullValue(), with("", Double::valueOf, gt(50))))))));

        query = new Query()
                .append(VariantQueryParam.QUAL.key(), "<50")
                .append(VariantQueryParam.FILE.key(), file12877);
        queryResult = dbAdaptor.get(query, options);
        System.out.println("queryResult.getNumResults() = " + queryResult.getNumResults());
        assertThat(queryResult, everyResult(allVariants, withStudy("S_1", withFileId(file12877,
                with(QUAL, fileEntry -> fileEntry.getAttributes().get(QUAL), allOf(notNullValue(), with("", Double::valueOf, lt(50))))))));

        query = new Query()
                .append(VariantQueryParam.QUAL.key(), "<<5")
                .append(VariantQueryParam.FILE.key(), file12877);
        queryResult = dbAdaptor.get(query, options);
        System.out.println("queryResult.getNumResults() = " + queryResult.getNumResults());
        assertThat(queryResult, everyResult(allVariants, withStudy("S_1", withFileId(file12877,
                with(QUAL, fileEntry -> fileEntry.getAttributes().get(QUAL), anyOf(with("", Double::valueOf, lt(5)), nullValue()))))));

        query = new Query()
                .append(VariantQueryParam.QUAL.key(), "<50")
                .append(VariantQueryParam.FILTER.key(), "LowGQX,LowMQ")
                .append(VariantQueryParam.FILE.key(), file12877);
        queryResult = dbAdaptor.get(query, options);
        System.out.println("queryResult.getNumResults() = " + queryResult.getNumResults());
        assertThat(queryResult, everyResult(allVariants, withStudy("S_1", withFileId(file12877,
                allOf(
                        with(QUAL, fileEntry -> fileEntry.getAttributes().get(QUAL), allOf(notNullValue(), with("", Double::valueOf, lt(50)))),
                        with(FILTER, fileEntry -> fileEntry.getAttributes().get(FILTER), anyOf(
                                containsString("LowGQX"),
                                containsString("LowMQ")
                        )))))));

    }

    @Test
    public void testGetByRelease() {
        query = new Query().append(VariantQueryParam.RELEASE.key(), 1);
        queryResult = dbAdaptor.get(query, options);
        VariantQueryResult<Variant> allVariants = dbAdaptor.get(new Query()
                .append(VariantQueryParam.STUDY.key(), "S_1")
                .append(VariantQueryParam.FILE.key(), file12877 + OR + file12878)
                .append(VariantQueryParam.INCLUDE_STUDY.key(), ALL)
                .append(VariantQueryParam.INCLUDE_SAMPLE.key(), ALL)
                .append(VariantQueryParam.INCLUDE_FILE.key(), ALL), options);
        assertThat(queryResult, everyResult(allVariants, withStudy("S_1", anyOf(withFileId(file12877), withFileId(file12878)))));

        query = new Query().append(VariantQueryParam.RELEASE.key(), 2);
        queryResult = dbAdaptor.get(query, options);
        allVariants = dbAdaptor.get(new Query()
                .append(VariantQueryParam.STUDY.key(), "S_1")
                .append(VariantQueryParam.INCLUDE_STUDY.key(), ALL)
                .append(VariantQueryParam.INCLUDE_SAMPLE.key(), ALL)
                .append(VariantQueryParam.INCLUDE_FILE.key(), ALL), options);
        assertThat(queryResult, everyResult(allVariants, withStudy("S_1", anyOf(withFileId(file12877), withFileId(file12878), withFileId(file12879), withFileId(file12880)))));

    }
}
