package org.opencb.opencga.storage.core.manager.variant;

import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.opencb.biodata.models.variant.StudyEntry;
import org.opencb.commons.datastore.core.Query;
import org.opencb.opencga.catalog.exceptions.CatalogException;
import org.opencb.opencga.catalog.managers.CatalogManager;
import org.opencb.opencga.catalog.managers.CatalogManagerExternalResource;
import org.opencb.opencga.core.models.*;
import org.opencb.opencga.storage.core.variant.adaptors.VariantQueryException;
import org.opencb.opencga.storage.core.variant.adaptors.VariantQueryParam;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.opencb.opencga.storage.core.variant.adaptors.VariantQueryParam.FILE;
import static org.opencb.opencga.storage.core.variant.adaptors.VariantQueryParam.GENOTYPE;
import static org.opencb.opencga.storage.core.variant.adaptors.VariantQueryParam.STUDY;

/**
 * Created on 12/12/17.
 *
 * @author Jacobo Coll &lt;jacobo167@gmail.com&gt;
 */
public class VariantCatalogQueryUtilsTest {

    @ClassRule
    public static CatalogManagerExternalResource catalogManagerExternalResource = new CatalogManagerExternalResource();

    @Rule
    public ExpectedException thrown = ExpectedException.none();

    private static CatalogManager catalog;
    private static String sessionId;
    private static VariantCatalogQueryUtils queryUtils;
    private static List<Sample> samples = new ArrayList<>();
    private static File file1;
    private static File file2;
    private static File file3;
    private static File file4;
    private static File file5;

    @BeforeClass
    public static void setUp() throws Exception {
        catalog = catalogManagerExternalResource.getCatalogManager();

        User user = catalog.getUserManager().create("user", "user", "my@email.org", "1234", "ACME", 1000L, null, null, null).first();
        sessionId = catalog.getUserManager().login("user", "1234");
        catalog.getProjectManager().create("p1", "p1", "", null, "hsapiens", "Homo Sapiens", null, "GRCh38", null, sessionId);
        catalog.getStudyManager().create("p1", "s1", null, "s1", Study.Type.CONTROL_SET, null, null, null, null, null, null, null, null, null, null, sessionId);
        catalog.getStudyManager().create("p1", "s2", null, "s2", Study.Type.CONTROL_SET, null, null, null, null, null, null, null, null, null, null, sessionId);
        catalog.getStudyManager().create("p1", "s3", null, "s3", Study.Type.CONTROL_SET, null, null, null, null, null, null, null, null, null, null, sessionId);
        file1 = createFile("file1.vcf");
        file2 = createFile("file2.vcf");
        createSample("sample1");
        createSample("sample2");
        catalog.getCohortManager().create("s1", new Cohort().setId("c1").setSamples(Collections.emptyList()), null, sessionId);

        catalog.getProjectManager().incrementRelease("p1", sessionId);
        file3 = createFile("file3.vcf");
        file4 = createFile("file4.vcf");
        file5 = createFile("file5.vcf", false);
        createSample("sample3");
        createSample("sample4");
        catalog.getCohortManager().create("s1", new Cohort().setId("c2").setSamples(Collections.emptyList()), null, sessionId);
        catalog.getCohortManager().create("s1", new Cohort().setId(StudyEntry.DEFAULT_COHORT).setSamples(samples), null, sessionId);
        queryUtils = new VariantCatalogQueryUtils(catalog);
    }

    public static void createSample(String name) throws CatalogException {
        samples.add(catalog.getSampleManager().create("s1", name, null, null, null, false, null, null, null, null, sessionId).first());
    }

    public static File createFile(String path) throws CatalogException {
        return createFile(path, true);
    }

    public static File createFile(String path, boolean indexed) throws CatalogException {
        File file = catalog.getFileManager().create("s1", File.Type.FILE, File.Format.VCF, File.Bioformat.VARIANT, path, null, null, null, 10, -1, null, -1, null, null, false, "", null, sessionId).first();
        if (indexed) {
            int release = catalog.getProjectManager().get("p1", null, sessionId).first().getCurrentRelease();
            catalog.getFileManager().updateFileIndexStatus(file, Status.READY, "", release, sessionId);
        }
        return file;
    }

    @Test
    public void queriesWithRelease() throws Exception {
        System.out.println(queryUtils.parseQuery(new Query(STUDY.key(), "s1").append(VariantQueryParam.SAMPLE.key(), "sample2").append(VariantQueryParam.RELEASE.key(), 2), sessionId).toJson());
        System.out.println(queryUtils.parseQuery(new Query(STUDY.key(), "s1").append(VariantQueryParam.SAMPLE.key(), "sample2").append(VariantQueryParam.RELEASE.key(), 1), sessionId).toJson());
        System.out.println(queryUtils.parseQuery(new Query(STUDY.key(), "s1").append(VariantQueryParam.INCLUDE_SAMPLE.key(), "sample2").append(VariantQueryParam.RELEASE.key(), 1), sessionId).toJson());
        System.out.println(queryUtils.parseQuery(new Query(STUDY.key(), "s1").append(VariantQueryParam.FILE.key(), "file1.vcf").append(VariantQueryParam.RELEASE.key(), 1), sessionId).toJson());
        System.out.println(queryUtils.parseQuery(new Query(STUDY.key(), "s1").append(VariantQueryParam.STATS_MAF.key(), "c1>0.1").append(VariantQueryParam.RELEASE.key(), 1), sessionId).toJson());
        System.out.println(queryUtils.parseQuery(new Query(STUDY.key(), "s1").append(VariantQueryParam.GENOTYPE.key(), "sample1:HOM_ALT,sample2:HET_REF").append(VariantQueryParam.RELEASE.key(), 1), sessionId).toJson());
    }

    @Test
    public void sampleWrongRelease() throws Exception {
        VariantQueryException e = VariantCatalogQueryUtils.wrongReleaseException(VariantQueryParam.SAMPLE, "sample3", 1);
        thrown.expectMessage(e.getMessage());
        thrown.expect(e.getClass());
        queryUtils.parseQuery(new Query(VariantQueryParam.SAMPLE.key(), "sample3")
                .append(VariantQueryParam.STUDY.key(), "s1")
                .append(VariantQueryParam.RELEASE.key(), 1), sessionId).toJson();
    }

    @Test
    public void sampleNotFound() throws Exception {
        thrown.expectMessage("not found");
        thrown.expect(CatalogException.class);
        queryUtils.parseQuery(new Query(VariantQueryParam.SAMPLE.key(), "sample_not_exists")
                .append(VariantQueryParam.STUDY.key(), "s1")
                .append(VariantQueryParam.RELEASE.key(), 1), sessionId).toJson();
    }

    @Test
    public void includeSampleWrongRelease() throws Exception {
        VariantQueryException e = VariantCatalogQueryUtils.wrongReleaseException(VariantQueryParam.INCLUDE_SAMPLE, "sample3", 1);
        thrown.expectMessage(e.getMessage());
        thrown.expect(e.getClass());
        queryUtils.parseQuery(new Query(VariantQueryParam.INCLUDE_SAMPLE.key(), "sample3")
                .append(VariantQueryParam.STUDY.key(), "s1")
                .append(VariantQueryParam.RELEASE.key(), 1), sessionId).toJson();
    }

    @Test
    public void fileWrongRelease() throws Exception {
        VariantQueryException e = VariantCatalogQueryUtils.wrongReleaseException(VariantQueryParam.FILE, "file3.vcf", 1);
        thrown.expectMessage(e.getMessage());
        thrown.expect(e.getClass());
        queryUtils.parseQuery(new Query(VariantQueryParam.FILE.key(), "file3.vcf")
                .append(VariantQueryParam.STUDY.key(), "s1")
                .append(VariantQueryParam.RELEASE.key(), 1), sessionId).toJson();
    }

    @Test
    public void fileNotFound() throws Exception {
        thrown.expectMessage("not found");
        thrown.expect(CatalogException.class);
        queryUtils.parseQuery(new Query(VariantQueryParam.FILE.key(), "non_existing_file.vcf")
                .append(VariantQueryParam.STUDY.key(), "s1")
                .append(VariantQueryParam.RELEASE.key(), 1), sessionId).toJson();
    }

    @Test
    public void fileNotIndexed() throws Exception {
        thrown.expectMessage("not indexed");
        thrown.expect(VariantQueryException.class);
        queryUtils.parseQuery(new Query(VariantQueryParam.FILE.key(), "file5.vcf")
                .append(VariantQueryParam.STUDY.key(), "s1")
                .append(VariantQueryParam.RELEASE.key(), 1), sessionId).toJson();
    }

    @Test
    public void fileWrongNameWithRelease() throws Exception {
        thrown.expectMessage("not found");
        thrown.expect(CatalogException.class);
        queryUtils.parseQuery(new Query(VariantQueryParam.FILE.key(), "non_existing_file.vcf")
                .append(VariantQueryParam.STUDY.key(), "s1"), sessionId).toJson();
    }

    @Test
    public void includeFileWrongRelease() throws Exception {
        VariantQueryException e = VariantCatalogQueryUtils.wrongReleaseException(VariantQueryParam.INCLUDE_FILE, "file3.vcf", 1);
        thrown.expectMessage(e.getMessage());
        thrown.expect(e.getClass());
        queryUtils.parseQuery(new Query(VariantQueryParam.INCLUDE_FILE.key(), "file3.vcf")
                .append(VariantQueryParam.STUDY.key(), "s1")
                .append(VariantQueryParam.RELEASE.key(), 1), sessionId).toJson();
    }

    @Test
    public void cohortWrongRelease() throws Exception {
        VariantQueryException e = VariantCatalogQueryUtils.wrongReleaseException(VariantQueryParam.COHORT, "c2", 1);
        thrown.expectMessage(e.getMessage());
        thrown.expect(e.getClass());
        queryUtils.parseQuery(new Query(VariantQueryParam.COHORT.key(), "c2")
                .append(VariantQueryParam.STUDY.key(), "s1")
                .append(VariantQueryParam.RELEASE.key(), 1), sessionId).toJson();
    }

    @Test
    public void cohortStatsWrongRelease() throws Exception {
        VariantQueryException e = VariantCatalogQueryUtils.wrongReleaseException(VariantQueryParam.STATS_MAF, "c2", 1);
        thrown.expectMessage(e.getMessage());
        thrown.expect(e.getClass());
        queryUtils.parseQuery(new Query(VariantQueryParam.STATS_MAF.key(), "c2>0.2")
                .append(VariantQueryParam.STUDY.key(), "s1")
                .append(VariantQueryParam.RELEASE.key(), 1), sessionId).toJson();
    }

    @Test
    public void parseQuery() throws Exception {
        assertEquals("user@p1:s1", parseValue(STUDY, "s1"));
        assertEquals("user@p1:s1,user@p1:s2", parseValue(STUDY, "s1,s2"));
        assertEquals("!user@p1:s1,user@p1:s2", parseValue(STUDY, "!s1,s2"));
        assertEquals("user@p1:s2;!user@p1:s1;user@p1:s3", parseValue(STUDY, "user@p1:s2;!s1;p1:s3"));

        assertEquals(file1.getName(), parseValue("s1", FILE, file1.getName()));
        assertEquals(file1.getName(), parseValue("s1", FILE, file1.getId()));
        assertEquals(file1.getName(), parseValue("s1", FILE, file1.getPath()));
        assertEquals(file1.getName() + "," + file2.getName(), parseValue("s1", FILE, file1.getPath() + "," + file2.getPath()));
        assertEquals(file1.getName() + ";" + file2.getName(), parseValue("s1", FILE, file1.getPath() + ";" + file2.getPath()));
//        assertEquals("file1.vcf", parseValue("s1", FILE, String.valueOf(file1.getUid())));

        assertEquals("sample1:HOM_ALT;sample2:HET_REF", parseValue("s1", GENOTYPE, "sample1:HOM_ALT;sample2:HET_REF"));
        assertEquals("sample1:HOM_ALT,sample2:HET_REF", parseValue("s1", GENOTYPE, "sample1:HOM_ALT,sample2:HET_REF"));


    }

    protected String parseValue(VariantQueryParam param, String value) throws CatalogException {
        return queryUtils.parseQuery(new Query(param.key(), value), sessionId).getString(param.key());
    }

    protected String parseValue(String study, VariantQueryParam param, String value) throws CatalogException {
        return queryUtils.parseQuery(new Query(STUDY.key(), study).append(param.key(), value), sessionId).getString(param.key());
    }

}
