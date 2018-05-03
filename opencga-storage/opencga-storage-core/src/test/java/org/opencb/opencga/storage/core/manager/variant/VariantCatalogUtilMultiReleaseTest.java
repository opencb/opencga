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

/**
 * Created on 12/12/17.
 *
 * @author Jacobo Coll &lt;jacobo167@gmail.com&gt;
 */
public class VariantCatalogUtilMultiReleaseTest {

    @ClassRule
    public static CatalogManagerExternalResource catalogManagerExternalResource = new CatalogManagerExternalResource();

    @Rule
    public ExpectedException thrown = ExpectedException.none();

    private static CatalogManager catalog;
    private static String sessionId;
    private static VariantCatalogQueryUtils queryUtils;
    private static List<Sample> samples = new ArrayList<>();

    @BeforeClass
    public static void setUp() throws Exception {
        catalog = catalogManagerExternalResource.getCatalogManager();

        User user = catalog.getUserManager().create("user", "user", "my@email.org", "1234", "ACME", 1000L, null, null, null).first();
        sessionId = catalog.getUserManager().login("user", "1234");
        catalog.getProjectManager().create("p1", "p1", "", null, "hsapiens", "Homo Sapiens", null, "GRCh38", null, sessionId);
        catalog.getStudyManager().create("p1", "s1", "s1", Study.Type.CONTROL_SET, null, null, null, null, null, null, null, null, null, null, sessionId);
        createFile("file1.vcf");
        createFile("file2.vcf");
        createSample("sample1");
        createSample("sample2");
        catalog.getCohortManager().create("s1", new Cohort().setName("c1").setSamples(Collections.emptyList()), null, sessionId);

        catalog.getProjectManager().incrementRelease("p1", sessionId);
        createFile("file3.vcf");
        createFile("file4.vcf");
        createSample("sample3");
        createSample("sample4");
        catalog.getCohortManager().create("s1", new Cohort().setName("c2").setSamples(Collections.emptyList()), null, sessionId);
        catalog.getCohortManager().create("s1", new Cohort().setName(StudyEntry.DEFAULT_COHORT).setSamples(samples), null, sessionId);
        queryUtils = new VariantCatalogQueryUtils(catalog);
    }

    public static void createSample(String name) throws CatalogException {
        samples.add(catalog.getSampleManager().create("s1", name, null, null, null, false, null, null, null, null, sessionId).first());
    }

    public static void createFile(String path) throws CatalogException {
        File file = catalog.getFileManager().create("s1", File.Type.FILE, File.Format.VCF, File.Bioformat.VARIANT, path, null, null, null, 10, -1, null, -1, null, null, false, "", null, sessionId).first();
        catalog.getFileManager().updateFileIndexStatus(file, Status.READY, "", sessionId);
    }

    @Test
    public void queriesWithRelease() throws Exception {
        System.out.println(queryUtils.parseQuery(new Query(VariantQueryParam.SAMPLE.key(), "sample2").append(VariantQueryParam.RELEASE.key(), 2), sessionId).toJson());
        System.out.println(queryUtils.parseQuery(new Query(VariantQueryParam.SAMPLE.key(), "sample2").append(VariantQueryParam.RELEASE.key(), 1), sessionId).toJson());
        System.out.println(queryUtils.parseQuery(new Query(VariantQueryParam.INCLUDE_SAMPLE.key(), "sample2").append(VariantQueryParam.RELEASE.key(), 1), sessionId).toJson());
        System.out.println(queryUtils.parseQuery(new Query(VariantQueryParam.FILE.key(), "file1.vcf").append(VariantQueryParam.RELEASE.key(), 1), sessionId).toJson());
        System.out.println(queryUtils.parseQuery(new Query(VariantQueryParam.STATS_MAF.key(), "c1>0.1").append(VariantQueryParam.RELEASE.key(), 1), sessionId).toJson());
    }

    @Test
    public void sampleWrongRelease() throws Exception {
        VariantQueryException e = VariantCatalogQueryUtils.wrongReleaseException(VariantQueryParam.SAMPLE, "sample3", 1);
        thrown.expectMessage(e.getMessage());
        thrown.expect(e.getClass());
        queryUtils.parseQuery(new Query(VariantQueryParam.SAMPLE.key(), "sample3")
                .append(VariantQueryParam.RELEASE.key(), 1), sessionId).toJson();
    }

    @Test
    public void includeSampleWrongRelease() throws Exception {
        VariantQueryException e = VariantCatalogQueryUtils.wrongReleaseException(VariantQueryParam.INCLUDE_SAMPLE, "sample3", 1);
        thrown.expectMessage(e.getMessage());
        thrown.expect(e.getClass());
        queryUtils.parseQuery(new Query(VariantQueryParam.INCLUDE_SAMPLE.key(), "sample3")
                .append(VariantQueryParam.RELEASE.key(), 1), sessionId).toJson();
    }

    @Test
    public void fileWrongRelease() throws Exception {
        VariantQueryException e = VariantCatalogQueryUtils.wrongReleaseException(VariantQueryParam.FILE, "file3.vcf", 1);
        thrown.expectMessage(e.getMessage());
        thrown.expect(e.getClass());
        queryUtils.parseQuery(new Query(VariantQueryParam.FILE.key(), "file3.vcf")
                .append(VariantQueryParam.RELEASE.key(), 1), sessionId).toJson();
    }

    @Test
    public void includeFileWrongRelease() throws Exception {
        VariantQueryException e = VariantCatalogQueryUtils.wrongReleaseException(VariantQueryParam.INCLUDE_FILE, "file3.vcf", 1);
        thrown.expectMessage(e.getMessage());
        thrown.expect(e.getClass());
        queryUtils.parseQuery(new Query(VariantQueryParam.INCLUDE_FILE.key(), "file3.vcf")
                .append(VariantQueryParam.RELEASE.key(), 1), sessionId).toJson();
    }

    @Test
    public void cohortWrongRelease() throws Exception {
        VariantQueryException e = VariantCatalogQueryUtils.wrongReleaseException(VariantQueryParam.COHORT, "c2", 1);
        thrown.expectMessage(e.getMessage());
        thrown.expect(e.getClass());
        queryUtils.parseQuery(new Query(VariantQueryParam.COHORT.key(), "c2")
                .append(VariantQueryParam.RELEASE.key(), 1), sessionId).toJson();
    }

    @Test
    public void cohortStatsWrongRelease() throws Exception {
        VariantQueryException e = VariantCatalogQueryUtils.wrongReleaseException(VariantQueryParam.STATS_MAF, "c2", 1);
        thrown.expectMessage(e.getMessage());
        thrown.expect(e.getClass());
        queryUtils.parseQuery(new Query(VariantQueryParam.STATS_MAF.key(), "c2>0.2")
                .append(VariantQueryParam.RELEASE.key(), 1), sessionId).toJson();
    }

}
