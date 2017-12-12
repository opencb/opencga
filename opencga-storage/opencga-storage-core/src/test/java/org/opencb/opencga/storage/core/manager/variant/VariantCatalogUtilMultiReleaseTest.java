package org.opencb.opencga.storage.core.manager.variant;

import org.junit.*;
import org.junit.rules.ExpectedException;
import org.opencb.commons.datastore.core.Query;
import org.opencb.opencga.catalog.exceptions.CatalogException;
import org.opencb.opencga.catalog.managers.CatalogManager;
import org.opencb.opencga.catalog.managers.CatalogManagerExternalResource;
import org.opencb.opencga.core.models.Cohort;
import org.opencb.opencga.core.models.File;
import org.opencb.opencga.core.models.Study;
import org.opencb.opencga.core.models.User;
import org.opencb.opencga.storage.core.variant.adaptors.VariantQueryException;
import org.opencb.opencga.storage.core.variant.adaptors.VariantQueryParam;

import java.util.Collections;

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

    @BeforeClass
    public static void setUp() throws Exception {
        catalog = catalogManagerExternalResource.getCatalogManager();

        User user = catalog.getUserManager().create("user", "user", "my@email.org", "1234", "ACME", 1000L, null, null).first();
        sessionId = catalog.getUserManager().login("user", "1234");
        catalog.getProjectManager().create("p1", "p1", "", null, "hsapiens", "Homo Sapiens", null, "GRCh38", null, sessionId);
        catalog.getStudyManager().create("p1", "s1", "s1", Study.Type.CONTROL_SET, null, null, null, null, null, null, null, null, null, null, sessionId);
        createFile("file1.vcf");
        createFile("file2.vcf");
        catalog.getSampleManager().create("s1", "sample1", null, null, null, false, null, null, null, null, sessionId);
        catalog.getSampleManager().create("s1", "sample2", null, null, null, false, null, null, null, null, sessionId);
        catalog.getCohortManager().create("s1", new Cohort().setName("c1").setSamples(Collections.emptyList()), null, sessionId);

        catalog.getProjectManager().incrementRelease("p1", sessionId);
        createFile("file3.vcf");
        createFile("file4.vcf");
        catalog.getSampleManager().create("s1", "sample3", null, null, null, false, null, null, null, null, sessionId);
        catalog.getSampleManager().create("s1", "sample4", null, null, null, false, null, null, null, null, sessionId);
        catalog.getCohortManager().create("s1", new Cohort().setName("c2").setSamples(Collections.emptyList()), null, sessionId);
        queryUtils = new VariantCatalogQueryUtils(catalog);
    }

    public static void createFile(String path) throws CatalogException {
        catalog.getFileManager().create("s1", File.Type.FILE, File.Format.VCF, File.Bioformat.VARIANT, path, null, null, null, 10, -1, null, -1, null, null, false, "", null, sessionId);
    }

    @Test
    public void queriesWithRelease() throws Exception {
        queryUtils.parseQuery(new Query(VariantQueryParam.SAMPLES.key(), "sample2").append(VariantQueryParam.RELEASE.key(), 2), sessionId).toJson();
        queryUtils.parseQuery(new Query(VariantQueryParam.SAMPLES.key(), "sample2").append(VariantQueryParam.RELEASE.key(), 1), sessionId).toJson();
        queryUtils.parseQuery(new Query(VariantQueryParam.RETURNED_SAMPLES.key(), "sample2").append(VariantQueryParam.RELEASE.key(), 1), sessionId).toJson();
        queryUtils.parseQuery(new Query(VariantQueryParam.FILES.key(), "file1.vcf").append(VariantQueryParam.RELEASE.key(), 1), sessionId).toJson();
        queryUtils.parseQuery(new Query(VariantQueryParam.STATS_MAF.key(), "c1>0.1").append(VariantQueryParam.RELEASE.key(), 1), sessionId).toJson();
    }

    @Test
    public void sampleWrongRelease() throws Exception {
        VariantQueryException e = VariantCatalogQueryUtils.wrongReleaseException(VariantQueryParam.SAMPLES, "sample3", 1);
        thrown.expectMessage(e.getMessage());
        thrown.expect(e.getClass());
        queryUtils.parseQuery(new Query(VariantQueryParam.SAMPLES.key(), "sample3")
                .append(VariantQueryParam.RELEASE.key(), 1), sessionId).toJson();
    }

    @Test
    public void includeSampleWrongRelease() throws Exception {
        VariantQueryException e = VariantCatalogQueryUtils.wrongReleaseException(VariantQueryParam.RETURNED_SAMPLES, "sample3", 1);
        thrown.expectMessage(e.getMessage());
        thrown.expect(e.getClass());
        queryUtils.parseQuery(new Query(VariantQueryParam.RETURNED_SAMPLES.key(), "sample3")
                .append(VariantQueryParam.RELEASE.key(), 1), sessionId).toJson();
    }

    @Test
    public void fileWrongRelease() throws Exception {
        VariantQueryException e = VariantCatalogQueryUtils.wrongReleaseException(VariantQueryParam.FILES, "file3.vcf", 1);
        thrown.expectMessage(e.getMessage());
        thrown.expect(e.getClass());
        queryUtils.parseQuery(new Query(VariantQueryParam.FILES.key(), "file3.vcf")
                .append(VariantQueryParam.RELEASE.key(), 1), sessionId).toJson();
    }

    @Test
    public void includeFileWrongRelease() throws Exception {
        VariantQueryException e = VariantCatalogQueryUtils.wrongReleaseException(VariantQueryParam.RETURNED_FILES, "file3.vcf", 1);
        thrown.expectMessage(e.getMessage());
        thrown.expect(e.getClass());
        queryUtils.parseQuery(new Query(VariantQueryParam.RETURNED_FILES.key(), "file3.vcf")
                .append(VariantQueryParam.RELEASE.key(), 1), sessionId).toJson();
    }

    @Test
    public void cohortWrongRelease() throws Exception {
        VariantQueryException e = VariantCatalogQueryUtils.wrongReleaseException(VariantQueryParam.COHORTS, "c2", 1);
        thrown.expectMessage(e.getMessage());
        thrown.expect(e.getClass());
        queryUtils.parseQuery(new Query(VariantQueryParam.COHORTS.key(), "c2")
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
