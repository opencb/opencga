package org.opencb.opencga.analysis.storage;

import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.opencb.commons.datastore.core.ObjectMap;
import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.commons.datastore.core.QueryResult;
import org.opencb.opencga.analysis.variant.CatalogStudyConfigurationFactory;
import org.opencb.opencga.catalog.utils.FileMetadataReader;
import org.opencb.opencga.catalog.managers.CatalogManager;
import org.opencb.opencga.catalog.CatalogManagerExternalResource;
import org.opencb.opencga.catalog.exceptions.CatalogException;
import org.opencb.opencga.catalog.models.*;
import org.opencb.opencga.catalog.managers.CatalogFileUtils;
import org.opencb.opencga.storage.core.metadata.StudyConfiguration;
import org.opencb.opencga.storage.core.variant.StudyConfigurationManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.URI;
import java.nio.file.Paths;
import java.util.*;
import java.util.stream.Collectors;

import static org.junit.Assert.*;
import static org.opencb.opencga.storage.core.variant.VariantStorageManagerTestUtils.DB_NAME;
import static org.opencb.opencga.storage.core.variant.VariantStorageManagerTestUtils.getResourceUri;

/**
 * Created by hpccoll1 on 16/07/15.
 */
public class CatalogStudyConfigurationFactoryTest {
    @ClassRule
    public static CatalogManagerExternalResource catalogManagerExternalResource = new CatalogManagerExternalResource();

    static private CatalogManager catalogManager;
    static private String sessionId;
    static private long projectId;
    static private long studyId;
    static private FileMetadataReader fileMetadataReader;
    static private CatalogFileUtils catalogFileUtils;
    static private long outputId;
    static Logger logger = LoggerFactory.getLogger(CatalogStudyConfigurationFactoryTest.class);
    static private String catalogPropertiesFile;
    static private final String userId = "user";
    static private List<File> files = new ArrayList<>();
    static private LinkedHashSet<Integer> indexedFiles = new LinkedHashSet<>();

    @BeforeClass
    public static void beforeClass() throws Exception {
//        ConsoleAppender stderr = (ConsoleAppender) LogManager.getRootLogger().getAppender("stderr");
//        stderr.setThreshold(Level.toLevel("debug"));

        catalogManager = catalogManagerExternalResource.getCatalogManager();
        fileMetadataReader = FileMetadataReader.get(catalogManager);
        catalogFileUtils = new CatalogFileUtils(catalogManager);

        User user = catalogManager.createUser(userId, "User", "user@email.org", "user", "ACME", null, null).first();
        sessionId = catalogManager.login(userId, "user", "localhost").first().getString("sessionId");
        projectId = catalogManager.createProject("p1", "p1", "Project 1", "ACME", null, sessionId).first().getId();
        studyId = catalogManager.createStudy(projectId, "s1", "s1", Study.Type.CASE_CONTROL, null, "Study 1", null, null, null, null, Collections.singletonMap(File.Bioformat.VARIANT, new DataStore("mongodb", DB_NAME)), null, null, null, sessionId).first().getId();
        outputId = catalogManager.createFolder(studyId, Paths.get("data", "index"), false, null, sessionId).first().getId();
        files.add(create("1000g_batches/1-500.filtered.10k.chr22.phase3_shapeit2_mvncall_integrated_v5.20130502.genotypes.vcf.gz"));
        files.add(create("1000g_batches/501-1000.filtered.10k.chr22.phase3_shapeit2_mvncall_integrated_v5.20130502.genotypes.vcf.gz", true));
        files.add(create("1000g_batches/1001-1500.filtered.10k.chr22.phase3_shapeit2_mvncall_integrated_v5.20130502.genotypes.vcf.gz"));
        files.add(create("1000g_batches/1501-2000.filtered.10k.chr22.phase3_shapeit2_mvncall_integrated_v5.20130502.genotypes.vcf.gz", true));
        files.add(create("1000g_batches/2001-2504.filtered.10k.chr22.phase3_shapeit2_mvncall_integrated_v5.20130502.genotypes.vcf.gz"));

    }

    public static File create(String resourceName) throws IOException, CatalogException {
        return create(resourceName, false);
    }

    public static File create(String resourceName, boolean indexed) throws IOException, CatalogException {
        File file;
        URI uri = getResourceUri(resourceName);
        file = fileMetadataReader.create(studyId, uri, "data/vcfs/", "", true, null, sessionId).first();
        catalogFileUtils.upload(uri, file, null, sessionId, false, false, true, false, Long.MAX_VALUE);
        if (indexed) {
            catalogManager.modifyFile(file.getId(), new ObjectMap("index", new FileIndex("user", "today",
                    new FileIndex.IndexStatus(FileIndex.IndexStatus.READY), 1234, Collections.emptyMap())), sessionId);
            indexedFiles.add((int) file.getId());
        }
        return catalogManager.getFile(file.getId(), sessionId).first();
    }

    @Test
    public void getNewStudyConfiguration() throws Exception {
        CatalogStudyConfigurationFactory studyConfigurationManager = new CatalogStudyConfigurationFactory(catalogManager);

        Study study = catalogManager.getStudy(studyId, sessionId).first();
        StudyConfiguration studyConfiguration = studyConfigurationManager.getStudyConfiguration(studyId, new StudyConfigurationManager(new org.opencb.commons.datastore.core.ObjectMap()) {
            protected QueryResult<StudyConfiguration> internalGetStudyConfiguration(String studyName, Long timeStamp, org.opencb.commons.datastore.core.QueryOptions options) {return null;}
            protected QueryResult internalUpdateStudyConfiguration(StudyConfiguration studyConfiguration, org.opencb.commons.datastore.core.QueryOptions options) {return null;}
            protected QueryResult<StudyConfiguration> internalGetStudyConfiguration(int studyId, Long timeStamp, org.opencb.commons.datastore.core.QueryOptions options) {
                StudyConfiguration studyConfiguration = new StudyConfiguration((int) study.getId(), "user@p1:s1");
                studyConfiguration.setIndexedFiles(indexedFiles);
                return new QueryResult<StudyConfiguration>("", 0, 0, 0, "", "", Collections.emptyList());
            }

        }, new QueryOptions(), sessionId);

        checkStudyConfiguration(study, studyConfiguration);
    }

    @Test
    public void getNewStudyConfigurationNullManager() throws Exception {
        CatalogStudyConfigurationFactory studyConfigurationManager = new CatalogStudyConfigurationFactory(catalogManager);

        Study study = catalogManager.getStudy(studyId, sessionId).first();
        StudyConfiguration studyConfiguration = studyConfigurationManager.getStudyConfiguration(studyId, null, new QueryOptions(), sessionId);

        checkStudyConfiguration(study, studyConfiguration);
    }

    @Test
    public void getStudyConfiguration() throws Exception {
        CatalogStudyConfigurationFactory studyConfigurationManager = new CatalogStudyConfigurationFactory(catalogManager);

        Study study = catalogManager.getStudy(studyId, sessionId).first();
        StudyConfiguration studyConfiguration = studyConfigurationManager.getStudyConfiguration(studyId, new StudyConfigurationManager(new org.opencb.commons.datastore.core.ObjectMap()) {
            protected QueryResult<StudyConfiguration> internalGetStudyConfiguration(String studyName, Long timeStamp, org.opencb.commons.datastore.core.QueryOptions options) {return null;}
            protected QueryResult internalUpdateStudyConfiguration(StudyConfiguration studyConfiguration, org.opencb.commons.datastore.core.QueryOptions options) {return null;}
            protected QueryResult<StudyConfiguration> internalGetStudyConfiguration(int studyId, Long timeStamp, org.opencb.commons.datastore.core.QueryOptions options) {
                StudyConfiguration studyConfiguration = new StudyConfiguration((int) study.getId(), "user@p1:s1");
                studyConfiguration.setIndexedFiles(indexedFiles);
                return new QueryResult<StudyConfiguration>("", 0, 1, 1, "", "", Collections.singletonList(studyConfiguration));
            }

        }, new QueryOptions(), sessionId);

        checkStudyConfiguration(study, studyConfiguration);
    }

    private void checkStudyConfiguration(Study study, StudyConfiguration studyConfiguration) throws CatalogException {
        assertEquals("user@p1:s1", studyConfiguration.getStudyName());
        assertEquals(study.getId(), studyConfiguration.getStudyId());

        assertTrue(studyConfiguration.getInvalidStats().isEmpty());

        for (Map.Entry<String, Integer> entry : studyConfiguration.getFileIds().entrySet()) {
            File file = catalogManager.getFile(entry.getValue(), sessionId).first();

            assertEquals(file.getName(), entry.getKey());
            int id = (int) file.getId();
            assertEquals(file.getSampleIds().stream().map(Long::intValue).collect(Collectors.toSet()), studyConfiguration.getSamplesInFiles().get((id)));
            if (file.getIndex() != null && file.getIndex().getStatus().getName().equals(FileIndex.IndexStatus.READY)) {
                assertTrue(studyConfiguration.getIndexedFiles().contains(id));
                assertTrue("Missing header for file " + file.getId(), studyConfiguration.getHeaders().containsKey(id));
                assertTrue("Missing header for file " + file.getId(), !studyConfiguration.getHeaders().get(id).isEmpty());
            } else {
                assertFalse(studyConfiguration.getIndexedFiles().contains(id));
                assertFalse("Should not contain header for file " + file.getId(), studyConfiguration.getHeaders().containsKey(id));
            }
        }
    }

}