package org.opencb.opencga.analysis.storage;

import org.apache.log4j.ConsoleAppender;
import org.apache.log4j.Level;
import org.apache.log4j.LogManager;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.opencb.datastore.core.ObjectMap;
import org.opencb.datastore.core.QueryOptions;
import org.opencb.datastore.core.QueryResult;
import org.opencb.opencga.analysis.files.FileMetadataReader;
import org.opencb.opencga.catalog.CatalogManager;
import org.opencb.opencga.catalog.CatalogManagerTest;
import org.opencb.opencga.catalog.exceptions.CatalogException;
import org.opencb.opencga.catalog.models.*;
import org.opencb.opencga.catalog.utils.CatalogFileUtils;
import org.opencb.opencga.storage.core.StudyConfiguration;
import org.opencb.opencga.storage.core.variant.StudyConfigurationManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.URI;
import java.nio.file.Paths;
import java.util.*;

import static org.junit.Assert.*;
import static org.opencb.opencga.storage.core.variant.VariantStorageManagerTestUtils.DB_NAME;
import static org.opencb.opencga.storage.core.variant.VariantStorageManagerTestUtils.getResourceUri;

/**
 * Created by hpccoll1 on 16/07/15.
 */
public class CatalogStudyConfigurationFactoryTest {


    static private CatalogManager catalogManager;
    static private String sessionId;
    static private int projectId;
    static private int studyId;
    static private FileMetadataReader fileMetadataReader;
    static private CatalogFileUtils catalogFileUtils;
    static private int outputId;
    static Logger logger = LoggerFactory.getLogger(AnalysisFileIndexerTest.class);
    static private String catalogPropertiesFile;
    static private final String userId = "user";
    static private List<File> files = new ArrayList<>();
    static private LinkedHashSet<Integer> indexedFiles = new LinkedHashSet<>();

    @BeforeClass
    public static void beforeClass() throws Exception {
        ConsoleAppender stderr = (ConsoleAppender) LogManager.getRootLogger().getAppender("stderr");
        stderr.setThreshold(Level.toLevel("debug"));

        catalogPropertiesFile = getResourceUri("catalog.properties").getPath();
        Properties properties = new Properties();
        properties.load(CatalogManagerTest.class.getClassLoader().getResourceAsStream("catalog.properties"));

        CatalogManagerTest.clearCatalog(properties);

        catalogManager = new CatalogManager(properties);
        fileMetadataReader = FileMetadataReader.get(catalogManager);
        catalogFileUtils = new CatalogFileUtils(catalogManager);

        User user = catalogManager.createUser(userId, "User", "user@email.org", "user", "ACME", null).first();
        sessionId = catalogManager.login(userId, "user", "localhost").first().getString("sessionId");
        projectId = catalogManager.createProject(userId, "p1", "p1", "Project 1", "ACME", null, sessionId).first().getId();
        studyId = catalogManager.createStudy(projectId, "s1", "s1", Study.Type.CASE_CONTROL, null, null, "Study 1", null, null, null, null, Collections.singletonMap(File.Bioformat.VARIANT, new DataStore("mongodb", DB_NAME)), null, null, null, sessionId).first().getId();
        outputId = catalogManager.createFolder(studyId, Paths.get("data", "index"), false, null, sessionId).first().getId();
        files.add(create("1-500.filtered.10k.chr22.phase3_shapeit2_mvncall_integrated_v5.20130502.genotypes.vcf.gz"));
        files.add(create("501-1000.filtered.10k.chr22.phase3_shapeit2_mvncall_integrated_v5.20130502.genotypes.vcf.gz", true));
        files.add(create("1001-1500.filtered.10k.chr22.phase3_shapeit2_mvncall_integrated_v5.20130502.genotypes.vcf.gz"));
        files.add(create("1501-2000.filtered.10k.chr22.phase3_shapeit2_mvncall_integrated_v5.20130502.genotypes.vcf.gz", true));
        files.add(create("2001-2504.filtered.10k.chr22.phase3_shapeit2_mvncall_integrated_v5.20130502.genotypes.vcf.gz"));

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
            catalogManager.modifyFile(file.getId(), new ObjectMap("index", new Index("user", "today", Index.Status.READY, 1234, Collections.emptyMap())), sessionId);
            indexedFiles.add(file.getId());
        }
        return catalogManager.getFile(file.getId(), sessionId).first();
    }

    @Test
    public void getNewStudyConfiguration() throws Exception {
        CatalogStudyConfigurationFactory studyConfigurationManager = new CatalogStudyConfigurationFactory(catalogManager);

        Study study = catalogManager.getStudy(studyId, sessionId).first();
        StudyConfiguration studyConfiguration = studyConfigurationManager.getStudyConfiguration(studyId, new StudyConfigurationManager(new ObjectMap()) {
            protected QueryResult<StudyConfiguration> internalGetStudyConfiguration(String studyName, Long timeStamp, QueryOptions options) {return null;}
            protected QueryResult internalUpdateStudyConfiguration(StudyConfiguration studyConfiguration, QueryOptions options) {return null;}
            protected QueryResult<StudyConfiguration> internalGetStudyConfiguration(int studyId, Long timeStamp, QueryOptions options) {
                StudyConfiguration studyConfiguration = new StudyConfiguration(study.getId(), "user@p1:s1");
                studyConfiguration.setIndexedFiles(indexedFiles);
                return new QueryResult<>("", 0, 0, 0, "", "", Collections.emptyList());
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
        StudyConfiguration studyConfiguration = studyConfigurationManager.getStudyConfiguration(studyId, new StudyConfigurationManager(new ObjectMap()) {
            protected QueryResult<StudyConfiguration> internalGetStudyConfiguration(String studyName, Long timeStamp, QueryOptions options) {return null;}
            protected QueryResult internalUpdateStudyConfiguration(StudyConfiguration studyConfiguration, QueryOptions options) {return null;}
            protected QueryResult<StudyConfiguration> internalGetStudyConfiguration(int studyId, Long timeStamp, QueryOptions options) {
                StudyConfiguration studyConfiguration = new StudyConfiguration(study.getId(), "user@p1:s1");
                studyConfiguration.setIndexedFiles(indexedFiles);
                return new QueryResult<>("", 0, 1, 1, "", "", Collections.singletonList(studyConfiguration));
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
            assertEquals(new HashSet<>(file.getSampleIds()), studyConfiguration.getSamplesInFiles().get(file.getId()));
            if (file.getIndex() != null && file.getIndex().getStatus().equals(Index.Status.READY)) {
                assertTrue(studyConfiguration.getIndexedFiles().contains(file.getId()));
                assertTrue(studyConfiguration.getHeaders().containsKey(file.getId()));
                assertTrue(!studyConfiguration.getHeaders().get(file.getId()).isEmpty());
            } else {
                assertFalse(studyConfiguration.getIndexedFiles().contains(file.getId()));
                assertFalse(studyConfiguration.getHeaders().containsKey(file.getId()));
            }
        }
    }

}