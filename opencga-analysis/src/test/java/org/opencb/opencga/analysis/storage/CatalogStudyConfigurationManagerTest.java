package org.opencb.opencga.analysis.storage;

import org.apache.log4j.ConsoleAppender;
import org.apache.log4j.Level;
import org.apache.log4j.LogManager;
import org.junit.Before;
import org.junit.Test;
import org.opencb.datastore.core.ObjectMap;
import org.opencb.datastore.core.QueryOptions;
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
public class CatalogStudyConfigurationManagerTest {


    private CatalogManager catalogManager;
    private String sessionId;
    private int projectId;
    private int studyId;
    private FileMetadataReader fileMetadataReader;
    private CatalogFileUtils catalogFileUtils;
    private int outputId;
    Logger logger = LoggerFactory.getLogger(AnalysisFileIndexerTest.class);
    private String catalogPropertiesFile;
    private final String userId = "user";
    private List<File> files = new ArrayList<>();

    @Before
    public void before() throws Exception {
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

    public File create(String resourceName) throws IOException, CatalogException {
        return create(resourceName, false);
    }

    public File create(String resourceName, boolean indexed) throws IOException, CatalogException {
        File file;
        URI uri = getResourceUri(resourceName);
        file = fileMetadataReader.create(studyId, uri, "data/vcfs/", "", true, null, sessionId).first();
        catalogFileUtils.upload(uri, file, null, sessionId, false, false, true, false, Long.MAX_VALUE);
        if (indexed) {
            catalogManager.modifyFile(file.getId(), new ObjectMap("index", new Index("user", "today", Index.Status.READY, 1234, Collections.emptyMap())), sessionId);
        }
        return catalogManager.getFile(file.getId(), sessionId).first();
    }

    @Test
    public void getStudyConfiguration() throws Exception {
        StudyConfigurationManager studyConfigurationManager = new CatalogStudyConfigurationManager(catalogManager, sessionId);

        StudyConfiguration studyConfiguration = studyConfigurationManager.getStudyConfiguration(studyId, new QueryOptions("sessionId", sessionId)).first();
        Study study = catalogManager.getStudy(studyId, sessionId).first();
        assertEquals(study.getAlias(), studyConfiguration.getStudyName());
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