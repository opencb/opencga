package org.opencb.opencga.catalog.managers;

import org.apache.commons.lang3.StringUtils;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.opencb.opencga.catalog.exceptions.ResourceException;
import org.opencb.opencga.catalog.utils.ResourceManager;
import org.opencb.opencga.core.common.GitRepositoryState;
import org.opencb.opencga.core.common.JacksonUtils;
import org.opencb.opencga.core.common.TimeUtils;
import org.opencb.opencga.core.models.resource.AnalysisResource;
import org.opencb.opencga.core.models.resource.AnalysisResourceList;
import org.opencb.opencga.core.models.resource.ResourceMetadata;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;
import java.util.stream.Collectors;

import static org.opencb.opencga.catalog.utils.ResourceManager.ANALYSIS_DIRNAME;
import static org.opencb.opencga.catalog.utils.ResourceManager.RESOURCES_DIRNAME;

public class ResourceManagerTest extends AbstractManagerTest {

    private Path openCgaHome;
    private Path scratchDir;
    private Path analysisPath;
    private Path analysisResourcePath;
    private ResourceMetadata resourceMetadata;
    private ResourceManager resourceManager;

    private String version = "3.3.0-SNAPSHOT";


    @Before
    public void setUp() throws Exception {
        super.setUp();

        openCgaHome = catalogManagerResource.getOpencgaHome();
        scratchDir = createDir("scratchdir");

        analysisPath = openCgaHome.resolve(ANALYSIS_DIRNAME);
        analysisResourcePath = analysisPath.resolve(RESOURCES_DIRNAME);

        resourceManager = new ResourceManager(openCgaHome);

//        InputStream stream = this.getClass().getClassLoader().getResourceAsStream("resources.json");
//        resourceMetadata = JacksonUtils.getDefaultObjectMapper().readerFor(ResourceMetadata.class).readValue(stream);
    }

    @Test(expected = ResourceException.class)
    public void testFetchRelatednessResource() throws ResourceException {
        String analysisId = "qc"; //""relatedness";
        String resourceName = "relatedness_thresholds.tsv"; //""variants.prune.in";

        Assert.assertFalse(Files.exists(analysisResourcePath.resolve(analysisId).resolve(resourceName)));

        File file = resourceManager.getResourceFile(analysisId, resourceName, version);
    }

    @Test
    public void testFetchAllResources() throws IOException, ResourceException {
        System.out.println("analysisResourcePath = " + analysisResourcePath.toAbsolutePath());
        Path outDir = createDir("jobdir").resolve(RESOURCES_DIRNAME);
        Files.createDirectories(outDir);
        System.out.println("outDir = " + outDir.toAbsolutePath());
        resourceManager.fetchAllResources(version, outDir, catalogManagerResource.getCatalogManager(), catalogManagerResource.getAdminToken());
        resourceMetadata = JacksonUtils.getDefaultObjectMapper().readerFor(ResourceMetadata.class).readValue(openCgaHome.resolve(ANALYSIS_DIRNAME).resolve(RESOURCES_DIRNAME).resolve(resourceManager.getResourceMetaFilename(version)).toFile());

        for (AnalysisResourceList list : resourceMetadata.getAnalysisResourceLists()) {
            for (AnalysisResource resource : list.getResources()) {
                Assert.assertTrue(Files.exists(analysisResourcePath.resolve(resource.getLocalRelativePath())));
            }
        }
    }

    @Test(expected = ResourceException.class)
    public void testFetchAllResourcesNoAdmin() throws ResourceException {
        resourceManager.fetchAllResources(version, null, catalogManagerResource.getCatalogManager(), normalToken1);
    }

    @Test
    public void testFetchResourcesForAGivenAnalysis() throws IOException, ResourceException {
        System.out.println("analysisResourcePath = " + analysisResourcePath.toAbsolutePath());
        Path outDir = createDir("jobdir").resolve(RESOURCES_DIRNAME);
        Files.createDirectories(outDir);
        System.out.println("outDir = " + outDir.toAbsolutePath());
        resourceManager.fetchAllResources(version, outDir, catalogManagerResource.getCatalogManager(), catalogManagerResource.getAdminToken());
        resourceMetadata = JacksonUtils.getDefaultObjectMapper().readerFor(ResourceMetadata.class).readValue(openCgaHome.resolve(ANALYSIS_DIRNAME).resolve(RESOURCES_DIRNAME).resolve(resourceManager.getResourceMetaFilename(version)).toFile());

        String analysisId = "qc";
        AnalysisResourceList analysisResourceList = null;
        for (AnalysisResourceList list : resourceMetadata.getAnalysisResourceLists()) {
            if (analysisId.equals(list.getAnalysisId())) {
                analysisResourceList = list;
                break;
            }
        }

        List<File> resourceFiles = resourceManager.getResourceFiles(analysisId, version);
        Assert.assertEquals(analysisResourceList.getResources().size(), resourceFiles.size());
        for (File resourceFile : resourceFiles) {
            Assert.assertTrue(Files.exists(resourceFile.toPath()));
            Assert.assertTrue(analysisResourceList.getResources().stream().map(r -> Paths.get(r.getLocalRelativePath()).getFileName().toString()).collect(Collectors.toList()).contains(resourceFile.getName()));
        }
    }

    @Test
    public void testFetchAGivenResource() throws IOException, ResourceException {
        System.out.println("analysisResourcePath = " + analysisResourcePath.toAbsolutePath());
        Path outDir = createDir("jobdir").resolve(RESOURCES_DIRNAME);
        Files.createDirectories(outDir);
        System.out.println("outDir = " + outDir.toAbsolutePath());
        resourceManager.fetchAllResources(version, outDir, catalogManagerResource.getCatalogManager(), catalogManagerResource.getAdminToken());
        resourceMetadata = JacksonUtils.getDefaultObjectMapper().readerFor(ResourceMetadata.class).readValue(openCgaHome.resolve(ANALYSIS_DIRNAME).resolve(RESOURCES_DIRNAME).resolve(resourceManager.getResourceMetaFilename(version)).toFile());

        String analysisId = "liftover";
        String resourceName = "chain.frq";
        AnalysisResourceList analysisResourceList = null;
        for (AnalysisResourceList list : resourceMetadata.getAnalysisResourceLists()) {
            if (analysisId.equals(list.getAnalysisId())) {
                analysisResourceList = list;
                break;
            }
        }

        File resourceFile = resourceManager.getResourceFile(analysisId, resourceName, version);
        Assert.assertTrue(Files.exists(resourceFile.toPath()));
        Assert.assertTrue(analysisResourceList.getResources().stream().map(r -> Paths.get(r.getLocalRelativePath()).getFileName().toString()).collect(Collectors.toList()).contains(resourceFile.getName()));
    }

    //-------------------------------------------------------------------------
    // P R I V A T E
    //-------------------------------------------------------------------------

    private ResourceMetadata createResourceMetadata() {
        ResourceMetadata resourceMetadata = new ResourceMetadata();
        resourceMetadata.setVersion(GitRepositoryState.getInstance().getBuildVersion());

        // Liftover
        AnalysisResourceList analysisResourceList = new AnalysisResourceList();
        analysisResourceList.setAnalysisId("liftover");
        AnalysisResource analysisResource = new AnalysisResource();
        analysisResource.setUrl("data/chain.frq");
        analysisResourceList.getResources().add(analysisResource);
        resourceMetadata.getAnalysisResourceLists().add(analysisResourceList);

        // QC
        analysisResourceList = new AnalysisResourceList();
        analysisResourceList.setAnalysisId("qc");
        analysisResource = new AnalysisResource();
        analysisResource.setUrl("data/relatedness_thresholds.tsv");
        analysisResourceList.getResources().add(analysisResource);
        analysisResource = new AnalysisResource();
        analysisResource.setUrl("data/prune.out");
        analysisResourceList.getResources().add(analysisResource);
        resourceMetadata.getAnalysisResourceLists().add(analysisResourceList);

        return resourceMetadata;
    }

    private Path createDir(String name) throws IOException {
        Path path;
        int c = 0;
        do {
            path = Paths.get("target/test-data").resolve("junit_opencga_" + name + "_"+ TimeUtils.getTimeMillis() + (c > 0 ? "_" + c : ""));
            c++;
        } while (path.toFile().exists());

        Files.createDirectories(path);
        return path;
    }

    /*
$ cat resources-3.3.0-SNAPSHOT.json | jq
{
  "version": "3.3.0-SNAPSHOT",
  "analysisResourceLists": [
    {
      "analysisId": "liftover",
      "resources": [
        {
          "name": "chain.frq",
          "url": "http://resources.opencb.org/task-6766/resources/data/liftover/chain-v31.2.frq",
          "md5": "2e74f0b1db0a4138fc21216036a3d28b",
          "target": [],
          "action": []
        }
      ]
    },
    {
      "analysisId": "exomiser",
      "resources": [
        {
          "name": "",
          "url": "http://resources.opencb.org/task-6766/resources/data/exomiser/2222_hg38.zip",
          "md5": "396fa5c808245262c4f3a70468387374",
          "target": [],
          "action": [
            "UNZIP"
          ]
        }
      ]
    },
    {
      "analysisId": "qc",
      "resources": [
        {
          "name": "",
          "url": "http://resources.opencb.org/task-6766/resources/data/qc/relatedness_thresholds.tsv",
          "md5": "97175d1574f14fe45bae13ffe277f295",
          "target": [
            "family",
            "individual"
          ],
          "action": []
        },
        {
          "name": "prune.out",
          "url": "http://resources.opencb.org/task-6766/resources/data/qc/prune-20241015.out",
          "md5": "0d7ce55d5c48870698633ca7694e95d3",
          "target": [],
          "action": []
        }
      ]
    }
  ]
}
*/
}