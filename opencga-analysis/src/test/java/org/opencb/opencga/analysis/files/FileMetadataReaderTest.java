package org.opencb.opencga.analysis.files;

import junit.framework.TestCase;
import org.junit.Before;
import org.junit.Test;
import org.opencb.datastore.core.ObjectMap;
import org.opencb.datastore.core.QueryOptions;
import org.opencb.datastore.core.QueryResult;
import org.opencb.opencga.catalog.db.api.CatalogSampleDBAdaptor;
import org.opencb.opencga.catalog.exceptions.CatalogException;
import org.opencb.opencga.catalog.models.Sample;
import org.opencb.opencga.catalog.utils.CatalogFileUtils;
import org.opencb.opencga.catalog.CatalogManager;
import org.opencb.opencga.catalog.CatalogManagerTest;
import org.opencb.opencga.catalog.models.File;
import org.opencb.opencga.catalog.models.Project;
import org.opencb.opencga.catalog.models.Study;
import org.opencb.opencga.core.common.StringUtils;
import org.opencb.opencga.core.common.TimeUtils;
import org.opencb.opencga.storage.core.StorageManager;
import org.opencb.opencga.storage.core.StorageManagerException;

import java.io.*;
import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardCopyOption;
import java.util.*;
import java.util.function.Function;
import java.util.stream.Collectors;

import static org.opencb.opencga.catalog.db.api.CatalogSampleDBAdaptor.*;

public class FileMetadataReaderTest extends TestCase {

    public static final String PASSWORD = "asdf";
    private CatalogManager catalogManager;
    private String sessionIdUser;
    private Project project;
    private Study study;
    private File folder;
    private URI vcfFileUri;
    public static final String VCF_FILE_NAME = "variant-test-file.vcf.gz";
    public static final String BAM_FILE_NAME = "HG00096.chrom20.small.bam";
    private URI bamFileUri;
    private final List<String> expectedSampleNames = Arrays.asList("NA19600", "NA19660", "NA19661", "NA19685");

    @Before
    public void setUp() throws IOException, CatalogException {
        InputStream is = CatalogManagerTest.class.getClassLoader().getResourceAsStream("catalog.properties");
        Properties properties = new Properties();
        properties.load(is);

        CatalogManagerTest.clearCatalog(properties);

        catalogManager = new CatalogManager(properties);

        catalogManager.createUser("user", "User Name", "mail@ebi.ac.uk", PASSWORD, "", null);
        sessionIdUser = catalogManager.login("user", PASSWORD, "127.0.0.1").first().getString("sessionId");
        project = catalogManager.createProject("user", "Project about some genomes", "1000G", "", "ACME", null, sessionIdUser).first();
        study = catalogManager.createStudy(project.getId(), "Phase 1", "phase1", Study.Type.TRIO, "Done", sessionIdUser).first();
        folder = catalogManager.createFolder(study.getId(), Paths.get("data/vcf/"), true, null, sessionIdUser).first();
        Path vcfPath = Paths.get("/tmp", VCF_FILE_NAME);
        Files.copy(this.getClass().getClassLoader().getResourceAsStream(VCF_FILE_NAME), vcfPath, StandardCopyOption.REPLACE_EXISTING);
        vcfFileUri = vcfPath.toUri();

        Path bamPath = Paths.get("/tmp", BAM_FILE_NAME);
        Files.copy(this.getClass().getClassLoader().getResourceAsStream(BAM_FILE_NAME), bamPath, StandardCopyOption.REPLACE_EXISTING);
        bamFileUri = bamPath.toUri();
    }

    @Test
    public void testCreate() throws CatalogException {
        QueryResult<File> fileQueryResult = FileMetadataReader.get(catalogManager).
                create(study.getId(), vcfFileUri, folder.getPath() + VCF_FILE_NAME, "", false, null, sessionIdUser);

        File file = fileQueryResult.first();

        assertEquals(File.Status.STAGE, file.getStatus());
        assertEquals(File.Format.VCF, file.getFormat());
        assertEquals(File.Bioformat.VARIANT, file.getBioformat());
        assertNotNull(file.getAttributes().get("variantSource"));
        assertEquals(4, file.getSampleIds().size());
        assertEquals(21473, file.getDiskUsage());

        new CatalogFileUtils(catalogManager).upload(vcfFileUri, file, null, sessionIdUser, false, false, true, true, Integer.MAX_VALUE);
        file = catalogManager.getFile(file.getId(), sessionIdUser).first();

        assertEquals(File.Status.READY, file.getStatus());
        assertEquals(File.Format.VCF, file.getFormat());
        assertEquals(File.Bioformat.VARIANT, file.getBioformat());
        assertNotNull(file.getAttributes().get("variantSource"));
        assertEquals(4, file.getSampleIds().size());
        assertEquals(21473, file.getDiskUsage());
    }

    @Test
    public void testGetBasicMetadata() throws CatalogException, IOException, StorageManagerException {
        byte[] bytes = StringUtils.randomString(1000).getBytes();
        File file = catalogManager.createFile(study.getId(), File.Format.PLAIN,
                File.Bioformat.NONE, folder.getPath() + "test.txt", bytes, "", false, sessionIdUser).first();

        assertEquals(bytes.length, file.getDiskUsage());

        String creationDate = file.getCreationDate();
        String modificationDate = file.getModificationDate();

        URI fileUri = catalogManager.getFileUri(file);

        try {
            Thread.sleep(1000); //Sleep 1 second to see changes on the "modificationDate"
        } catch (InterruptedException ignored) {}

        OutputStream outputStream = new FileOutputStream(Paths.get(fileUri).toFile(), true);
        byte[] bytes2 = StringUtils.randomString(100).getBytes();
        outputStream.write(bytes2);
        outputStream.close();

        file = FileMetadataReader.get(catalogManager).
                setMetadataInformation(file, null, null, sessionIdUser, false);

        assertEquals(bytes.length + bytes2.length, file.getDiskUsage());
        assertTrue(TimeUtils.toDate(modificationDate).getTime() < TimeUtils.toDate(file.getModificationDate()).getTime());
        assertEquals(creationDate, file.getCreationDate());

    }

    @Test
    public void testGetMetadataFromVcf()
            throws CatalogException, StorageManagerException {
        QueryResult<File> fileQueryResult = catalogManager.createFile(study.getId(), File.Format.PLAIN,
                File.Bioformat.NONE, folder.getPath() + VCF_FILE_NAME, "", false, -1, sessionIdUser);

        File file = fileQueryResult.first();

        assertEquals(File.Status.STAGE, file.getStatus());
        assertEquals(File.Format.PLAIN, file.getFormat());
        assertEquals(File.Bioformat.NONE, file.getBioformat());
        assertNull(file.getAttributes().get("variantSource"));
        assertEquals(0, file.getSampleIds().size());
        assertEquals(0, file.getDiskUsage());

        new CatalogFileUtils(catalogManager).
                upload(vcfFileUri, file, null, sessionIdUser, false, false, true, true, Integer.MAX_VALUE);

        file = catalogManager.getFile(file.getId(), null, sessionIdUser).first();
        assertTrue(file.getDiskUsage() > 0);

        file = FileMetadataReader.get(catalogManager).
                setMetadataInformation(file, null, null, sessionIdUser, false);

        assertEquals(File.Status.READY, file.getStatus());
        assertEquals(File.Format.VCF, file.getFormat());
        assertEquals(File.Bioformat.VARIANT, file.getBioformat());
        assertNotNull(file.getAttributes().get("variantSource"));
        assertEquals(4, file.getSampleIds().size());
        assertEquals(expectedSampleNames, ((Map<String, Object>) file.getAttributes().get("variantSource")).get("samples"));
        List<Sample> samples = catalogManager.getAllSamples(study.getId(), new QueryOptions(SampleFilterOption.id.toString(), file.getSampleIds()), sessionIdUser).getResult();
        Map<Integer, Sample> sampleMap = samples.stream().collect(Collectors.toMap(Sample::getId, Function.identity()));
        assertEquals(expectedSampleNames.get(0), sampleMap.get(file.getSampleIds().get(0)).getName());
        assertEquals(expectedSampleNames.get(1), sampleMap.get(file.getSampleIds().get(1)).getName());
        assertEquals(expectedSampleNames.get(2), sampleMap.get(file.getSampleIds().get(2)).getName());
        assertEquals(expectedSampleNames.get(3), sampleMap.get(file.getSampleIds().get(3)).getName());

    }


    @Test
    public void testGetMetadataFromVcfWithAlreadyExistingSamples() throws CatalogException, StorageManagerException {
        //Create the samples in the same order than in the file
        for (String sampleName : expectedSampleNames) {
            catalogManager.createSample(study.getId(), sampleName, "", "", Collections.emptyMap(), new QueryOptions(), sessionIdUser);
        }
        testGetMetadataFromVcf();
    }

    @Test
    public void testGetMetadataFromVcfWithAlreadyExistingSamplesUnsorted() throws CatalogException, StorageManagerException {
        //Create samples in a different order than the file order
        catalogManager.createSample(study.getId(), expectedSampleNames.get(2), "", "", Collections.emptyMap(), new QueryOptions(), sessionIdUser);
        catalogManager.createSample(study.getId(), expectedSampleNames.get(0), "", "", Collections.emptyMap(), new QueryOptions(), sessionIdUser);
        catalogManager.createSample(study.getId(), expectedSampleNames.get(3), "", "", Collections.emptyMap(), new QueryOptions(), sessionIdUser);
        catalogManager.createSample(study.getId(), expectedSampleNames.get(1), "", "", Collections.emptyMap(), new QueryOptions(), sessionIdUser);

        testGetMetadataFromVcf();
    }

    @Test
    public void testGetMetadataFromVcfWithSomeExistingSamples() throws CatalogException, StorageManagerException {
        catalogManager.createSample(study.getId(), expectedSampleNames.get(2), "", "", Collections.emptyMap(), new QueryOptions(), sessionIdUser);
        catalogManager.createSample(study.getId(), expectedSampleNames.get(0), "", "", Collections.emptyMap(), new QueryOptions(), sessionIdUser);

        testGetMetadataFromVcf();
    }

    @Test
    public void testDoNotOverwriteSampleIds() throws CatalogException, StorageManagerException {
        File file = catalogManager.createFile(study.getId(), File.Format.PLAIN,
                File.Bioformat.NONE, folder.getPath() + VCF_FILE_NAME, "", false, -1, sessionIdUser).first();

        new CatalogFileUtils(catalogManager).
                upload(vcfFileUri, file, null, sessionIdUser, false, false, true, true, Integer.MAX_VALUE);

        //Add a sampleId
        int sampleId = catalogManager.createSample(study.getId(), "Bad_Sample", "Air", "", null, null, sessionIdUser).first().getId();
        catalogManager.modifyFile(file.getId(), new ObjectMap("sampleIds", Collections.singletonList(sampleId)), sessionIdUser);

        file = catalogManager.getFile(file.getId(), null, sessionIdUser).first();
        assertEquals(1, file.getSampleIds().size());
        assertEquals(sampleId, file.getSampleIds().get(0).intValue());

        file = FileMetadataReader.get(catalogManager).
                setMetadataInformation(file, null, null, sessionIdUser, false);

        assertEquals(File.Status.READY, file.getStatus());
        assertEquals(File.Format.VCF, file.getFormat());
        assertEquals(File.Bioformat.VARIANT, file.getBioformat());
        assertNotNull(file.getAttributes().get("variantSource"));
        assertEquals(1, file.getSampleIds().size());
        assertTrue(file.getSampleIds().contains(sampleId));
    }

    @Test
    public void testGetMetadataFromBam()
            throws CatalogException, StorageManagerException {

        File file = catalogManager.createFile(study.getId(), File.Format.PLAIN,
                File.Bioformat.NONE, folder.getPath() + BAM_FILE_NAME, "", false, -1, sessionIdUser).first();

        assertEquals(File.Status.STAGE, file.getStatus());
        assertEquals(File.Format.PLAIN, file.getFormat());
        assertEquals(File.Bioformat.NONE, file.getBioformat());
        assertNull(file.getAttributes().get("variantSource"));
        assertEquals(0, file.getSampleIds().size());
        assertEquals(0, file.getDiskUsage());

        new CatalogFileUtils(catalogManager).
                upload(bamFileUri, file, null, sessionIdUser, false, false, true, true, Integer.MAX_VALUE);

        file = catalogManager.getFile(file.getId(), null, sessionIdUser).first();

        assertTrue(file.getDiskUsage() > 0);

        file = FileMetadataReader.get(catalogManager).
                setMetadataInformation(file, null, null, sessionIdUser, false);

        assertEquals(File.Status.READY, file.getStatus());
//        assertEquals(File.Format.GZIP, file.getFormat());
        assertEquals(File.Bioformat.ALIGNMENT, file.getBioformat());
        assertNotNull(file.getAttributes().get("alignmentHeader"));
        assertEquals(1, file.getSampleIds().size());
        assertEquals("HG00096", catalogManager.getSample(file.getSampleIds().get(0), null, sessionIdUser).first().getName());
    }


}