/*
 * Copyright 2015-2020 OpenCB
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.opencb.opencga.catalog.utils;

import org.apache.commons.lang3.RandomStringUtils;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.opencga.catalog.exceptions.CatalogException;
import org.opencb.opencga.catalog.managers.AbstractManagerTest;
import org.opencb.opencga.core.models.file.File;
import org.opencb.opencga.core.models.file.FileCreateParams;
import org.opencb.opencga.core.models.file.FileStatus;
import org.opencb.opencga.core.models.file.FileUpdateParams;
import org.opencb.opencga.core.models.sample.Sample;
import org.opencb.opencga.core.testclassification.duration.MediumTests;

import java.io.*;
import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardCopyOption;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.*;
import static org.opencb.opencga.catalog.utils.FileMetadataReader.VARIANT_FILE_METADATA;

@Category(MediumTests.class)
public class FileMetadataReaderTest extends AbstractManagerTest {

//    @Rule
//    public CatalogManagerExternalResource catalogManagerResource = new CatalogManagerExternalResource();
//
//    private CatalogManager catalogManager;
//    protected String organizationId = "zetta";
//    private String ownerToken;
//    private Project project;
//    private Study study;
    private File folder;
    private URI vcfFileUri;
    public static final String VCF_FILE_NAME = "variant-test-file.vcf.gz";
    public static final String BAM_FILE_NAME = "HG00096.chrom20.small.bam";
    private URI bamFileUri;
    private final List<String> expectedSampleNames = Arrays.asList("NA19600", "NA19660", "NA19661", "NA19685");

//    protected static final QueryOptions INCLUDE_RESULT = new QueryOptions(ParamConstants.INCLUDE_RESULT_PARAM, true);

    @Before
    public void setUp() throws Exception {
        super.setUp();
//        catalogManager = catalogManagerResource.getCatalogManager();
//
//        String opencgaToken = catalogManager.getUserManager().loginAsAdmin(TestParamConstants.ADMIN_PASSWORD).getToken();
//
//        catalogManager.getUserManager().create(organizationId, "user", "User Name", "mail@ebi.ac.uk", TestParamConstants.PASSWORD, "", null, Account.AccountType.FULL, opencgaToken);
//        ownerToken = catalogManager.getUserManager().login(organizationId, "user", TestParamConstants.PASSWORD).getToken();
//        project = catalogManager.getProjectManager().create(organizationId, "1000G", "Project about some genomes", "", "Homo sapiens",
//                null, "GRCh38", INCLUDE_RESULT, ownerToken).first();
//        study = catalogManager.getStudyManager().create(project.getId(), "phase1", null, "Phase 1", "Done", null, null, null, null, INCLUDE_RESULT, ownerToken).first();
        folder = catalogManager.getFileManager().createFolder(studyFqn, Paths.get("data/vcf/").toString(), true,
                null, QueryOptions.empty(), ownerToken).first();

        Path vcfPath = catalogManagerResource.getOpencgaHome().resolve(VCF_FILE_NAME);
        Files.copy(this.getClass().getClassLoader().getResourceAsStream("biofiles/" + VCF_FILE_NAME), vcfPath, StandardCopyOption.REPLACE_EXISTING);
        vcfFileUri = vcfPath.toUri();

        Path bamPath = catalogManagerResource.getOpencgaHome().resolve(BAM_FILE_NAME);
        Files.copy(this.getClass().getClassLoader().getResourceAsStream("biofiles/" + BAM_FILE_NAME), bamPath, StandardCopyOption.REPLACE_EXISTING);
        bamFileUri = bamPath.toUri();

    }

    @Test
    public void testGetBasicMetadata() throws CatalogException, IOException {
        File file = catalogManager.getFileManager().create(studyFqn,
                new FileCreateParams()
                        .setContent(RandomStringUtils.randomAlphanumeric(1000))
                        .setType(File.Type.FILE)
                        .setPath(folder.getPath() + "test.txt"),
                false, ownerToken).first();

        assertEquals(1000, file.getSize());

        URI fileUri = catalogManager.getFileManager().getUri(organizationId, file);

        try {
            Thread.sleep(1000); //Sleep 1 second to see changes on the "modificationDate"
        } catch (InterruptedException ignored) {
        }

        OutputStream outputStream = new FileOutputStream(Paths.get(fileUri).toFile(), true);
        byte[] bytes2 = RandomStringUtils.randomAlphanumeric(100).getBytes();
        outputStream.write(bytes2);
        outputStream.close();


        FileMetadataReader.get(catalogManager).addMetadataInformation(studyFqn, file);

        assertEquals(1000 + bytes2.length, file.getSize());
    }

    @Test
    public void testGetMetadataFromVcf() throws CatalogException, IOException {
        File file;
        try (InputStream inputStream = new BufferedInputStream(new FileInputStream(new java.io.File(vcfFileUri)))) {
            file = catalogManager.getFileManager().upload(studyFqn, inputStream,
                    new File().setPath(folder.getPath() + VCF_FILE_NAME), false, false, ownerToken).first();
        }

        assertTrue(file.getSize() > 0);

        FileMetadataReader.get(catalogManager).addMetadataInformation(studyFqn, file);

        assertEquals(FileStatus.READY, file.getInternal().getStatus().getId());
        assertEquals(File.Format.VCF, file.getFormat());
        assertEquals(File.Bioformat.VARIANT, file.getBioformat());
        assertNotNull(file.getAttributes().get(VARIANT_FILE_METADATA));
        assertEquals(4, file.getSampleIds().size());
        assertEquals(expectedSampleNames, ((Map<String, Object>) file.getAttributes().get(VARIANT_FILE_METADATA)).get("sampleIds"));
//        catalogManager.getSampleManager().search(studyFqn, new Query(SampleDBAdaptor.QueryParams.ID.key(),
//                file.getSamples().stream().map(Sample::getId).collect(Collectors.toList())), new QueryOptions(), ownerToken).getResults();
//
//        assertTrue(expectedSampleNames.containsAll(file.getSamples().stream().map(Sample::getId).collect(Collectors.toSet())));
    }


    @Test
    public void testGetMetadataFromVcfWithAlreadyExistingSamples() throws CatalogException, IOException {
        //Create the samples in the same order than in the file
        for (String sampleName : expectedSampleNames) {
            catalogManager.getSampleManager().create(studyFqn, new Sample().setId(sampleName), new QueryOptions(), ownerToken);
        }
        testGetMetadataFromVcf();
    }

    @Test
    public void testGetMetadataFromVcfWithAlreadyExistingSamplesUnsorted() throws CatalogException, IOException {
        //Create samples in a different order than the file order
        catalogManager.getSampleManager().create(studyFqn, new Sample().setId(expectedSampleNames.get(2)), new QueryOptions(), ownerToken);

        catalogManager.getSampleManager().create(studyFqn, new Sample().setId(expectedSampleNames.get(0)), new QueryOptions(), ownerToken);
        catalogManager.getSampleManager().create(studyFqn, new Sample().setId(expectedSampleNames.get(3)), new QueryOptions(), ownerToken);
        catalogManager.getSampleManager().create(studyFqn, new Sample().setId(expectedSampleNames.get(1)), new QueryOptions(), ownerToken);

        testGetMetadataFromVcf();
    }

    @Test
    public void testGetMetadataFromVcfWithSomeExistingSamples() throws CatalogException, IOException {
        catalogManager.getSampleManager().create(studyFqn, new Sample().setId(expectedSampleNames.get(2)), new QueryOptions(),
                ownerToken);
        catalogManager.getSampleManager().create(studyFqn, new Sample().setId(expectedSampleNames.get(0)), new QueryOptions(),
                ownerToken);

        testGetMetadataFromVcf();
    }

    @Test
    public void testDoNotOverwriteSampleIds() throws CatalogException, IOException {
        File file;
        try (InputStream inputStream = new BufferedInputStream(new FileInputStream(new java.io.File(vcfFileUri)))) {
            file = catalogManager.getFileManager().upload(studyFqn, inputStream,
                    new File().setPath(folder.getPath() + VCF_FILE_NAME), false, false, ownerToken).first();
        }
        assertEquals(FileStatus.READY, file.getInternal().getStatus().getId());
        assertEquals(File.Format.VCF, file.getFormat());
        assertEquals(File.Bioformat.VARIANT, file.getBioformat());
        assertNotNull(file.getAttributes().get(VARIANT_FILE_METADATA));
        assertEquals(4, file.getSampleIds().size());

        //Add a sampleId
        String sampleId = catalogManager.getSampleManager().create(studyFqn, new Sample().setId("Bad_Sample"), INCLUDE_RESULT, ownerToken)
                .first().getId();
        catalogManager.getFileManager().update(studyFqn, file.getPath(),
                new FileUpdateParams().setSampleIds(Collections.singletonList(sampleId)), new QueryOptions(), ownerToken);

        file = catalogManager.getFileManager().get(studyFqn, file.getPath(), null, ownerToken).first();
        assertEquals(5, file.getSampleIds().size());
        assertEquals(sampleId, file.getSampleIds().get(4));
    }

    @Test
    public void testGetMetadataFromBam() throws CatalogException, IOException {
        File file;
        try (InputStream inputStream = new BufferedInputStream(new FileInputStream(new java.io.File(bamFileUri)))) {
            file = catalogManager.getFileManager().upload(studyFqn, inputStream,
                    new File().setPath(folder.getPath() + BAM_FILE_NAME), false, false, ownerToken).first();
        }

        assertTrue(file.getSize() > 0);

        FileMetadataReader.get(catalogManager).addMetadataInformation(studyFqn, file);

        assertEquals(FileStatus.READY, file.getInternal().getStatus().getId());
//        assertEquals(File.Format.GZIP, file.getFormat());
        assertEquals(File.Bioformat.ALIGNMENT, file.getBioformat());
        assertNotNull(file.getAttributes().get("alignmentHeader"));
        assertEquals(1, file.getSampleIds().size());
//        assertEquals("HG00096", catalogManager.getSampleManager().get(studyFqn, file.getSamples().get(0).getId(), null,
//                ownerToken).first().getId());
    }


}