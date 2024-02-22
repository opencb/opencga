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

package org.opencb.opencga.catalog.managers;

import com.fasterxml.jackson.core.JsonProcessingException;
import org.apache.commons.lang3.RandomStringUtils;
import org.apache.commons.lang3.StringUtils;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.opencb.biodata.models.clinical.interpretation.Software;
import org.opencb.commons.datastore.core.DataResult;
import org.opencb.commons.datastore.core.ObjectMap;
import org.opencb.commons.datastore.core.Query;
import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.opencga.TestParamConstants;
import org.opencb.opencga.catalog.db.api.FileDBAdaptor;
import org.opencb.opencga.catalog.db.api.SampleDBAdaptor;
import org.opencb.opencga.catalog.db.api.StudyDBAdaptor;
import org.opencb.opencga.catalog.db.mongodb.MongoBackupUtils;
import org.opencb.opencga.catalog.exceptions.*;
import org.opencb.opencga.catalog.io.IOManager;
import org.opencb.opencga.catalog.utils.Constants;
import org.opencb.opencga.catalog.utils.ParamUtils;
import org.opencb.opencga.core.api.ParamConstants;
import org.opencb.opencga.core.common.TimeUtils;
import org.opencb.opencga.core.common.UriUtils;
import org.opencb.opencga.core.models.AclEntryList;
import org.opencb.opencga.core.models.common.AnnotationSet;
import org.opencb.opencga.core.models.file.*;
import org.opencb.opencga.core.models.sample.Sample;
import org.opencb.opencga.core.models.study.*;
import org.opencb.opencga.core.response.OpenCGAResult;
import org.opencb.opencga.core.testclassification.duration.MediumTests;

import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.FileInputStream;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

import static org.hamcrest.CoreMatchers.*;
import static org.junit.Assert.*;

/**
 * Created by pfurio on 24/08/16.
 */
@Category(MediumTests.class)
public class FileManagerTest extends AbstractManagerTest {

    private FileManager fileManager;

    @Before
    public void setUp() throws Exception {
        super.setUp();
        fileManager = catalogManager.getFileManager();
        // Ensure this threshold is restored
        fileManager.setFileSampleLinkThreshold(organizationId, 5000);
    }

    private DataResult<File> link(URI uriOrigin, String pathDestiny, String studyIdStr, ObjectMap params, String sessionId)
            throws CatalogException {
        return fileManager.link(studyIdStr, uriOrigin, pathDestiny, params, sessionId);
    }

    @Test
    public void testCreateFileFromUnsharedStudy() throws CatalogException {
        try {
            fileManager.create(studyFqn2, new FileCreateParams()
                            .setType(File.Type.FILE)
                            .setPath("data/test/folder/file.txt")
                            .setDescription("My description"),
                    true, normalToken1);
            fail("The file could be created despite not having the proper permissions.");
        } catch (CatalogAuthorizationException e) {
            assertEquals(0, fileManager.search(studyFqn2, new Query(FileDBAdaptor.QueryParams.PATH.key(),
                    "data/test/folder/file.txt"), null, ownerToken).getNumResults());
        }
    }

    @Test
    public void testCreateFileFromSharedStudy() throws CatalogException {
        StudyAclParams aclParams = new StudyAclParams("", "analyst");
        catalogManager.getStudyManager().updateAcl(studyFqn, normalUserId2, aclParams, ParamUtils.AclAction.ADD, ownerToken);
        fileManager.create(studyFqn,
                new FileCreateParams()
                        .setType(File.Type.FILE)
                        .setPath("data/test/folder/file.txt")
                        .setDescription("My description")
                        .setContent("blabla"),
                true, normalToken2);
        assertEquals(1, fileManager.search(studyFqn, new Query(FileDBAdaptor.QueryParams.PATH.key(),
                "data/test/folder/file.txt"), null, ownerToken).getNumResults());
    }

    URI getStudyURI() throws CatalogException {
        return catalogManager.getStudyManager().get(studyFqn,
                new QueryOptions(QueryOptions.INCLUDE, StudyDBAdaptor.QueryParams.URI.key()), ownerToken).first().getUri();
    }

    @Test
    public void testSearchById() throws CatalogException {
        Query query = new Query(FileDBAdaptor.QueryParams.ID.key(), "~/^data/");
        OpenCGAResult<File> search = catalogManager.getFileManager().search(studyFqn, query, FileManager.INCLUDE_FILE_IDS, ownerToken);
        assertEquals(11, search.getNumResults());
    }

    @Test
    public void testLinkCram() throws CatalogException {
        String reference = getClass().getResource("/biofiles/cram/hg19mini.fasta").getFile();
        File referenceFile = fileManager.link(studyFqn, Paths.get(reference).toUri(), "", null, ownerToken).first();
        assertEquals(File.Format.FASTA, referenceFile.getFormat());
        assertEquals(File.Bioformat.REFERENCE_GENOME, referenceFile.getBioformat());

        SmallRelatedFileParams relatedFile = new SmallRelatedFileParams("hg19mini.fasta", FileRelatedFile.Relation.REFERENCE_GENOME);
        String cramFile = getClass().getResource("/biofiles/cram/cram_with_crai_index.cram").getFile();
        DataResult<File> link = fileManager.link(studyFqn, Paths.get(cramFile).toUri(), "",
                new ObjectMap("relatedFiles", Collections.singletonList(relatedFile)), ownerToken);
        assertTrue(!link.first().getAttributes().isEmpty());
        assertNotNull(link.first().getAttributes().get("alignmentHeader"));
        assertEquals(File.Format.CRAM, link.first().getFormat());
        assertEquals(File.Bioformat.ALIGNMENT, link.first().getBioformat());
        assertEquals(referenceFile.getId(), link.first().getRelatedFiles().get(0).getFile().getId());
        assertEquals(FileRelatedFile.Relation.REFERENCE_GENOME, link.first().getRelatedFiles().get(0).getRelation());

        Sample sample = catalogManager.getSampleManager().get(studyFqn, link.first().getSampleIds().get(0), QueryOptions.empty(), ownerToken).first();
        assertEquals("cram_with_crai_index.cram", sample.getFileIds().get(0));
    }

    @Test
    public void testLinkAnalystUser() throws CatalogException {
        catalogManager.getUserManager().create("analyst", "analyst", "a@mail.com", TestParamConstants.PASSWORD, organizationId, 200000L, opencgaToken);
        catalogManager.getStudyManager().updateAcl(studyFqn, "analyst", new StudyAclParams("", "analyst"), ParamUtils.AclAction.SET, ownerToken);
        String analystToken = catalogManager.getUserManager().login(organizationId, "analyst", TestParamConstants.PASSWORD).getToken();

        String reference = getClass().getResource("/biofiles/cram/hg19mini.fasta").getFile();
        File referenceFile = fileManager.link(studyFqn, Paths.get(reference).toUri(), "", null, analystToken).first();
        assertEquals(File.Format.FASTA, referenceFile.getFormat());
        assertEquals(File.Bioformat.REFERENCE_GENOME, referenceFile.getBioformat());
    }

    @Test
    public void testLinkUserWithNoWritePermissions() throws CatalogException {
        catalogManager.getUserManager().create("view_user", "view_user", "a@mail.com", TestParamConstants.PASSWORD, organizationId, 200000L, opencgaToken);
        catalogManager.getStudyManager().updateAcl(studyFqn, "view_user", new StudyAclParams("", "view_only"), ParamUtils.AclAction.SET, ownerToken);
        String analystToken = catalogManager.getUserManager().login(organizationId, "view_user", TestParamConstants.PASSWORD).getToken();

        String reference = getClass().getResource("/biofiles/cram/hg19mini.fasta").getFile();

        thrown.expect(CatalogException.class);
        thrown.expectMessage("WRITE_FILES");
        fileManager.link(studyFqn, Paths.get(reference).toUri(), "", null, analystToken).first();
    }

    @Test
    public void testLinkFileWithoutReadPermissions() throws IOException, CatalogException {
        java.io.File file = MongoBackupUtils.createDebugFile("/tmp/file_" + RandomStringUtils.randomAlphanumeric(5) + ".vcf");
        Files.setPosixFilePermissions(Paths.get(file.toURI()), new HashSet<>());
        thrown.expect(CatalogIOException.class);
        thrown.expectMessage("read VariantSource");
        fileManager.link(studyFqn, new FileLinkParams().setUri(file.getPath()), false, ownerToken);
    }

    @Test
    public void createDirectoryTest() throws CatalogException {
        FileCreateParams params = new FileCreateParams()
                .setType(File.Type.DIRECTORY)
                .setPath("/files/folder");
        File file = fileManager.create(studyFqn, params, true, ownerToken).first();
        assertEquals(params.getPath().substring(1) + "/", file.getPath());
    }

    @Test
    public void createDirectoryFailTest() throws CatalogException {
        FileCreateParams params = new FileCreateParams()
                .setType(File.Type.DIRECTORY)
                .setPath("/files/folder/");
        thrown.expect(CatalogException.class);
        thrown.expectMessage("parents");
        fileManager.create(studyFqn, params, false, ownerToken);
    }

    @Test
    public void createWithBase64File1Test() throws CatalogException {
        String base64 = "iVBORw0KGgoAAAANSUhEUgAAABgAAAAYCAYAAADgdz34AAAABHNCSVQICAgIfAhkiAAAAAlwSFlzAAAApgAAAKYB3X3/OAAAABl0RVh0U29mdHdhcmUAd3d3Lmlua3NjYXBlLm9yZ5vuPBoAAANCSURBVEiJtZZPbBtFFMZ/M7ubXdtdb1xSFyeilBapySVU8h8OoFaooFSqiihIVIpQBKci6KEg9Q6H9kovIHoCIVQJJCKE1ENFjnAgcaSGC6rEnxBwA04Tx43t2FnvDAfjkNibxgHxnWb2e/u992bee7tCa00YFsffekFY+nUzFtjW0LrvjRXrCDIAaPLlW0nHL0SsZtVoaF98mLrx3pdhOqLtYPHChahZcYYO7KvPFxvRl5XPp1sN3adWiD1ZAqD6XYK1b/dvE5IWryTt2udLFedwc1+9kLp+vbbpoDh+6TklxBeAi9TL0taeWpdmZzQDry0AcO+jQ12RyohqqoYoo8RDwJrU+qXkjWtfi8Xxt58BdQuwQs9qC/afLwCw8tnQbqYAPsgxE1S6F3EAIXux2oQFKm0ihMsOF71dHYx+f3NND68ghCu1YIoePPQN1pGRABkJ6Bus96CutRZMydTl+TvuiRW1m3n0eDl0vRPcEysqdXn+jsQPsrHMquGeXEaY4Yk4wxWcY5V/9scqOMOVUFthatyTy8QyqwZ+kDURKoMWxNKr2EeqVKcTNOajqKoBgOE28U4tdQl5p5bwCw7BWquaZSzAPlwjlithJtp3pTImSqQRrb2Z8PHGigD4RZuNX6JYj6wj7O4TFLbCO/Mn/m8R+h6rYSUb3ekokRY6f/YukArN979jcW+V/S8g0eT/N3VN3kTqWbQ428m9/8k0P/1aIhF36PccEl6EhOcAUCrXKZXXWS3XKd2vc/TRBG9O5ELC17MmWubD2nKhUKZa26Ba2+D3P+4/MNCFwg59oWVeYhkzgN/JDR8deKBoD7Y+ljEjGZ0sosXVTvbc6RHirr2reNy1OXd6pJsQ+gqjk8VWFYmHrwBzW/n+uMPFiRwHB2I7ih8ciHFxIkd/3Omk5tCDV1t+2nNu5sxxpDFNx+huNhVT3/zMDz8usXC3ddaHBj1GHj/As08fwTS7Kt1HBTmyN29vdwAw+/wbwLVOJ3uAD1wi/dUH7Qei66PfyuRj4Ik9is+hglfbkbfR3cnZm7chlUWLdwmprtCohX4HUtlOcQjLYCu+fzGJH2QRKvP3UNz8bWk1qMxjGTOMThZ3kvgLI5AzFfo379UAAAAASUVORK5CYII=";
        FileCreateParams params = new FileCreateParams()
                .setContent(base64)
                .setFormat(File.Format.IMAGE)
                .setType(File.Type.FILE)
                .setPath("/files/folder/heart.png");
        File file = fileManager.create(studyFqn, params, true, ownerToken).first();
        FileContent content = fileManager.image(studyFqn, file.getPath(), ownerToken).first();
        assertEquals(base64, content.getContent());
        assertTrue(file.getTags().isEmpty());
        assertNull(file.getSoftware());
        assertEquals(File.Bioformat.NONE, file.getBioformat());
        assertEquals(File.Format.IMAGE, file.getFormat());
        assertTrue(file.getSampleIds().isEmpty());
    }

    @Test
    public void createWithBase64File2Test() throws CatalogException {
        String base64 = "iVBORw0KGgoAAAANSUhEUgAAABgAAAAYCAYAAADgdz34AAAABHNCSVQICAgIfAhkiAAAAAlwSFlzAAAApgAAAKYB3X3/OAAAABl0RVh0U29mdHdhcmUAd3d3Lmlua3NjYXBlLm9yZ5vuPBoAAANCSURBVEiJtZZPbBtFFMZ/M7ubXdtdb1xSFyeilBapySVU8h8OoFaooFSqiihIVIpQBKci6KEg9Q6H9kovIHoCIVQJJCKE1ENFjnAgcaSGC6rEnxBwA04Tx43t2FnvDAfjkNibxgHxnWb2e/u992bee7tCa00YFsffekFY+nUzFtjW0LrvjRXrCDIAaPLlW0nHL0SsZtVoaF98mLrx3pdhOqLtYPHChahZcYYO7KvPFxvRl5XPp1sN3adWiD1ZAqD6XYK1b/dvE5IWryTt2udLFedwc1+9kLp+vbbpoDh+6TklxBeAi9TL0taeWpdmZzQDry0AcO+jQ12RyohqqoYoo8RDwJrU+qXkjWtfi8Xxt58BdQuwQs9qC/afLwCw8tnQbqYAPsgxE1S6F3EAIXux2oQFKm0ihMsOF71dHYx+f3NND68ghCu1YIoePPQN1pGRABkJ6Bus96CutRZMydTl+TvuiRW1m3n0eDl0vRPcEysqdXn+jsQPsrHMquGeXEaY4Yk4wxWcY5V/9scqOMOVUFthatyTy8QyqwZ+kDURKoMWxNKr2EeqVKcTNOajqKoBgOE28U4tdQl5p5bwCw7BWquaZSzAPlwjlithJtp3pTImSqQRrb2Z8PHGigD4RZuNX6JYj6wj7O4TFLbCO/Mn/m8R+h6rYSUb3ekokRY6f/YukArN979jcW+V/S8g0eT/N3VN3kTqWbQ428m9/8k0P/1aIhF36PccEl6EhOcAUCrXKZXXWS3XKd2vc/TRBG9O5ELC17MmWubD2nKhUKZa26Ba2+D3P+4/MNCFwg59oWVeYhkzgN/JDR8deKBoD7Y+ljEjGZ0sosXVTvbc6RHirr2reNy1OXd6pJsQ+gqjk8VWFYmHrwBzW/n+uMPFiRwHB2I7ih8ciHFxIkd/3Omk5tCDV1t+2nNu5sxxpDFNx+huNhVT3/zMDz8usXC3ddaHBj1GHj/As08fwTS7Kt1HBTmyN29vdwAw+/wbwLVOJ3uAD1wi/dUH7Qei66PfyuRj4Ik9is+hglfbkbfR3cnZm7chlUWLdwmprtCohX4HUtlOcQjLYCu+fzGJH2QRKvP3UNz8bWk1qMxjGTOMThZ3kvgLI5AzFfo379UAAAAASUVORK5CYII=";
        FileCreateParams params = new FileCreateParams()
                .setContent(base64)
                .setTags(Arrays.asList("A", "B"))
                .setBioformat(File.Bioformat.VARIANT)
                .setType(File.Type.FILE)
                .setFormat(File.Format.IMAGE)
                .setSoftware(new Software().setName("software"))
                .setSampleIds(Arrays.asList(s_1Id, s_2Id))
                .setPath("/files/folder/heart.png");
        File file = fileManager.create(studyFqn, params, true, ownerToken).first();
        FileContent content = fileManager.image(studyFqn, file.getPath(), ownerToken).first();
        assertEquals(base64, content.getContent());
        assertEquals(params.getTags().size(), file.getTags().size());
        assertTrue(file.getTags().containsAll(params.getTags()));
        assertNotNull(file.getSoftware());
        assertEquals(params.getSoftware().getName(), file.getSoftware().getName());
        assertEquals(params.getBioformat(), file.getBioformat());
        assertEquals(params.getSampleIds().size(), file.getSampleIds().size());
        assertTrue(file.getSampleIds().containsAll(params.getSampleIds()));

        OpenCGAResult<Sample> sampleResult = catalogManager.getSampleManager().get(studyFqn, params.getSampleIds(), QueryOptions.empty(),
                ownerToken);
        assertEquals(2, sampleResult.getNumResults());
        for (Sample sample : sampleResult.getResults()) {
            assertEquals(2, sample.getFileIds().size());
            assertEquals(file.getId(), sample.getFileIds().get(1));
        }
    }

    @Test
    public void createTxtFileNoExtensionTest() throws CatalogException {
        String content = "This is my content";
        FileCreateParams params = new FileCreateParams()
                .setContent(content)
                .setType(File.Type.FILE)
                .setPath("/files/folder/file");

        File file = fileManager.create(studyFqn, params, true, ownerToken).first();
        FileContent fileContent = fileManager.head(studyFqn, file.getId(), 0, 1, ownerToken).first();
        assertEquals(content, fileContent.getContent().trim());
    }

    @Test
    public void createTxtFileTest() throws CatalogException {
        String content = "This is my content";
        FileCreateParams params = new FileCreateParams()
                .setContent(content)
                .setType(File.Type.FILE)
                .setPath("/files/folder/file.txt");

        File file = fileManager.create(studyFqn, params, true, ownerToken).first();
        FileContent fileContent = fileManager.head(studyFqn, file.getId(), 0, 1, ownerToken).first();
        assertEquals(content, fileContent.getContent().trim());
    }

    @Test
    public void createWithBase64FileSampleDontExistTest() throws CatalogException {
        String base64 = "iVBORw0KGgoAAAANSUhEUgAAABgAAAAYCAYAAADgdz34AAAABHNCSVQICAgIfAhkiAAAAAlwSFlzAAAApgAAAKYB3X3/OAAAABl0RVh0U29mdHdhcmUAd3d3Lmlua3NjYXBlLm9yZ5vuPBoAAANCSURBVEiJtZZPbBtFFMZ/M7ubXdtdb1xSFyeilBapySVU8h8OoFaooFSqiihIVIpQBKci6KEg9Q6H9kovIHoCIVQJJCKE1ENFjnAgcaSGC6rEnxBwA04Tx43t2FnvDAfjkNibxgHxnWb2e/u992bee7tCa00YFsffekFY+nUzFtjW0LrvjRXrCDIAaPLlW0nHL0SsZtVoaF98mLrx3pdhOqLtYPHChahZcYYO7KvPFxvRl5XPp1sN3adWiD1ZAqD6XYK1b/dvE5IWryTt2udLFedwc1+9kLp+vbbpoDh+6TklxBeAi9TL0taeWpdmZzQDry0AcO+jQ12RyohqqoYoo8RDwJrU+qXkjWtfi8Xxt58BdQuwQs9qC/afLwCw8tnQbqYAPsgxE1S6F3EAIXux2oQFKm0ihMsOF71dHYx+f3NND68ghCu1YIoePPQN1pGRABkJ6Bus96CutRZMydTl+TvuiRW1m3n0eDl0vRPcEysqdXn+jsQPsrHMquGeXEaY4Yk4wxWcY5V/9scqOMOVUFthatyTy8QyqwZ+kDURKoMWxNKr2EeqVKcTNOajqKoBgOE28U4tdQl5p5bwCw7BWquaZSzAPlwjlithJtp3pTImSqQRrb2Z8PHGigD4RZuNX6JYj6wj7O4TFLbCO/Mn/m8R+h6rYSUb3ekokRY6f/YukArN979jcW+V/S8g0eT/N3VN3kTqWbQ428m9/8k0P/1aIhF36PccEl6EhOcAUCrXKZXXWS3XKd2vc/TRBG9O5ELC17MmWubD2nKhUKZa26Ba2+D3P+4/MNCFwg59oWVeYhkzgN/JDR8deKBoD7Y+ljEjGZ0sosXVTvbc6RHirr2reNy1OXd6pJsQ+gqjk8VWFYmHrwBzW/n+uMPFiRwHB2I7ih8ciHFxIkd/3Omk5tCDV1t+2nNu5sxxpDFNx+huNhVT3/zMDz8usXC3ddaHBj1GHj/As08fwTS7Kt1HBTmyN29vdwAw+/wbwLVOJ3uAD1wi/dUH7Qei66PfyuRj4Ik9is+hglfbkbfR3cnZm7chlUWLdwmprtCohX4HUtlOcQjLYCu+fzGJH2QRKvP3UNz8bWk1qMxjGTOMThZ3kvgLI5AzFfo379UAAAAASUVORK5CYII=";
        FileCreateParams params = new FileCreateParams()
                .setContent(base64)
                .setTags(Arrays.asList("A", "B"))
                .setBioformat(File.Bioformat.VARIANT)
                .setType(File.Type.FILE)
                .setSoftware(new Software().setName("software"))
                .setSampleIds(Arrays.asList(s_1Id, "sample1"))
                .setPath("/files/folder/heart.png");

        thrown.expect(CatalogException.class);
        thrown.expectMessage("sample1");
        fileManager.create(studyFqn, params, true, ownerToken).first();
    }

    @Test
    public void createWithBase64FileWrongPath1Test() throws CatalogException {
        String base64 = "iVBORw0KGgoAAAANSUhEUgAAABgAAAAYCAYAAADgdz34AAAABHNCSVQICAgIfAhkiAAAAAlwSFlzAAAApgAAAKYB3X3/OAAAABl0RVh0U29mdHdhcmUAd3d3Lmlua3NjYXBlLm9yZ5vuPBoAAANCSURBVEiJtZZPbBtFFMZ/M7ubXdtdb1xSFyeilBapySVU8h8OoFaooFSqiihIVIpQBKci6KEg9Q6H9kovIHoCIVQJJCKE1ENFjnAgcaSGC6rEnxBwA04Tx43t2FnvDAfjkNibxgHxnWb2e/u992bee7tCa00YFsffekFY+nUzFtjW0LrvjRXrCDIAaPLlW0nHL0SsZtVoaF98mLrx3pdhOqLtYPHChahZcYYO7KvPFxvRl5XPp1sN3adWiD1ZAqD6XYK1b/dvE5IWryTt2udLFedwc1+9kLp+vbbpoDh+6TklxBeAi9TL0taeWpdmZzQDry0AcO+jQ12RyohqqoYoo8RDwJrU+qXkjWtfi8Xxt58BdQuwQs9qC/afLwCw8tnQbqYAPsgxE1S6F3EAIXux2oQFKm0ihMsOF71dHYx+f3NND68ghCu1YIoePPQN1pGRABkJ6Bus96CutRZMydTl+TvuiRW1m3n0eDl0vRPcEysqdXn+jsQPsrHMquGeXEaY4Yk4wxWcY5V/9scqOMOVUFthatyTy8QyqwZ+kDURKoMWxNKr2EeqVKcTNOajqKoBgOE28U4tdQl5p5bwCw7BWquaZSzAPlwjlithJtp3pTImSqQRrb2Z8PHGigD4RZuNX6JYj6wj7O4TFLbCO/Mn/m8R+h6rYSUb3ekokRY6f/YukArN979jcW+V/S8g0eT/N3VN3kTqWbQ428m9/8k0P/1aIhF36PccEl6EhOcAUCrXKZXXWS3XKd2vc/TRBG9O5ELC17MmWubD2nKhUKZa26Ba2+D3P+4/MNCFwg59oWVeYhkzgN/JDR8deKBoD7Y+ljEjGZ0sosXVTvbc6RHirr2reNy1OXd6pJsQ+gqjk8VWFYmHrwBzW/n+uMPFiRwHB2I7ih8ciHFxIkd/3Omk5tCDV1t+2nNu5sxxpDFNx+huNhVT3/zMDz8usXC3ddaHBj1GHj/As08fwTS7Kt1HBTmyN29vdwAw+/wbwLVOJ3uAD1wi/dUH7Qei66PfyuRj4Ik9is+hglfbkbfR3cnZm7chlUWLdwmprtCohX4HUtlOcQjLYCu+fzGJH2QRKvP3UNz8bWk1qMxjGTOMThZ3kvgLI5AzFfo379UAAAAASUVORK5CYII=";
        FileCreateParams params = new FileCreateParams()
                .setContent(base64)
                .setFormat(File.Format.IMAGE)
                .setType(File.Type.FILE)
                .setPath("/files/folder/");
        thrown.expect(CatalogException.class);
        thrown.expectMessage("type");
        thrown.expectMessage("path");
        fileManager.create(studyFqn, params, true, ownerToken);
    }

    @Test
    public void createWithBase64FileMissingPathTest() throws CatalogException {
        String base64 = "iVBORw0KGgoAAAANSUhEUgAAABgAAAAYCAYAAADgdz34AAAABHNCSVQICAgIfAhkiAAAAAlwSFlzAAAApgAAAKYB3X3/OAAAABl0RVh0U29mdHdhcmUAd3d3Lmlua3NjYXBlLm9yZ5vuPBoAAANCSURBVEiJtZZPbBtFFMZ/M7ubXdtdb1xSFyeilBapySVU8h8OoFaooFSqiihIVIpQBKci6KEg9Q6H9kovIHoCIVQJJCKE1ENFjnAgcaSGC6rEnxBwA04Tx43t2FnvDAfjkNibxgHxnWb2e/u992bee7tCa00YFsffekFY+nUzFtjW0LrvjRXrCDIAaPLlW0nHL0SsZtVoaF98mLrx3pdhOqLtYPHChahZcYYO7KvPFxvRl5XPp1sN3adWiD1ZAqD6XYK1b/dvE5IWryTt2udLFedwc1+9kLp+vbbpoDh+6TklxBeAi9TL0taeWpdmZzQDry0AcO+jQ12RyohqqoYoo8RDwJrU+qXkjWtfi8Xxt58BdQuwQs9qC/afLwCw8tnQbqYAPsgxE1S6F3EAIXux2oQFKm0ihMsOF71dHYx+f3NND68ghCu1YIoePPQN1pGRABkJ6Bus96CutRZMydTl+TvuiRW1m3n0eDl0vRPcEysqdXn+jsQPsrHMquGeXEaY4Yk4wxWcY5V/9scqOMOVUFthatyTy8QyqwZ+kDURKoMWxNKr2EeqVKcTNOajqKoBgOE28U4tdQl5p5bwCw7BWquaZSzAPlwjlithJtp3pTImSqQRrb2Z8PHGigD4RZuNX6JYj6wj7O4TFLbCO/Mn/m8R+h6rYSUb3ekokRY6f/YukArN979jcW+V/S8g0eT/N3VN3kTqWbQ428m9/8k0P/1aIhF36PccEl6EhOcAUCrXKZXXWS3XKd2vc/TRBG9O5ELC17MmWubD2nKhUKZa26Ba2+D3P+4/MNCFwg59oWVeYhkzgN/JDR8deKBoD7Y+ljEjGZ0sosXVTvbc6RHirr2reNy1OXd6pJsQ+gqjk8VWFYmHrwBzW/n+uMPFiRwHB2I7ih8ciHFxIkd/3Omk5tCDV1t+2nNu5sxxpDFNx+huNhVT3/zMDz8usXC3ddaHBj1GHj/As08fwTS7Kt1HBTmyN29vdwAw+/wbwLVOJ3uAD1wi/dUH7Qei66PfyuRj4Ik9is+hglfbkbfR3cnZm7chlUWLdwmprtCohX4HUtlOcQjLYCu+fzGJH2QRKvP3UNz8bWk1qMxjGTOMThZ3kvgLI5AzFfo379UAAAAASUVORK5CYII=";
        FileCreateParams params = new FileCreateParams()
                .setContent(base64);
        thrown.expect(CatalogException.class);
        thrown.expectMessage("path");
        fileManager.create(studyFqn, params, true, ownerToken).first();
    }

    @Test
    public void createWithBase64FileMissingContentTest() throws CatalogException {
        FileCreateParams params = new FileCreateParams()
                .setType(File.Type.FILE)
                .setPath("/files/folder/heart.png");
        thrown.expect(CatalogException.class);
        thrown.expectMessage("content");
        fileManager.create(studyFqn, params, true, ownerToken).first();
    }

    @Test
    public void testUpdateRelatedFiles() throws CatalogException {
        FileUpdateParams updateParams = new FileUpdateParams()
                .setRelatedFiles(Collections.singletonList(new SmallRelatedFileParams(testFile2, FileRelatedFile.Relation.PRODUCED_FROM)));
        fileManager.update(studyFqn, testFile1, updateParams, QueryOptions.empty(), ownerToken);

        File file = fileManager.get(studyFqn, testFile1, QueryOptions.empty(), ownerToken).first();
        assertEquals(1, file.getRelatedFiles().size());
        assertEquals(testFile2, file.getRelatedFiles().get(0).getFile().getPath());
        assertEquals(FileRelatedFile.Relation.PRODUCED_FROM, file.getRelatedFiles().get(0).getRelation());

        Map<String, String> actionMap = new HashMap<>();
        actionMap.put(FileDBAdaptor.QueryParams.RELATED_FILES.key(), ParamUtils.BasicUpdateAction.SET.name());

        updateParams = new FileUpdateParams()
                .setRelatedFiles(Collections.singletonList(new SmallRelatedFileParams(testFile2, FileRelatedFile.Relation.PART_OF_PAIR)));
        fileManager.update(studyFqn, testFile1, updateParams, new QueryOptions(Constants.ACTIONS, actionMap), ownerToken);
        file = fileManager.get(studyFqn, testFile1, QueryOptions.empty(), ownerToken).first();
        assertEquals(1, file.getRelatedFiles().size());
        assertEquals(testFile2, file.getRelatedFiles().get(0).getFile().getPath());
        assertEquals(FileRelatedFile.Relation.PART_OF_PAIR, file.getRelatedFiles().get(0).getRelation());

        actionMap.put(FileDBAdaptor.QueryParams.RELATED_FILES.key(), ParamUtils.BasicUpdateAction.REMOVE.name());
        fileManager.update(studyFqn, testFile1, updateParams, new QueryOptions(Constants.ACTIONS, actionMap), ownerToken);
        file = fileManager.get(studyFqn, testFile1, QueryOptions.empty(), ownerToken).first();
        assertEquals(0, file.getRelatedFiles().size());

        // We add it again
        updateParams = new FileUpdateParams()
                .setRelatedFiles(Collections.singletonList(new SmallRelatedFileParams(testFile2, FileRelatedFile.Relation.PRODUCED_FROM)));
        fileManager.update(studyFqn, testFile1, updateParams, QueryOptions.empty(), ownerToken);

        // And now we will update with an empty list
        updateParams = new FileUpdateParams().setRelatedFiles(Collections.emptyList());
        actionMap.put(FileDBAdaptor.QueryParams.RELATED_FILES.key(), ParamUtils.BasicUpdateAction.SET.name());
        fileManager.update(studyFqn, testFile1, updateParams, new QueryOptions(Constants.ACTIONS, actionMap), ownerToken);
        assertEquals(0, file.getRelatedFiles().size());
    }

    @Test
    public void testLinkVCFandBAMPair() throws CatalogException {
        String vcfFile = getClass().getResource("/biofiles/variant-test-file.vcf.gz").getFile();
        fileManager.link(studyFqn, new FileLinkParams(vcfFile, "", "", "", null, null, null, null, null), false, ownerToken);

        String bamFile = getClass().getResource("/biofiles/NA19600.chrom20.small.bam").getFile();
        fileManager.link(studyFqn, new FileLinkParams(bamFile, "", "", "", null, null, null, null, null), false, ownerToken);

        Sample sample = catalogManager.getSampleManager().get(studyFqn, "NA19600",
                new QueryOptions(QueryOptions.INCLUDE, SampleDBAdaptor.QueryParams.FILE_IDS.key()), ownerToken).first();
        assertEquals(2, sample.getFileIds().size());
        assertTrue(Arrays.asList("variant-test-file.vcf.gz", "NA19600.chrom20.small.bam").containsAll(sample.getFileIds()));
    }

    @Test
    public void testLinkVirtualWithDifferentSamples() throws CatalogException {
        String vcfFile = getClass().getResource("/biofiles/variant-test-file.vcf.gz").getFile();
        fileManager.link(studyFqn, new FileLinkParams(vcfFile, "", "", "", null, "biofiles/virtual_file.vcf", null, null, null), false, ownerToken);

        String bamFile = getClass().getResource("/biofiles/NA19600.chrom20.small.bam").getFile();
        thrown.expect(CatalogException.class);
        thrown.expectMessage("samples differ");
        fileManager.link(studyFqn, new FileLinkParams(bamFile, "", "", "", null, "biofiles/virtual_file.vcf", null, null, null), false, ownerToken);
    }

    @Test
    public void testLinkVirtualExcludeType() throws CatalogException {
        String vcfFile = getClass().getResource("/biofiles/variant-test-file.vcf.gz").getFile();
        fileManager.link(studyFqn, new FileLinkParams(vcfFile, "", "", "", null, "biofiles/virtual_file.vcf", null, null, null), false, ownerToken);

        OpenCGAResult<File> result;

        result = fileManager.get(studyFqn,
                Arrays.asList("variant-test-file.vcf.gz"),
                new QueryOptions(QueryOptions.INCLUDE, FileDBAdaptor.QueryParams.RELATED_FILES.key()), ownerToken);
        assertEquals(1, result.getNumResults());

        result = fileManager.get(studyFqn,
                Arrays.asList("variant-test-file.vcf.gz"),
                new QueryOptions(QueryOptions.EXCLUDE, "type"), ownerToken);
        assertEquals(1, result.getNumResults());

    }

    @Test
    public void testLinkVirtual() throws CatalogException {
        String vcfFile = getClass().getResource("/biofiles/variant-test-file.vcf.gz").getFile();
        fileManager.link(studyFqn, new FileLinkParams(vcfFile, "", "", "", null, "biofiles/virtual_file.vcf", null, null, null), false, ownerToken);

        String vcfFileCopy = getClass().getResource("/biofiles/variant-test-file-copy.vcf.gz").getFile();
        fileManager.link(studyFqn, new FileLinkParams(vcfFileCopy, "", "", "", null, "biofiles/virtual_file.vcf", null, null, null), false, ownerToken);

        checkTestLinkVirtualFile(false);
    }

    @Test
    public void testLinkVirtualOverSampleLinkThreshold() throws CatalogException {
        String vcfFile = getClass().getResource("/biofiles/variant-test-file.vcf.gz").getFile();
        fileManager.setFileSampleLinkThreshold(organizationId, 2);

        fileManager.link(studyFqn, new FileLinkParams(vcfFile, "", "", "", null, "biofiles/virtual_file.vcf", null, null, null), false, ownerToken);

        String vcfFileCopy = getClass().getResource("/biofiles/variant-test-file-copy.vcf.gz").getFile();
        fileManager.link(studyFqn, new FileLinkParams(vcfFileCopy, "", "", "", null, "biofiles/virtual_file.vcf", null, null, null), false, ownerToken);

        checkTestLinkVirtualFile(true);
    }

    private void checkTestLinkVirtualFile(boolean missingSamples) throws CatalogException {
        OpenCGAResult<File> result = fileManager.get(studyFqn,
                Arrays.asList("variant-test-file.vcf.gz", "variant-test-file-copy.vcf.gz", "virtual_file.vcf"), QueryOptions.empty(), ownerToken);

        assertEquals(3, result.getNumResults());

        assertEquals(0, result.getResults().get(0).getSampleIds().size());
        assertEquals(0, result.getResults().get(1).getSampleIds().size());
        if (missingSamples) {
            assertEquals(FileStatus.MISSING_SAMPLES, result.getResults().get(2).getInternal().getStatus().getId());
            assertEquals(0, result.getResults().get(2).getSampleIds().size());
        } else {
            assertEquals(FileStatus.READY, result.getResults().get(2).getInternal().getStatus().getId());
            assertEquals(4, result.getResults().get(2).getSampleIds().size());
        }

        assertEquals(File.Type.FILE, result.getResults().get(0).getType());
        assertEquals(File.Type.FILE, result.getResults().get(1).getType());
        assertEquals(File.Type.VIRTUAL, result.getResults().get(2).getType());

        assertTrue(FileUtils.isPartial(result.getResults().get(0)));
        assertTrue(FileUtils.isPartial(result.getResults().get(1)));
        assertFalse(FileUtils.isPartial(result.getResults().get(2)));

        assertEquals(1, result.getResults().get(0).getRelatedFiles().size());
        assertEquals(1, result.getResults().get(1).getRelatedFiles().size());
        assertEquals(2, result.getResults().get(2).getRelatedFiles().size());

        assertEquals(FileRelatedFile.Relation.MULTIPART, result.getResults().get(0).getRelatedFiles().get(0).getRelation());
        assertEquals(FileRelatedFile.Relation.MULTIPART, result.getResults().get(1).getRelatedFiles().get(0).getRelation());
        assertEquals(FileRelatedFile.Relation.MULTIPART, result.getResults().get(2).getRelatedFiles().get(0).getRelation());
        assertEquals(FileRelatedFile.Relation.MULTIPART, result.getResults().get(2).getRelatedFiles().get(1).getRelation());

        assertNotNull(result.getResults().get(0).getRelatedFiles().get(0).getFile().getInternal());
        assertNotNull(result.getResults().get(1).getRelatedFiles().get(0).getFile().getInternal());
        assertNull(result.getResults().get(2).getRelatedFiles().get(0).getFile().getInternal());
        assertNull(result.getResults().get(2).getRelatedFiles().get(1).getFile().getInternal());
    }

    @Test
    public void testGetBase64Image() throws CatalogException {
        String qualityImageFile = getClass().getResource("/fastqc-per_base_sequence_quality.png").getFile();
        fileManager.link(studyFqn, new FileLinkParams(qualityImageFile, "", "", "", null, null, null, null, null), false, ownerToken);

        OpenCGAResult<FileContent> result = fileManager.image(studyFqn, "fastqc-per_base_sequence_quality.png", ownerToken);
        assertEquals(1, result.getNumResults());
        assertEquals("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", result.first().getContent());

        thrown.expect(CatalogException.class);
        thrown.expectMessage("not an image");
        fileManager.image(studyFqn, "test_1K.txt.gz", ownerToken);
    }

    @Test
    public void testLinkFolder() throws CatalogException, IOException {
//        // We will link the same folders that are already created in this study into another folder
        URI uri = createExternalDummyData().toUri();
//        long folderId = catalogManager.searchFile(studyUid, new Query(FileDBAdaptor.QueryParams.PATH.key(), "data/"), null,
//                sessionIdUser).first().getId();
//        int numFiles = catalogManager.getAllFilesInFolder(folderId, null, sessionIdUser).getNumResults();
//
//        catalogManager.link(uri, "data/", studyFqn, new ObjectMap(), sessionIdUser);
//        int numFilesAfterLink = catalogManager.getAllFilesInFolder(folderId, null, sessionIdUser).getNumResults();
//        assertEquals("Linking the same folders should not change the number of files in catalog", numFiles, numFilesAfterLink);

        // Now we try to create it into a folder that does not exist with parents = true
        link(uri, "myDirectory", studyFqn, new ObjectMap("parents", true), ownerToken);
        DataResult<File> folderDataResult = fileManager.search(studyFqn, new Query()
                .append(FileDBAdaptor.QueryParams.PATH.key(), "myDirectory/"), null, ownerToken);
        assertEquals(1, folderDataResult.getNumResults());
        assertTrue(!folderDataResult.first().isExternal());

        folderDataResult = fileManager.search(studyFqn, new Query(FileDBAdaptor.QueryParams.PATH.key(),
                "myDirectory/A/"), null, ownerToken);
        assertEquals(1, folderDataResult.getNumResults());
        assertTrue(folderDataResult.first().isExternal());

        folderDataResult = fileManager.search(studyFqn, new Query(FileDBAdaptor.QueryParams.PATH.key(),
                "myDirectory/A/C/D/"), null, ownerToken);
        assertEquals(1, folderDataResult.getNumResults());
        assertTrue(folderDataResult.first().isExternal());
        folderDataResult = fileManager.search(studyFqn, new Query(FileDBAdaptor.QueryParams.PATH.key(),
                "myDirectory/A/B/"), null, ownerToken);
        assertEquals(1, folderDataResult.getNumResults());
        assertTrue(folderDataResult.first().isExternal());

        // Now we try to create it into a folder that does not exist with parents = false
        thrown.expect(CatalogException.class);
        thrown.expectMessage("already linked");
        link(uri, "myDirectory2", studyFqn, new ObjectMap(), ownerToken);
    }

    @Test
    public void testLinkFolder2() throws CatalogException, IOException {
        // We will link the same folders that are already created in this study into another folder
        URI uri = Paths.get(getStudyURI()).resolve("data").toUri();

        // Now we try to create it into a folder that does not exist with parents = false
        thrown.expect(CatalogException.class);
        thrown.expectMessage("not exist");
        link(uri, "myDirectory2", studyFqn, new ObjectMap(), ownerToken);
    }


    @Test
    public void testLinkFolder3() throws CatalogException, IOException {
        URI uri = Paths.get(getStudyURI()).resolve("data").toUri();
        thrown.expect(CatalogException.class);
        thrown.expectMessage("already existed and is not external");
        link(uri, null, studyFqn, new ObjectMap(), ownerToken);

//        // Make sure that the path of the files linked do not start with /
//        Query query = new Query(FileDBAdaptor.QueryParams.STUDY_UID.key(), studyUid)
//                .append(FileDBAdaptor.QueryParams.EXTERNAL.key(), true);
//        QueryOptions queryOptions = new QueryOptions(QueryOptions.INCLUDE, FileDBAdaptor.QueryParams.PATH.key());
//        DataResult<File> fileDataResult = fileManager.get(query, queryOptions, sessionIdUser);
//        assertEquals(5, fileDataResult.getNumResults());
//        for (File file : fileDataResult.getResults()) {
//            assertTrue(!file.getPath().startsWith("/"));
//        }
    }

    // This test will make sure that we can link several times the same uri into the same path with same results and without crashing
    // However, if we try to link to a different path, we will fail
    @Test
    public void testLinkFolder4() throws CatalogException, IOException {
        URI uri = Paths.get(getStudyURI()).resolve("data").toUri();
        ObjectMap params = new ObjectMap("parents", true);
        DataResult<File> allFiles = link(uri, "test/myLinkedFolder/", studyFqn, params, ownerToken);
        assertEquals(11, allFiles.getNumResults());

        DataResult<File> sameAllFiles = link(uri, "test/myLinkedFolder/", studyFqn, params, ownerToken);
        assertEquals(allFiles.getNumResults(), sameAllFiles.getNumResults());

        List<File> result = allFiles.getResults();
        for (int i = 0; i < result.size(); i++) {
            assertEquals(allFiles.getResults().get(i).getUid(), sameAllFiles.getResults().get(i).getUid());
            assertEquals(allFiles.getResults().get(i).getPath(), sameAllFiles.getResults().get(i).getPath());
            assertEquals(allFiles.getResults().get(i).getUri(), sameAllFiles.getResults().get(i).getUri());
        }

        thrown.expect(CatalogException.class);
        thrown.expectMessage("already linked");
        link(uri, "data", studyFqn, new ObjectMap(), ownerToken);
    }

    @Test
    public void testLinkNormalizedUris() throws CatalogException, IOException, URISyntaxException {
        Path path = createExternalDummyData();
        URI uri = UriUtils.createUri(path.toString() + "/../A");
        ObjectMap params = new ObjectMap("parents", true);
        DataResult<File> allFiles = link(uri, "test/myLinkedFolder/", studyFqn, params, ownerToken);
        assertEquals(6, allFiles.getNumResults());
        for (File file : allFiles.getResults()) {
            assertTrue(file.getUri().isAbsolute());
            assertEquals(file.getUri().normalize(), file.getUri());
        }
    }

    private Path createExternalDummyData() throws CatalogIOException, IOException {
        Path jUnitDir = Paths.get(catalogManager.getConfiguration().getWorkspace()).getParent();

        IOManager ioManager = catalogManager.getIoManagerFactory().getDefault();
        ioManager.createDirectory(jUnitDir.resolve("A").resolve("B").toUri(), true);
        ioManager.createDirectory(jUnitDir.resolve("A").resolve("C").resolve("D").toUri(), true);
        ioManager.copy(new ByteArrayInputStream("blablabla".getBytes()), jUnitDir.resolve("A").resolve("C").resolve("file1.txt").toUri());
        ioManager.copy(new ByteArrayInputStream("blablabla".getBytes()),
                jUnitDir.resolve("A").resolve("C").resolve("D").resolve("file3.txt").toUri());

        return jUnitDir.resolve("A");
    }

    @Test
    public void testLinkNonExistentFile() throws CatalogException, IOException {
        URI uri = Paths.get(getStudyURI().resolve("inexistentData")).toUri();
        ObjectMap params = new ObjectMap("parents", true);
        thrown.expect(CatalogException.class);
        thrown.expectMessage("does not exist");
        link(uri, "test/myLinkedFolder/", studyFqn, params, ownerToken);
    }

    // The VCF file that is going to be linked contains names with "." Issue: #570
    @Test
    public void testLinkFile() throws CatalogException, IOException, URISyntaxException {
        URI uri = getClass().getResource("/biofiles/variant-test-file-dot-names.vcf.gz").toURI();
        DataResult<File> link = fileManager.link(studyFqn, uri, ".", new ObjectMap(), ownerToken);

        assertEquals(4, link.first().getSampleIds().size());

        Query query = new Query(SampleDBAdaptor.QueryParams.ID.key(), link.first().getSampleIds());
        DataResult<Sample> sampleDataResult = catalogManager.getSampleManager().search(studyFqn, query, QueryOptions.empty(), ownerToken);

        assertEquals(4, sampleDataResult.getNumResults());
        List<String> sampleNames = sampleDataResult.getResults().stream().map(Sample::getId).collect(Collectors.toList());
        assertTrue(sampleNames.contains("test-name.bam"));
        assertTrue(sampleNames.contains("NA19660"));
        assertTrue(sampleNames.contains("NA19661"));
        assertTrue(sampleNames.contains("NA19685"));
    }

    @Test
    public void testLinkFilePassingNoDirectoryPath() throws CatalogException, URISyntaxException {
        URI uri = getClass().getResource("/biofiles/variant-test-file-dot-names.vcf.gz").toURI();

        String path = "A/B/C/variant-test-file-dot-names.vcf.gz";
        // Instead of choosing a directory path, we will set a file equivalent to the file name (not how OpenCGA asks for the path)
        OpenCGAResult<File> link = fileManager.link(studyFqn, new FileLinkParams().setUri(uri.toString()).setPath(path), true, ownerToken);
        assertEquals(path, link.first().getPath());
        assertEquals(File.Type.FILE, link.first().getType());
    }

    @Test
    public void changeNameTest() throws CatalogException {
        // Link VCF file. This VCF file will automatically create sample NA19600
        String vcfFile = getClass().getResource("/biofiles/variant-test-file.vcf.gz").getFile();
        catalogManager.getFileManager().link(studyFqn, new FileLinkParams(vcfFile, "data/", "", "", null, null, null, null, null), true, token);

        Query query = new Query(SampleDBAdaptor.QueryParams.FILE_IDS.key(), "variant-test-file.vcf.gz");
        QueryOptions options = new QueryOptions(QueryOptions.INCLUDE, SampleDBAdaptor.QueryParams.FILE_IDS.key());
        OpenCGAResult<Sample> sampleResult = catalogManager.getSampleManager().search(studyFqn, query, options, token);
        assertEquals(4, sampleResult.getNumResults());
        for (Sample sample : sampleResult.getResults()) {
            assertEquals(1, sample.getFileIds().size());
            assertEquals("data:variant-test-file.vcf.gz", sample.getFileIds().get(0));
            assertEquals(1, sample.getVersion());
        }

        // Rename file
        FileUpdateParams updateParams = new FileUpdateParams().setName("variant_test.vcf.gz");
        catalogManager.getFileManager().update(studyFqn, "variant-test-file.vcf.gz", updateParams, null, token);

        assertThrows("not found", CatalogException.class, () -> catalogManager.getFileManager().get(studyFqn, "variant-test-file.vcf.gz", null, token));
        File file = catalogManager.getFileManager().get(studyFqn, updateParams.getName(), FileManager.INCLUDE_FILE_URI_PATH, token).first();
        assertEquals("data:" + updateParams.getName(), file.getId());
        assertEquals("data/" + updateParams.getName(), file.getPath());
        assertEquals(updateParams.getName(), file.getName());

        sampleResult = catalogManager.getSampleManager().search(studyFqn, query, options, token);
        assertEquals(0, sampleResult.getNumResults());

        query = new Query(SampleDBAdaptor.QueryParams.FILE_IDS.key(), updateParams.getName());
        sampleResult = catalogManager.getSampleManager().search(studyFqn, query, options, token);
        assertEquals(4, sampleResult.getNumResults());
        for (Sample sample : sampleResult.getResults()) {
            assertEquals(1, sample.getFileIds().size());
            assertEquals("data:" + updateParams.getName(), sample.getFileIds().get(0));
            assertEquals(2, sample.getVersion());
        }
    }

    @Test
    public void testAssociateSamples() throws CatalogException, URISyntaxException {
        URI uri = getClass().getResource("/biofiles/variant-test-file-dot-names.vcf.gz").toURI();
        DataResult<File> link = fileManager.link(studyFqn, uri, ".", new ObjectMap(), ownerToken);
        assertEquals(4, link.first().getSampleIds().size());
        assertThat(catalogManager.getSampleManager().get(studyFqn, "test-name.bam", QueryOptions.empty(), ownerToken).first().getFileIds(),
                hasItem(link.first().getId()));
        assertThat(catalogManager.getSampleManager().get(studyFqn, "NA19660", QueryOptions.empty(), ownerToken).first().getFileIds(),
                hasItem(link.first().getId()));
        assertThat(catalogManager.getSampleManager().get(studyFqn, "NA19661", QueryOptions.empty(), ownerToken).first().getFileIds(),
                hasItem(link.first().getId()));
        assertThat(catalogManager.getSampleManager().get(studyFqn, "NA19685", QueryOptions.empty(), ownerToken).first().getFileIds(),
                hasItem(link.first().getId()));

        Map<String, Object> actionMap = new HashMap<>();
        actionMap.put(FileDBAdaptor.QueryParams.SAMPLE_IDS.key(), ParamUtils.BasicUpdateAction.SET.name());
        fileManager.update(studyFqn, link.first().getId(), new FileUpdateParams().setSampleIds(Collections.emptyList()),
                new QueryOptions(Constants.ACTIONS, actionMap), ownerToken);
        assertThat(catalogManager.getSampleManager().get(studyFqn, "test-name.bam", QueryOptions.empty(), ownerToken).first().getFileIds(),
                not(hasItem(link.first().getId())));
        assertThat(catalogManager.getSampleManager().get(studyFqn, "NA19660", QueryOptions.empty(), ownerToken).first().getFileIds(),
                not(hasItem(link.first().getId())));
        assertThat(catalogManager.getSampleManager().get(studyFqn, "NA19661", QueryOptions.empty(), ownerToken).first().getFileIds(),
                not(hasItem(link.first().getId())));
        assertThat(catalogManager.getSampleManager().get(studyFqn, "NA19685", QueryOptions.empty(), ownerToken).first().getFileIds(),
                not(hasItem(link.first().getId())));

        File file = fileManager.get(studyFqn, link.first().getId(), QueryOptions.empty(), ownerToken).first();
        assertEquals(0, file.getSampleIds().size());

        actionMap.put(FileDBAdaptor.QueryParams.SAMPLE_IDS.key(), ParamUtils.BasicUpdateAction.ADD.name());
        fileManager.update(studyFqn, link.first().getId(), new FileUpdateParams().setSampleIds(Arrays.asList("NA19660", "NA19661")),
                new QueryOptions(Constants.ACTIONS, actionMap), ownerToken);

        file = fileManager.get(studyFqn, link.first().getId(), QueryOptions.empty(), ownerToken).first();
        assertEquals(2, file.getSampleIds().size());
        assertThat(catalogManager.getSampleManager().get(studyFqn, "NA19661", QueryOptions.empty(), ownerToken).first().getFileIds(),
                hasItem(file.getId()));
        assertThat(catalogManager.getSampleManager().get(studyFqn, "NA19660", QueryOptions.empty(), ownerToken).first().getFileIds(),
                hasItem(file.getId()));

        actionMap.put(FileDBAdaptor.QueryParams.SAMPLE_IDS.key(), ParamUtils.BasicUpdateAction.REMOVE.name());
        fileManager.update(studyFqn, link.first().getId(), new FileUpdateParams().setSampleIds(Arrays.asList("NA19661")),
                new QueryOptions(Constants.ACTIONS, actionMap), ownerToken);

        file = fileManager.get(studyFqn, link.first().getId(), QueryOptions.empty(), ownerToken).first();
        assertEquals(1, file.getSampleIds().size());
        assertEquals("NA19660", file.getSampleIds().get(0));
        assertThat(catalogManager.getSampleManager().get(studyFqn, "NA19660", QueryOptions.empty(), ownerToken).first().getFileIds(),
                hasItem(file.getId()));
        assertThat(catalogManager.getSampleManager().get(studyFqn, "NA19661", QueryOptions.empty(), ownerToken).first().getFileIds(),
                not(hasItem(file.getId())));

        actionMap.put(FileDBAdaptor.QueryParams.SAMPLE_IDS.key(), ParamUtils.BasicUpdateAction.SET);
        fileManager.update(studyFqn, link.first().getId(), new FileUpdateParams().setSampleIds(Arrays.asList("NA19661")),
                new QueryOptions(Constants.ACTIONS, actionMap), ownerToken);
        file = fileManager.get(studyFqn, link.first().getId(), QueryOptions.empty(), ownerToken).first();
        assertEquals(1, file.getSampleIds().size());
        assertEquals("NA19661", file.getSampleIds().get(0));
        assertThat(catalogManager.getSampleManager().get(studyFqn, "NA19660", QueryOptions.empty(), ownerToken).first().getFileIds(),
                not(hasItem(file.getId())));
        assertThat(catalogManager.getSampleManager().get(studyFqn, "NA19661", QueryOptions.empty(), ownerToken).first().getFileIds(),
                hasItem(file.getId()));

        file = fileManager.get(studyFqn, link.first().getId(),
                new QueryOptions(QueryOptions.INCLUDE, FileDBAdaptor.QueryParams.SAMPLE_IDS.key()), ownerToken).first();
        assertEquals(1, file.getSampleIds().size());
        assertEquals("NA19661", file.getSampleIds().get(0));
        assertNull(file.getCreationDate());

        file = fileManager.get(studyFqn, link.first().getId(),
                new QueryOptions(QueryOptions.EXCLUDE, FileDBAdaptor.QueryParams.SAMPLE_IDS.key()), ownerToken).first();
        assertTrue(file.getSampleIds().isEmpty());
        assertNotNull(file.getCreationDate());
    }

    @Test
    public void testLinkFileWithDifferentSampleNames() throws CatalogException, URISyntaxException {
        URI uri = getClass().getResource("/biofiles/variant-test-file-dot-names.vcf.gz").toURI();

        Map<String, String> sampleIdNames = new HashMap<>();
        sampleIdNames.put("test-name.bam", "sample1");
        sampleIdNames.put("NA19660", "sample2");
        sampleIdNames.put("NA19661", "sample3");
        sampleIdNames.put("NA19685", "sample4");

        FileLinkParams params = new FileLinkParams()
                .setUri(uri.toString())
                .setInternal(new FileLinkInternalParams(sampleIdNames));
        DataResult<File> link = fileManager.link(studyFqn, params, false, ownerToken);

        assertEquals(4, link.first().getSampleIds().size());

        Query query = new Query(SampleDBAdaptor.QueryParams.ID.key(), link.first().getSampleIds());
        DataResult<Sample> sampleDataResult = catalogManager.getSampleManager().search(studyFqn, query, QueryOptions.empty(), ownerToken);

        assertEquals(4, sampleDataResult.getNumResults());
        List<String> sampleNames = sampleDataResult.getResults().stream().map(Sample::getId).collect(Collectors.toList());
        assertTrue(sampleNames.contains("sample1"));
        assertTrue(sampleNames.contains("sample2"));
        assertTrue(sampleNames.contains("sample3"));
        assertTrue(sampleNames.contains("sample4"));

        assertEquals(sampleIdNames, link.first().getInternal().getSampleMap());
    }

    @Test
    public void testLinkFileWithDifferentSampleNamesFromVCFHeader() throws CatalogException, URISyntaxException {
        URI uri = getClass().getResource("/biofiles/variant-test-sample-mapping.vcf").toURI();


        FileLinkParams params = new FileLinkParams()
                .setUri(uri.toString());
        DataResult<File> link = fileManager.link(studyFqn, params, false, ownerToken);

        assertEquals(3, link.first().getSampleIds().size());
        assertEquals(Arrays.asList("sample_tumor", "sample_normal", "sample_other"), link.first().getSampleIds());
        assertEquals(Arrays.asList("TUMOR", "NORMAL", "OTHER"), new ObjectMap(link.first().getAttributes()).getAsStringList("variantFileMetadata.attributes.originalSamples"));

        Query query = new Query(SampleDBAdaptor.QueryParams.ID.key(), link.first().getSampleIds());
        DataResult<Sample> sampleDataResult = catalogManager.getSampleManager().search(studyFqn, query, QueryOptions.empty(), ownerToken);

        assertEquals(3, sampleDataResult.getNumResults());
        List<String> sampleNames = sampleDataResult.getResults().stream().map(Sample::getId).collect(Collectors.toList());
        assertTrue(sampleNames.contains("sample_tumor"));
        assertTrue(sampleNames.contains("sample_normal"));
        assertTrue(sampleNames.contains("sample_other"));

    }

    @Test
    public void testFileHooks() throws CatalogException, URISyntaxException {
        URI uri = getClass().getResource("/biofiles/variant-test-file-dot-names.vcf.gz").toURI();
        DataResult<File> link = fileManager.link(studyFqn, uri, ".", new ObjectMap(), ownerToken);

        assertEquals(2, link.first().getTags().size());
        assertTrue(link.first().getTags().containsAll(Arrays.asList("VCF", "FILE")));
    }

    @Test
    public void stressTestLinkFile() throws Exception {
        URI uri = getClass().getResource("/biofiles/variant-test-file.vcf.gz").toURI();
        AtomicInteger numFailures = new AtomicInteger();
        AtomicInteger numOk = new AtomicInteger();
        int numThreads = 10;
        int numOperations = 250;

        ExecutorService executorService = Executors.newFixedThreadPool(numThreads);

        for (int i = 0; i < numOperations; i++) {
            executorService.submit(() -> {
                try {
                    fileManager.link(studyFqn, new FileLinkParams().setUri(uri.getPath()).setPath("."), false, ownerToken);
                    numOk.incrementAndGet();
                } catch (Exception ignore) {
                    ignore.printStackTrace();
                    numFailures.incrementAndGet();
                }
            });

        }
        executorService.awaitTermination(5, TimeUnit.SECONDS);
        executorService.shutdown();

        int unexecuted = executorService.shutdownNow().size();
        System.out.println("Number of tasks non-executed " + unexecuted);
        System.out.println("numFailures = " + numFailures);
        System.out.println("numOk.get() = " + numOk.get());

        assertEquals(numOperations, numOk.get());
    }

    @Test
    public void testUnlinkFolder() throws CatalogException, IOException {
        URI uri = createExternalDummyData().toUri();
        link(uri, "myDirectory", studyFqn, new ObjectMap("parents", true), ownerToken);

        IOManager ioManager = catalogManager.getIoManagerFactory().get(uri);

        Query query = new Query()
                .append(FileDBAdaptor.QueryParams.STUDY_UID.key(), studyUid)
                .append(FileDBAdaptor.QueryParams.PATH.key(), "~myDirectory/*")
                .append(FileDBAdaptor.QueryParams.INTERNAL_STATUS_ID.key(), FileStatus.READY);
        DataResult<File> fileDataResultLinked = fileManager.search(studyFqn, query, null, ownerToken);

        System.out.println("Number of files/folders linked = " + fileDataResultLinked.getNumResults());

        // We set to PENDING DELETE the subdirectory that will be unlinked
        Query updateQuery = new Query()
                .append(FileDBAdaptor.QueryParams.STUDY_UID.key(), studyUid)
                .append(FileDBAdaptor.QueryParams.PATH.key(), "~myDirectory/A/*")
                .append(FileDBAdaptor.QueryParams.INTERNAL_STATUS_ID.key(), FileStatus.READY);
        setToPendingDelete(studyFqn, updateQuery);

        // Now we try to unlink them
        fileManager.unlink(studyFqn, "myDirectory/A/", ownerToken);
        fileDataResultLinked = fileManager.search(studyFqn, query, null, ownerToken);
        assertEquals(1, fileDataResultLinked.getNumResults());

        query = new Query()
                .append(FileDBAdaptor.QueryParams.STUDY_UID.key(), studyUid)
                .append(FileDBAdaptor.QueryParams.PATH.key(), "~myDirectory/*")
                .append(FileDBAdaptor.QueryParams.DELETED.key(), true);
        DataResult<File> fileDataResultUnlinked = fileManager.search(studyFqn, query, null, ownerToken);
        assertEquals(6, fileDataResultUnlinked.getNumResults());

        for (File file : fileDataResultUnlinked.getResults()) {
            assertEquals("Status should be to REMOVED", FileStatus.REMOVED, file.getInternal().getStatus().getId());
            assertEquals("Name should not have changed", file.getName(), file.getName());
            assertTrue("File uri: " + file.getUri() + " should exist", ioManager.exists(file.getUri()));
        }
    }

    @Test
    public void testUnlinkFile() throws CatalogException, IOException {
        URI uri = Paths.get(getStudyURI()).resolve("data").toUri();
        link(uri, "myDirectory", studyFqn, new ObjectMap("parents", true), ownerToken);

        Query query = new Query()
                .append(FileDBAdaptor.QueryParams.PATH.key(), "~myDirectory/*")
                .append(FileDBAdaptor.QueryParams.INTERNAL_STATUS_ID.key(), FileStatus.READY);
        DataResult<File> fileDataResultLinked = fileManager.search(studyFqn, query, null, ownerToken);

        int numberLinkedFiles = fileDataResultLinked.getNumResults();
        System.out.println("Number of files/folders linked = " + numberLinkedFiles);

        Query unlinkQuery = new Query(FileDBAdaptor.QueryParams.PATH.key(), "myDirectory/data/test/folder/test_0.5K.txt");

        setToPendingDelete(studyFqn, unlinkQuery);

        // Now we try to unlink the file
        fileManager.unlink(studyFqn, "myDirectory/data/test/folder/test_0.5K.txt", ownerToken);
        fileDataResultLinked = fileManager.search(studyFqn, unlinkQuery, QueryOptions.empty(), ownerToken);
        assertEquals(0, fileDataResultLinked.getNumResults());

        unlinkQuery.put(FileDBAdaptor.QueryParams.DELETED.key(), true);
        fileDataResultLinked = fileManager.search(studyFqn, unlinkQuery, QueryOptions.empty(), ownerToken);
        assertEquals(1, fileDataResultLinked.getNumResults());
        assertEquals(FileStatus.REMOVED, fileDataResultLinked.first().getInternal().getStatus().getId());

        // Check the other root linked files/folders have not been touched
        fileDataResultLinked = fileManager.search(studyFqn, query, QueryOptions.empty(), ownerToken);
        assertEquals(numberLinkedFiles - 1, fileDataResultLinked.getNumResults());

        // We send the unlink command again
        thrown.expect(CatalogException.class);
        thrown.expectMessage("not found");
        fileManager.unlink(studyFqn, "myDirectory/data/test/folder/test_0.5K.txt", ownerToken);
    }

    @Test
    public void testCreateFile() throws CatalogException, IOException {
        String content = "This is the content\tof the file";
        try {
            fileManager.create(studyFqn3,
                    new FileCreateParams()
                            .setType(File.Type.FILE)
                            .setPath("data/test/myTest/myFile.txt")
                            .setContent("This is the content\tof the file"),
                    false, orgAdminToken1);
            fail("An error should be raised because parents is false");
        } catch (CatalogException e) {
            System.out.println("Correct");
        }

        DataResult<File> fileDataResult = fileManager.create(studyFqn3,
                new FileCreateParams()
                        .setType(File.Type.FILE)
                        .setPath("data/test/myTest/myFile.txt")
                        .setContent(content),
                true, orgAdminToken1);
        IOManager ioManager = catalogManager.getIoManagerFactory().get(fileDataResult.first().getUri());
        assertTrue(ioManager.exists(fileDataResult.first().getUri()));

        DataInputStream fileObject = ioManager.getFileObject(fileDataResult.first().getUri(), -1, -1);
        assertEquals(content, fileObject.readLine());
    }

    @Test
    public void testCreateFolder() throws Exception {
        Set<String> paths = fileManager.search(studyFqn, new Query("type", File.Type.DIRECTORY), new QueryOptions(), orgAdminToken1)
                .getResults().stream().map(File::getPath).collect(Collectors.toSet());
        assertEquals(9, paths.size());
        assertTrue(paths.containsAll(Arrays.asList("", "JOBS/", "data/", "data/test/", "data/test/folder/", "data/d1/", "data/d1/d2/",
                "data/d1/d2/d3/", "data/d1/d2/d3/d4/")));

        Path folderPath = Paths.get("data", "new", "folder");
        File folder = fileManager.createFolder(studyFqn, folderPath.toString(), true, null, QueryOptions.empty(), orgAdminToken1).first();
        System.out.println(folder);
        IOManager ioManager = catalogManager.getIoManagerFactory().get(folder.getUri());
        assertTrue(!ioManager.exists(folder.getUri()));

        paths = fileManager.search(studyFqn, new Query(FileDBAdaptor.QueryParams.TYPE.key(), File.Type.DIRECTORY), new QueryOptions(),
                orgAdminToken1).getResults().stream().map(File::getPath).collect(Collectors.toSet());
        assertEquals(11, paths.size());
        assertTrue(paths.containsAll(Arrays.asList("", "JOBS/", "data/", "data/test/", "data/test/folder/", "data/d1/", "data/d1/d2/",
                "data/d1/d2/d3/", "data/d1/d2/d3/d4/", "data/new/", "data/new/folder/")));

        URI uri = fileManager.getUri(organizationId, folder);
        assertTrue(!catalogManager.getIoManagerFactory().get(uri).exists(uri));

        fileManager.createFolder(studyFqn, Paths.get("WOLOLO").toString(), true, null, QueryOptions.empty(), orgAdminToken1);

        String newStudy = catalogManager.getStudyManager().create(project2, "alias", null, "name", "", null, null, null, null, null, orgAdminToken1).first().getFqn();

        folder = fileManager.createFolder(newStudy, Paths.get("WOLOLO").toString(), true, null,
                QueryOptions.empty(), orgAdminToken1).first();
        assertTrue(!ioManager.exists(folder.getUri()));
    }

    @Test
    public void testCreateFolderAlreadyExists() throws Exception {
        Set<String> paths = fileManager.search(studyFqn3, new Query("type", File.Type.DIRECTORY), new QueryOptions(), orgAdminToken1).getResults().stream().map(File::getPath).collect(Collectors.toSet());
        assertEquals(2, paths.size());
        assertTrue(paths.contains(""));             //root
//        assertTrue(paths.contains("data/"));        //data
//        assertTrue(paths.contains("analysis/"));    //analysis

        Path folderPath = Paths.get("data", "new", "folder");
        File folder = fileManager.createFolder(studyFqn3, folderPath.toString(), true, null, null, orgAdminToken1).first();

        assertNotNull(folder);
        assertTrue(folder.getPath().contains(folderPath.toString()));

        // When creating the same folder, we should not complain and return it directly
        File sameFolder = fileManager.createFolder(studyFqn3, folderPath.toString(), true, null, null, orgAdminToken1).first();
        assertNotNull(sameFolder);
        assertEquals(folder.getPath(), sameFolder.getPath());
        assertEquals(folder.getUid(), sameFolder.getUid());

        // However, a user without create permissions will receive an exception
        thrown.expect(CatalogAuthorizationException.class);
        fileManager.createFolder(studyFqn3, folderPath.toString(), true, null, null, normalToken1);
    }

    @Test
    public void testAnnotationWrongEntity() throws CatalogException, JsonProcessingException {
        List<Variable> variables = new ArrayList<>();
        variables.add(new Variable("var_name", "", "", Variable.VariableType.STRING, "", true, false, Collections.emptyList(), null, 0, "", "",
                null, Collections.emptyMap()));
        variables.add(new Variable("AGE", "", "", Variable.VariableType.INTEGER, "", false, false, Collections.emptyList(), null, 0, "", "",
                null, Collections.emptyMap()));
        variables.add(new Variable("HEIGHT", "", "", Variable.VariableType.DOUBLE, "", false, false, Collections.emptyList(), null, 0, "",
                "", null, Collections.emptyMap()));
        VariableSet vs1 = catalogManager.getStudyManager().createVariableSet(studyFqn, "vs1", "vs1", false, false, "", null, variables,
                Collections.singletonList(VariableSet.AnnotableDataModels.SAMPLE), ownerToken).first();

        ObjectMap annotations = new ObjectMap()
                .append("var_name", "Joe")
                .append("AGE", 25)
                .append("HEIGHT", 180);
        AnnotationSet annotationSet = new AnnotationSet("annotation1", vs1.getId(), annotations);

        FileUpdateParams updateParams = new FileUpdateParams().setAnnotationSets(Collections.singletonList(annotationSet));

        thrown.expect(CatalogException.class);
        thrown.expectMessage("intended only for");
        fileManager.update(studyFqn, "data/", updateParams, QueryOptions.empty(), ownerToken);
    }

    @Test
    public void testAnnotationForAnyEntity() throws CatalogException, JsonProcessingException {
        List<Variable> variables = new ArrayList<>();
        variables.add(new Variable("var_name", "", "", Variable.VariableType.STRING, "", true, false, Collections.emptyList(), null, 0, "", "",
                null, Collections.emptyMap()));
        variables.add(new Variable("AGE", "", "", Variable.VariableType.INTEGER, "", false, false, Collections.emptyList(), null, 0, "", "",
                null, Collections.emptyMap()));
        variables.add(new Variable("HEIGHT", "", "", Variable.VariableType.DOUBLE, "", false, false, Collections.emptyList(), null, 0, "",
                "", null, Collections.emptyMap()));
        VariableSet vs1 = catalogManager.getStudyManager().createVariableSet(studyFqn, "vs1", "vs1", false, false, "", null, variables,
                null, ownerToken).first();

        ObjectMap annotations = new ObjectMap()
                .append("var_name", "Joe")
                .append("AGE", 25)
                .append("HEIGHT", 180);
        AnnotationSet annotationSet = new AnnotationSet("annotation1", vs1.getId(), annotations);

        FileUpdateParams updateParams = new FileUpdateParams().setAnnotationSets(Collections.singletonList(annotationSet));

        DataResult<File> updateResult = fileManager.update(studyFqn, "data/", updateParams, QueryOptions.empty(), ownerToken);
        assertEquals(1, updateResult.getNumUpdated());

        File file = fileManager.get(studyFqn, "data/", QueryOptions.empty(), ownerToken).first();
        assertEquals(1, file.getAnnotationSets().size());
    }

    @Test
    public void testAnnotations() throws CatalogException, JsonProcessingException {
        List<Variable> variables = new ArrayList<>();
        variables.add(new Variable("var_name", "", "", Variable.VariableType.STRING, "", true, false, Collections.emptyList(), null, 0, "", "",
                null, Collections.emptyMap()));
        variables.add(new Variable("AGE", "", "", Variable.VariableType.INTEGER, "", false, false, Collections.emptyList(), null, 0, "", "",
                null, Collections.emptyMap()));
        variables.add(new Variable("HEIGHT", "", "", Variable.VariableType.DOUBLE, "", false, false, Collections.emptyList(), null, 0, "",
                "", null, Collections.emptyMap()));
        VariableSet vs1 = catalogManager.getStudyManager().createVariableSet(studyFqn, "vs1", "vs1", false, false, "", null, variables,
                Collections.singletonList(VariableSet.AnnotableDataModels.FILE), ownerToken).first();

        ObjectMap annotations = new ObjectMap()
                .append("var_name", "Joe")
                .append("AGE", 25)
                .append("HEIGHT", 180);
        AnnotationSet annotationSet = new AnnotationSet("annotation1", vs1.getId(), annotations);
        AnnotationSet annotationSet1 = new AnnotationSet("annotation2", vs1.getId(), annotations);

        FileUpdateParams updateParams = new FileUpdateParams().setAnnotationSets(Arrays.asList(annotationSet, annotationSet1));
        DataResult<File> updateResult = fileManager.update(studyFqn, "data/", updateParams, QueryOptions.empty(), ownerToken);
        assertEquals(1, updateResult.getNumUpdated());

        File file = fileManager.get(studyFqn, "data/", QueryOptions.empty(), ownerToken).first();
        assertEquals(2, file.getAnnotationSets().size());
    }

    @Test
    public void testUpdateSamples() throws CatalogException {
        // Update the same sample twice to the file
        Sample sample1 = catalogManager.getSampleManager().get(studyFqn, "s_1", QueryOptions.empty(), ownerToken).first();
        Sample sample2 = catalogManager.getSampleManager().get(studyFqn, "s_2", QueryOptions.empty(), ownerToken).first();
        assertFalse(sample1.getFileIds().contains("data:test:folder:test_1K.txt.gz"));
        assertFalse(sample2.getFileIds().contains("data:test:folder:test_1K.txt.gz"));

        FileUpdateParams updateParams = new FileUpdateParams().setSampleIds(Arrays.asList("s_1", "s_1", "s_2", "s_1"));
        DataResult<File> updateResult = fileManager.update(studyFqn, "test_1K.txt.gz", updateParams, null, ownerToken);
        assertEquals(1, updateResult.getNumUpdated());

        File file = fileManager.get(studyFqn, "test_1K.txt.gz", QueryOptions.empty(), ownerToken).first();
        assertEquals(2, file.getSampleIds().size());
        assertTrue(file.getSampleIds().containsAll(Arrays.asList("s_1", "s_2")));

        OpenCGAResult<Sample> sampleResult = catalogManager.getSampleManager().get(studyFqn, Arrays.asList("s_1", "s_2"), QueryOptions.empty(), ownerToken);
        assertEquals(2, sampleResult.getNumResults());
        for (Sample sample : sampleResult.getResults()) {
            assertTrue(sample.getFileIds().contains(file.getId()));
        }

        System.out.println(file.getId());
        sample1 = catalogManager.getSampleManager().get(studyFqn, "s_1", QueryOptions.empty(), ownerToken).first();
        sample2 = catalogManager.getSampleManager().get(studyFqn, "s_2", QueryOptions.empty(), ownerToken).first();
        assertTrue(sample1.getFileIds().contains(file.getId()));
        assertTrue(sample2.getFileIds().contains(file.getId()));
    }

    @Test
    public void testCreate() throws Exception {
        String fileName = "item." + TimeUtils.getTimeMillis() + ".vcf";
        DataResult<File> fileResult = fileManager.create(studyFqn,
                new FileCreateParams()
                        .setType(File.Type.FILE)
                        .setFormat(File.Format.PLAIN)
                        .setBioformat(File.Bioformat.VARIANT)
                        .setPath("data/" + fileName)
                        .setDescription("description")
                        .setContent(MongoBackupUtils.getDummyVCFContent()),
                true, ownerToken);
        assertEquals(3, fileResult.first().getSampleIds().size());

        fileName = "item." + TimeUtils.getTimeMillis() + ".vcf";
        fileManager.create(studyFqn,
                new FileCreateParams()
                        .setType(File.Type.FILE)
                        .setFormat(File.Format.PLAIN)
                        .setBioformat(File.Bioformat.VARIANT)
                        .setPath("data/" + fileName)
                        .setDescription("description")
                        .setContent(MongoBackupUtils.getDummyVCFContent()),
                true, ownerToken);

        fileName = "item." + TimeUtils.getTimeMillis() + ".txt";
        DataResult<File> queryResult = fileManager.create(studyFqn,
                new FileCreateParams()
                        .setContent(RandomStringUtils.randomAlphanumeric(200))
                        .setType(File.Type.FILE)
                        .setPath("data/" + fileName),
                false, ownerToken);
        assertEquals(FileStatus.READY, queryResult.first().getInternal().getStatus().getId());
        assertEquals(200, queryResult.first().getSize());

        fileManager.create(studyFqn,
                new FileCreateParams()
                        .setType(File.Type.FILE)
                        .setFormat(File.Format.PLAIN)
                        .setBioformat(File.Bioformat.NONE)
                        .setPath("data/deletable/folder/item." + TimeUtils.getTimeMillis() + ".txt")
                        .setDescription("description")
                        .setContent(MongoBackupUtils.createRandomString(200)),
                true, ownerToken);

        fileManager.create(studyFqn2,
                new FileCreateParams()
                        .setType(File.Type.FILE)
                        .setFormat(File.Format.PLAIN)
                        .setBioformat(File.Bioformat.NONE)
                        .setPath("data/deletable/item." + TimeUtils.getTimeMillis() + ".txt")
                        .setDescription("description")
                        .setContent(MongoBackupUtils.createRandomString(200)),
                true, ownerToken);

        fileManager.create(studyFqn2,
                new FileCreateParams()
                        .setType(File.Type.FILE)
                        .setFormat(File.Format.PLAIN)
                        .setBioformat(File.Bioformat.NONE)
                        .setPath("item." + TimeUtils.getTimeMillis() + ".txt")
                        .setDescription("file at root")
                        .setContent(MongoBackupUtils.createRandomString(200)),
                true, ownerToken);

        fileName = "item." + TimeUtils.getTimeMillis() + ".txt";
        fileManager.create(studyFqn2,
                new FileCreateParams()
                        .setType(File.Type.FILE)
                        .setFormat(File.Format.PLAIN)
                        .setBioformat(File.Bioformat.NONE)
                        .setPath(fileName)
                        .setDescription("file at root")
                        .setContent(MongoBackupUtils.createRandomString(200)),
                true, ownerToken);

        DataResult<File> fileDataResult = fileManager.get(studyFqn2, fileName, null, ownerToken);
        assertTrue(fileDataResult.first().getSize() > 0);
    }

    @Test
    public void testCreateFileInLinkedFolder() throws Exception {
        // Create an empty folder
        Path dir = catalogManagerResource.getOpencgaHome().resolve("folder_to_link");
        Files.createDirectory(dir);
        URI uri = dir.toUri();

        // Link the folder in the root
        link(uri, "", studyFqn, new ObjectMap(), ownerToken);

        File file = fileManager.create(studyFqn,
                new FileCreateParams()
                        .setContent("bla bla")
                        .setType(File.Type.FILE)
                        .setFormat(File.Format.PLAIN)
                        .setPath("folder_to_link/file.txt"),
                false, ownerToken).first();

        assertEquals(uri.resolve("file.txt"), file.getUri());
    }

    @Test
    public void testDownloadAndHeadFile() throws CatalogException, IOException, InterruptedException {
        String fileName = "item." + TimeUtils.getTimeMillis() + ".vcf";

        File file = fileManager.create(studyFqn,
                new FileCreateParams()
                        .setType(File.Type.FILE)
                        .setFormat(File.Format.PLAIN)
                        .setBioformat(File.Bioformat.VARIANT)
                        .setPath("data/" + fileName)
                        .setDescription("description")
                        .setContent(MongoBackupUtils.getDummyVCFContent()),
                true, ownerToken).first();

        byte[] bytes = new byte[100];
        byte[] bytesOrig = new byte[100];
        DataInputStream fis = new DataInputStream(new FileInputStream(file.getUri().getPath()));
        DataInputStream dis = fileManager.download(studyFqn, file.getPath(), -1, -1, ownerToken);
        fis.read(bytesOrig, 0, 100);
        dis.read(bytes, 0, 100);
        fis.close();
        dis.close();
        assertArrayEquals(bytesOrig, bytes);

        int offset = 1;
        int limit = 10;
        dis = fileManager.download(studyFqn, file.getPath(), offset, limit, ownerToken);
        fis = new DataInputStream(new FileInputStream(file.getUri().getPath()));
        for (int i = 0; i < offset; i++) {
            fis.readLine();
        }


        String line;
        int lines = 0;
        while ((line = dis.readLine()) != null) {
            lines++;
            System.out.println(line);
            assertEquals(fis.readLine(), line);
        }

        assertEquals(limit - offset, lines);

        fis.close();
        dis.close();
    }

    @Test
    public void testDownloadFile() throws CatalogException, IOException, URISyntaxException {
        URI sourceUri = getClass().getResource("/biofiles/variant-test-file.vcf.gz").toURI();
        OpenCGAResult<File> fileResult = fileManager.link(studyFqn, sourceUri, "data/", new ObjectMap("parents", true), ownerToken);

        DataInputStream dis = fileManager.download(studyFqn, fileResult.first().getPath(), -1, -1, ownerToken);

        byte[] bytes = new byte[(int) fileResult.first().getSize()];
        dis.read(bytes, 0, (int) fileResult.first().getSize());
        assertTrue(Arrays.equals(Files.readAllBytes(Paths.get(sourceUri)), bytes));
    }

    public int countElementsInTree(FileTree fileTree) {
        int total = 1;
        for (FileTree child : fileTree.getChildren()) {
            total += countElementsInTree(child);
        }
        return total;
    }

    @Test
    public void testGetTreeView() throws CatalogException {
        fileManager.create(studyFqn, new FileCreateParams()
                .setPath("myFile_a.txt")
                .setType(File.Type.FILE)
                .setContent("content"), false, ownerToken);
        fileManager.create(studyFqn, new FileCreateParams()
                .setPath("myFile_b.txt")
                .setType(File.Type.FILE)
                .setContent("content"), false, ownerToken);
        fileManager.create(studyFqn, new FileCreateParams()
                .setPath("myFile_c.txt")
                .setType(File.Type.FILE)
                .setContent("content"), false, ownerToken);

        fileManager.create(studyFqn, new FileCreateParams()
                .setPath("data/myFile_a.txt")
                .setType(File.Type.FILE)
                .setContent("content"), false, ownerToken);
        fileManager.create(studyFqn, new FileCreateParams()
                .setPath("data/myFile_b.txt")
                .setType(File.Type.FILE)
                .setContent("content"), false, ownerToken);
        fileManager.create(studyFqn, new FileCreateParams()
                .setPath("data/myFile_c.txt")
                .setType(File.Type.FILE)
                .setContent("content"), false, ownerToken);

        fileManager.create(studyFqn, new FileCreateParams()
                .setPath("JOBS/myFile_a.txt")
                .setType(File.Type.FILE)
                .setContent("content"), false, ownerToken);
        fileManager.create(studyFqn, new FileCreateParams()
                .setPath("JOBS/myFile_b.txt")
                .setType(File.Type.FILE)
                .setContent("content"), false, ownerToken);
        fileManager.create(studyFqn, new FileCreateParams()
                .setPath("JOBS/myFile_c.txt")
                .setType(File.Type.FILE)
                .setContent("content"), false, ownerToken);

        fileManager.create(studyFqn, new FileCreateParams()
                .setPath("JOBS/AAAAAA/myFile_a.txt")
                .setType(File.Type.FILE)
                .setContent("content"), true, ownerToken);
        fileManager.create(studyFqn, new FileCreateParams()
                .setPath("JOBS/BBBBBB/myFile_b.txt")
                .setType(File.Type.FILE)
                .setContent("content"), true, ownerToken);
        fileManager.create(studyFqn, new FileCreateParams()
                .setPath("JOBS/CCCCCC/myFile_c.txt")
                .setType(File.Type.FILE)
                .setContent("content"), true, ownerToken);

        DataResult<FileTree> fileTree = fileManager.getTree(studyFqn, "/", 5, new QueryOptions(QueryOptions.INCLUDE, FileDBAdaptor.QueryParams.ID.key()), ownerToken);
        assertEquals(27, fileTree.getNumResults());
        assertEquals(27, countElementsInTree(fileTree.first()));

        fileTree = fileManager.getTree(studyFqn, "/", 2, new QueryOptions(), ownerToken);
        assertEquals(17, fileTree.getNumResults());

        QueryOptions options = new QueryOptions(QueryOptions.INCLUDE, FileDBAdaptor.QueryParams.ID.key());
        fileTree = fileManager.getTree(studyFqn, "/", 2, options, ownerToken);
        assertNotNull(fileTree.first().getFile().getId());
        assertNull(fileTree.first().getFile().getName());

        for (FileTree child : fileTree.first().getChildren()) {
            assertNotNull(child.getFile().getId());
            assertNull(child.getFile().getName());
        }
    }

    @Test
    public void testGetTreeViewMoreThanOneFile() throws CatalogException {

        // Create a new study so more than one file will be found under the root /. However, it should be able to consider the study given
        // properly
        catalogManager.getStudyManager().create(project1, "phase2", null, "Phase 2", "Done", null, null, null, null, null, ownerToken);

        DataResult<FileTree> fileTree = fileManager.getTree(studyFqn, "/", 5, new QueryOptions(), ownerToken);
        assertEquals(12, fileTree.getNumResults());

        fileTree = fileManager.getTree("phase2", ".", 5, new QueryOptions(), ownerToken);
        assertEquals(2, fileTree.getNumResults());
    }

    @Test
    public void renameFileTest() throws CatalogException {
        DataResult<File> queryResult1 = fileManager.create(studyFqn, new FileCreateParams()
                        .setPath("data/file.txt")
                        .setType(File.Type.FILE)
                        .setContent(RandomStringUtils.randomAlphanumeric(200)),
                true, ownerToken);
        assertEquals(1, queryResult1.getNumResults());

        DataResult<File> queryResult = fileManager.create(studyFqn, new FileCreateParams()
                        .setPath("data/nested/folder/file2.txt")
                        .setType(File.Type.FILE)
                        .setContent(RandomStringUtils.randomAlphanumeric(200)),
                true, ownerToken);
        assertEquals(1, queryResult.getNumResults());

        fileManager.rename(studyFqn, "data/nested/", "nested2", ownerToken);
        Set<String> paths = fileManager.search(studyFqn, new Query(), new QueryOptions(), ownerToken)
                .getResults()
                .stream().map(File::getPath).collect(Collectors.toSet());

        assertTrue(paths.contains("data/nested2/"));
        assertFalse(paths.contains("data/nested/"));
        assertTrue(paths.contains("data/nested2/folder/"));
        assertTrue(paths.contains("data/nested2/folder/file2.txt"));
        assertTrue(paths.contains("data/file.txt"));

        fileManager.rename(studyFqn, "data/", "Data", ownerToken);
        paths = fileManager.search(studyFqn, new Query(), new QueryOptions(), ownerToken).getResults()
                .stream().map(File::getPath).collect(Collectors.toSet());

        assertTrue(paths.contains("Data/"));
        assertTrue(paths.contains("Data/file.txt"));
        assertTrue(paths.contains("Data/nested2/"));
        assertTrue(paths.contains("Data/nested2/folder/"));
        assertTrue(paths.contains("Data/nested2/folder/file2.txt"));
    }

    @Test
    public void getFileIdByString() throws CatalogException {
        StudyAclParams aclParams = new StudyAclParams("", "analyst");
        catalogManager.getStudyManager().updateAcl(studyFqn, normalUserId2, aclParams, ParamUtils.AclAction.ADD, ownerToken);
        File file = fileManager.create(studyFqn,
                new FileCreateParams()
                        .setType(File.Type.FILE)
                        .setFormat(File.Format.UNKNOWN)
                        .setBioformat(File.Bioformat.NONE)
                        .setPath("data/test/folder/file.txt")
                        .setDescription("My description")
                        .setContent("blabla"),
                true, normalToken2).first();
        long fileId = fileManager.get(studyFqn, file.getPath(), FileManager.INCLUDE_FILE_IDS, ownerToken).first().getUid();
        assertEquals(file.getUid(), fileId);

        fileId = fileManager.get(studyFqn, file.getPath(), FileManager.INCLUDE_FILE_IDS, ownerToken).first().getUid();
        assertEquals(file.getUid(), fileId);

        fileId = fileManager.get(studyFqn, "/", FileManager.INCLUDE_FILE_IDS, ownerToken).first().getUid();
        System.out.println(fileId);
    }

    @Test
    public void renameFileEmptyName() throws CatalogException {
        thrown.expect(CatalogParameterException.class);
        thrown.expectMessage(containsString("null or empty"));
        fileManager.rename(studyFqn, "data/", "", ownerToken);
    }

    @Test
    public void renameFileSlashInName() throws CatalogException {
        thrown.expect(CatalogParameterException.class);
        fileManager.rename(studyFqn, "data/", "my/folder", ownerToken);
    }

    @Test
    public void renameFileAlreadyExists() throws CatalogException {
        fileManager.createFolder(studyFqn, "analysis/", false, "", new QueryOptions(),
                ownerToken);
        thrown.expect(CatalogException.class);
        thrown.expectMessage("already exists");
        fileManager.rename(studyFqn, "data/", "analysis", ownerToken);
    }

    @Test
    public void searchFileTest() throws CatalogException {
        Query query;
        DataResult<File> result;

        // Look for a file and folder
        DataResult<File> queryResults = fileManager.get(studyFqn, Arrays.asList("data/", "data/test/folder/test_1K.txt.gz"),
                new QueryOptions(QueryOptions.INCLUDE, Arrays.asList(FileDBAdaptor.QueryParams.NAME.key())), ownerToken);
        assertEquals(2, queryResults.getNumResults());
        assertTrue("Name not included", queryResults.getResults().stream().map(File::getName)
                .filter(org.apache.commons.lang3.StringUtils::isNotEmpty)
                .collect(Collectors.toList()).size() == 2);

        query = new Query(FileDBAdaptor.QueryParams.NAME.key(), "~data");
        result = fileManager.search(studyFqn, query, null, ownerToken);
        assertEquals(1, result.getNumResults());

        query = new Query(FileDBAdaptor.QueryParams.NAME.key(), "~txt.gz$");
        result = fileManager.search(studyFqn, query, null, ownerToken);
        assertEquals(1, result.getNumResults());

        //Get all files in data
        query = new Query(FileDBAdaptor.QueryParams.PATH.key(), "~data/[^/]+/?")
                .append(FileDBAdaptor.QueryParams.TYPE.key(), "FILE");
        result = fileManager.search(studyFqn, query, null, ownerToken);
        assertEquals(4, result.getNumResults());

        //Folder "jobs" does not exist
        query = new Query(FileDBAdaptor.QueryParams.DIRECTORY.key(), "jobs");
        result = fileManager.search(studyFqn, query, null, ownerToken);
        assertEquals(0, result.getNumResults());

        //Get all files in data
        query = new Query(FileDBAdaptor.QueryParams.DIRECTORY.key(), "data/");
        result = fileManager.search(studyFqn, query, null, ownerToken);
        assertEquals(2, result.getNumResults());

        //Get all files in data
        query = new Query(FileDBAdaptor.QueryParams.DIRECTORY.key(), "/data/");
        result = fileManager.search(studyFqn, query, null, ownerToken);
        assertEquals(2, result.getNumResults());

        //Get all files in data recursively
//        query = new Query(FileDBAdaptor.QueryParams.DIRECTORY.key(), "~data/.*");
//        result = fileManager.search(studyFqn, query, null, token);
//        assertEquals(5, result.getNumResults());

        query = new Query(FileDBAdaptor.QueryParams.TYPE.key(), "FILE");
        result = fileManager.search(studyFqn, query, null, ownerToken);
        result.getResults().forEach(f -> assertEquals(File.Type.FILE, f.getType()));
        int numFiles = result.getNumResults();
        assertEquals(4, numFiles);

        query = new Query(FileDBAdaptor.QueryParams.TYPE.key(), "DIRECTORY");
        result = fileManager.search(studyFqn, query, null, ownerToken);
        result.getResults().forEach(f -> assertEquals(File.Type.DIRECTORY, f.getType()));
        int numFolders = result.getNumResults();
        assertEquals(9, numFolders);

        query = new Query(FileDBAdaptor.QueryParams.PATH.key(), "");
        result = fileManager.search(studyFqn, query, null, ownerToken);
        assertEquals(1, result.getNumResults());
        assertEquals(".", result.first().getName());


        query = new Query(FileDBAdaptor.QueryParams.TYPE.key(), "FILE,DIRECTORY");
        result = fileManager.search(studyFqn, query, null, ownerToken);
        assertEquals(13, result.getNumResults());
        assertEquals(numFiles + numFolders, result.getNumResults());

        query = new Query("type", "FILE");
        query.put("size", ">500");
        result = fileManager.search(studyFqn, query, null, ownerToken);
        assertEquals(2, result.getNumResults());

        query = new Query("type", "FILE");
        query.put("size", "<=500");
        result = fileManager.search(studyFqn, query, null, ownerToken);
        assertEquals(2, result.getNumResults());

        List<String> sampleIds = catalogManager.getSampleManager().search(studyFqn, new Query(SampleDBAdaptor.QueryParams.ID.key(), "s_1,s_3,s_4"), null, ownerToken).getResults()
                .stream()
                .map(Sample::getId)
                .collect(Collectors.toList());
        result = fileManager.search(studyFqn, new Query(FileDBAdaptor.QueryParams.SAMPLE_IDS.key(), sampleIds), null, ownerToken);
        assertEquals(1, result.getNumResults());

        query = new Query(FileDBAdaptor.QueryParams.TYPE.key(), "FILE");
        query.put(FileDBAdaptor.QueryParams.FORMAT.key(), "PLAIN");
        result = fileManager.search(studyFqn, query, null, ownerToken);
        assertEquals(3, result.getNumResults());

        QueryOptions options = new QueryOptions(QueryOptions.LIMIT, 2).append(QueryOptions.COUNT, true);
        result = fileManager.search(studyFqn, new Query(), options, ownerToken);
        assertEquals(2, result.getNumResults());
        assertEquals(13, result.getNumMatches());
    }
//
//    @Test
//    public void testSearchFileBoolean() throws CatalogException {
//        Query query;
//        DataResult<File> result;
//        FileDBAdaptor.QueryParams battributes = FileDBAdaptor.QueryParams.BATTRIBUTES;
//
//        query = new Query(battributes.key() + ".boolean", "true");       //boolean in [true]
//        result = fileManager.search(studyFqn, query, null, token);
//        assertEquals(1, result.getNumResults());
//
//        query = new Query(battributes.key() + ".boolean", "false");      //boolean in [false]
//        result = fileManager.search(studyFqn, query, null, token);
//        assertEquals(1, result.getNumResults());
//
//        query = new Query(battributes.key() + ".boolean", "!=false");    //boolean in [null, true]
//        query.put("type", "FILE");
//        result = fileManager.search(studyFqn, query, null, token);
//        assertEquals(2, result.getNumResults());
//
//        query = new Query(battributes.key() + ".boolean", "!=true");     //boolean in [null, false]
//        query.put("type", "FILE");
//        result = fileManager.search(studyFqn, query, null, token);
//        assertEquals(2, result.getNumResults());
//    }

    @Test
    public void testSearchFileFail1() throws CatalogException {
        thrown.expect(CatalogDBException.class);
        fileManager.search(studyFqn, new Query(FileDBAdaptor.QueryParams.NATTRIBUTES.key() + ".numValue", "==NotANumber"), null,
                ownerToken);
    }

    @Test
    public void testGetFileParents1() throws CatalogException {
        DataResult<File> fileParents = fileManager.getParents(studyFqn, "data/test/folder/", true, QueryOptions.empty(), ownerToken);
        assertEquals(4, fileParents.getNumResults());
        assertEquals("", fileParents.getResults().get(0).getPath());
        assertEquals("data/", fileParents.getResults().get(1).getPath());
        assertEquals("data/test/", fileParents.getResults().get(2).getPath());
        assertEquals("data/test/folder/", fileParents.getResults().get(3).getPath());
    }

    @Test
    public void testGetFileParents2() throws CatalogException {
        DataResult<File> fileParents = fileManager.getParents(studyFqn, "data/test/folder/test_1K.txt.gz", true, QueryOptions.empty(), ownerToken);
        assertEquals(5, fileParents.getNumResults());
        assertEquals("", fileParents.getResults().get(0).getPath());
        assertEquals("data/", fileParents.getResults().get(1).getPath());
        assertEquals("data/test/", fileParents.getResults().get(2).getPath());
        assertEquals("data/test/folder/", fileParents.getResults().get(3).getPath());
        assertEquals("data/test/folder/test_1K.txt.gz", fileParents.getResults().get(4).getPath());
    }

    @Test
    public void testGetFileParents3() throws CatalogException {
        DataResult<File> fileParents = fileManager.getParents(studyFqn, "data/test/", true,
                new QueryOptions("include", "projects.studies.files.path,projects.studies.files.id"), ownerToken);
        assertEquals(3, fileParents.getNumResults());
        assertEquals("", fileParents.getResults().get(0).getPath());
        assertEquals("data/", fileParents.getResults().get(1).getPath());
        assertEquals("data/test/", fileParents.getResults().get(2).getPath());

        fileParents.getResults().forEach(f -> {
            assertNull(f.getName());
            assertNotNull(f.getPath());
            assertTrue(f.getUid() != 0);
        });

    }

    @Test
    public void testGetFileWithSamples() throws CatalogException {
        DataResult<File> fileDataResult = fileManager.get(studyFqn, "data/test/", QueryOptions.empty(),
                ownerToken);
        assertEquals(1, fileDataResult.getNumResults());
        assertEquals(0, fileDataResult.first().getSampleIds().size());

        // Create two samples
        Sample sample1 = catalogManager.getSampleManager().create(studyFqn, new Sample().setId("sample1"), INCLUDE_RESULT, ownerToken).first();
        Sample sample2 = catalogManager.getSampleManager().create(studyFqn, new Sample().setId("sample2"), INCLUDE_RESULT, ownerToken).first();

        // Associate the two samples to the file
        fileManager.update(studyFqn, "data/test/", new FileUpdateParams().setSampleIds(Arrays.asList(sample1.getId(), sample2.getId())),
                QueryOptions.empty(), ownerToken);

        // Fetch the file
        fileDataResult = fileManager.get(studyFqn, "data/test/", new QueryOptions(
                        QueryOptions.INCLUDE, Arrays.asList(FileDBAdaptor.QueryParams.ID.key(), FileDBAdaptor.QueryParams.SAMPLE_IDS.key())),
                ownerToken);
        assertEquals(1, fileDataResult.getNumResults());
        assertEquals(2, fileDataResult.first().getSampleIds().size());
        for (String sampleId : fileDataResult.first().getSampleIds()) {
            assertTrue(StringUtils.isNotEmpty(sampleId));
        }
    }

    // Try to delete files/folders whose status is STAGED, MISSING...
    @Test
    public void testDelete1() throws CatalogException {
        String filePath = "data/";
        Query query = new Query()
                .append(FileDBAdaptor.QueryParams.STUDY_UID.key(), studyUid)
                .append(FileDBAdaptor.QueryParams.PATH.key(), filePath);
        DataResult<File> fileDataResult = fileManager.search(studyFqn, query, null, ownerToken);

        // Change the status to MISSING
        FileUpdateParams updateParams = new FileUpdateParams()
                .setInternal(new SmallFileInternal(new FileStatus(FileStatus.MISSING)));
        catalogManager.getFileManager().update(studyFqn, filePath, updateParams, null, ownerToken);

        try {
            fileManager.delete(studyFqn, new Query(FileDBAdaptor.QueryParams.UID.key(), fileDataResult.first().getUid()), null, ownerToken);
            fail("Expected fail. It should not be able to delete");
        } catch (CatalogException e) {
            assertTrue(e.getMessage().contains("Cannot delete"));
        }
        // Change the status to STAGED
        updateParams = new FileUpdateParams()
                .setInternal(new SmallFileInternal(new FileStatus(FileStatus.STAGE)));
        catalogManager.getFileManager().update(studyFqn, filePath, updateParams, null, ownerToken);

        try {
            fileManager.delete(studyFqn, new Query(FileDBAdaptor.QueryParams.UID.key(), fileDataResult.first().getUid()), null, ownerToken);
            fail("Expected fail. It should not be able to delete");
        } catch (CatalogException e) {
            assertTrue(e.getMessage().contains("Cannot delete"));
        }

        // Change the status to READY
        query = new Query(FileDBAdaptor.QueryParams.PATH.key(), "~^" + filePath + "*");
        setToPendingDelete(studyFqn, query);

        DataResult deleteResult = fileManager.delete(studyFqn, new Query(FileDBAdaptor.QueryParams.UID.key(),
                fileDataResult.first().getUid()), null, ownerToken);
        assertEquals(11, deleteResult.getNumMatches());
        assertEquals(11, deleteResult.getNumUpdated());
    }

    // It will try to delete a folder in status ready
    @Test
    public void testDelete2() throws CatalogException, IOException {
        String filePath = "data/";
        Query query = new Query()
                .append(FileDBAdaptor.QueryParams.STUDY_UID.key(), studyUid)
                .append(FileDBAdaptor.QueryParams.PATH.key(), filePath);
        File file = fileManager.search(studyFqn, query, null, ownerToken).first();

        // We look for all the files and folders that fall within that folder
        query = new Query()
                .append(FileDBAdaptor.QueryParams.STUDY_UID.key(), studyUid)
                .append(FileDBAdaptor.QueryParams.PATH.key(), "~^" + filePath + "*")
                .append(FileDBAdaptor.QueryParams.INTERNAL_STATUS_ID.key(), FileStatus.READY);
        int numResults = fileManager.search(studyFqn, query, null, ownerToken).getNumResults();
        assertEquals(11, numResults);

        query = new Query(FileDBAdaptor.QueryParams.PATH.key(), "~^" + file.getPath() + "*");
        setToPendingDelete(studyFqn, query);

        // We delete it
        fileManager.delete(studyFqn, new Query(FileDBAdaptor.QueryParams.UID.key(), file.getUid()), null, ownerToken);

        // The files should have been moved to trashed status
        OpenCGAResult<File> search = fileManager.search(studyFqn, query, null, ownerToken);
        assertEquals(11, search.getNumResults());
        for (File trashedFile : search.getResults()) {
            assertEquals(FileStatus.TRASHED, trashedFile.getInternal().getStatus().getId());
        }
    }

    // It will try to delete a folder in status ready and skip the trash
    @Test
    public void testDelete3() throws CatalogException, IOException {
        String filePath = "data/";
        Query query = new Query()
                .append(FileDBAdaptor.QueryParams.STUDY_UID.key(), studyUid)
                .append(FileDBAdaptor.QueryParams.PATH.key(), filePath);
        File file = fileManager.search(studyFqn, query, null, ownerToken).first();

        // We look for all the files and folders that fall within that folder
        query = new Query()
                .append(FileDBAdaptor.QueryParams.STUDY_UID.key(), studyUid)
                .append(FileDBAdaptor.QueryParams.PATH.key(), "~^" + filePath + "*")
                .append(FileDBAdaptor.QueryParams.INTERNAL_STATUS.key(), FileStatus.READY);
        int numResults = fileManager.search(studyFqn, query, null, ownerToken).getNumResults();
        assertEquals(11, numResults);

        setToPendingDelete(studyFqn, query);

        // We delete it
        QueryOptions queryOptions = new QueryOptions(Constants.SKIP_TRASH, true);
        fileManager.delete(studyFqn, new Query(FileDBAdaptor.QueryParams.UID.key(), file.getUid()),
                queryOptions, ownerToken);

        // The files should have been moved to trashed status
        numResults = fileManager.search(studyFqn, query, null, ownerToken).getNumResults();
        assertEquals(0, numResults);

        query.put(FileDBAdaptor.QueryParams.DELETED.key(), true);
        query.put(FileDBAdaptor.QueryParams.INTERNAL_STATUS.key(), FileStatus.DELETED);
        numResults = fileManager.search(studyFqn, query, null, ownerToken).getNumResults();
        assertEquals(11, numResults);
    }

    @Test
    public void testDeleteFile() throws CatalogException, IOException {
        List<File> result = fileManager.search(studyFqn, new Query(FileDBAdaptor.QueryParams.TYPE.key(),
                "FILE"), new QueryOptions(), ownerToken).getResults();

        // 1st we set the status to PENDING DELETE.
        setToPendingDelete(studyFqn, new Query(FileDBAdaptor.QueryParams.TYPE.key(), "FILE"));

        for (File file : result) {
            fileManager.delete(studyFqn, new Query(FileDBAdaptor.QueryParams.UID.key(), file.getUid()), null, ownerToken);
        }
//        CatalogFileUtils catalogFileUtils = new CatalogFileUtils(catalogManager);
        fileManager.search(studyFqn, new Query(FileDBAdaptor.QueryParams.TYPE.key(), "FILE"), new QueryOptions(), ownerToken).getResults().forEach(f -> {
            assertEquals(f.getInternal().getStatus().getId(), FileStatus.TRASHED);
        });

        result = fileManager.search(studyFqn2, new Query(FileDBAdaptor.QueryParams.TYPE.key(), "FILE"), new QueryOptions(), ownerToken).getResults();
        // 1st we set the status to PENDING DELETE.
        setToPendingDelete(studyFqn2, new Query(FileDBAdaptor.QueryParams.TYPE.key(), "FILE"));
        for (File file : result) {
            fileManager.delete(studyFqn2, new Query(FileDBAdaptor.QueryParams.UID.key(), file.getUid()), null, ownerToken);
        }
        fileManager.search(studyFqn2, new Query(FileDBAdaptor.QueryParams.TYPE.key(), "FILE"), new QueryOptions(), ownerToken).getResults().forEach(f -> {
            assertEquals(f.getInternal().getStatus().getId(), FileStatus.TRASHED);
        });
    }

    @Test
    public void removeFileReferencesFromSamplesOnFileDeleteTest() throws CatalogException {
        String fileId = "data:test:folder:test_0.1K.png";
        File file = fileManager.get(studyFqn, fileId, QueryOptions.empty(), ownerToken).first();
        assertFalse(file.getSampleIds().isEmpty());
        assertEquals(5, file.getSampleIds().size());

        List<Sample> samples = catalogManager.getSampleManager().search(studyFqn,
                new Query(SampleDBAdaptor.QueryParams.FILE_IDS.key(), fileId), QueryOptions.empty(), ownerToken).getResults();
        assertEquals(5, samples.size());

        for (Sample sample : samples) {
            assertEquals(1, sample.getFileIds().size());
            assertEquals(fileId, sample.getFileIds().get(0));
        }

        // Send to TRASH BIN
        setToPendingDelete(studyFqn, new Query(FileDBAdaptor.QueryParams.ID.key(), fileId));
        fileManager.delete(studyFqn, Collections.singletonList(fileId), QueryOptions.empty(), ownerToken);

        file = fileManager.get(studyFqn, fileId, QueryOptions.empty(), ownerToken).first();
        assertFalse(file.getSampleIds().isEmpty());
        assertEquals(5, file.getSampleIds().size());
        assertEquals(FileStatus.TRASHED, file.getInternal().getStatus().getId());

        samples = catalogManager.getSampleManager().search(studyFqn,
                new Query(SampleDBAdaptor.QueryParams.FILE_IDS.key(), fileId), QueryOptions.empty(), ownerToken).getResults();
        assertEquals(5, samples.size());

        for (Sample sample : samples) {
            assertEquals(1, sample.getFileIds().size());
            assertEquals(fileId, sample.getFileIds().get(0));
        }

        // Delete permanently
        setToPendingDelete(studyFqn, new Query(FileDBAdaptor.QueryParams.ID.key(), fileId));
        fileManager.delete(studyFqn, Collections.singletonList(fileId), new QueryOptions(Constants.SKIP_TRASH, true), ownerToken);

        List<Sample> noResults = catalogManager.getSampleManager().search(studyFqn,
                new Query(SampleDBAdaptor.QueryParams.FILE_IDS.key(), fileId), QueryOptions.empty(), ownerToken).getResults();
        assertEquals(0, noResults.size());

        samples = catalogManager.getSampleManager().get(studyFqn, samples.stream().map(Sample::getId).collect(Collectors.toList()),
                QueryOptions.empty(), ownerToken).getResults();
        assertEquals(5, samples.size());

        for (Sample sample : samples) {
            assertEquals(0, sample.getFileIds().size());
        }

        thrown.expect(CatalogException.class);
        thrown.expectMessage("Missing");
        fileManager.get(studyFqn, fileId, QueryOptions.empty(), ownerToken).first();
    }


    @Test
    public void testDeleteLeafFolder() throws CatalogException, IOException {
        File deletable = fileManager.get(studyFqn, "/data/test/folder/", QueryOptions.empty(), ownerToken).first();
        deleteFolderAndCheck(deletable);
    }

    @Test
    public void testDeleteMiddleFolder() throws CatalogException, IOException {
        File deletable = fileManager.get(studyFqn, "/data/", QueryOptions.empty(), ownerToken).first();
        deleteFolderAndCheck(deletable);
    }

    @Test
    public void testDeleteRootFolder() throws CatalogException {
        File deletable = fileManager.get(studyFqn, "/", QueryOptions.empty(), ownerToken).first();

        thrown.expect(CatalogException.class);
        thrown.expectMessage("Root directories cannot be deleted");
        fileManager.delete(studyFqn, new Query(FileDBAdaptor.QueryParams.PATH.key(), deletable.getPath()), null, ownerToken);
    }

    // Cannot delete staged files
    @Test
    public void deleteFolderTest() throws CatalogException, IOException {
        List<File> folderFiles = new LinkedList<>();

        File folder = createBasicDirectoryFileTestEnvironment(folderFiles);

        IOManager ioManager = catalogManager.getIoManagerFactory().get(fileManager.getUri(organizationId, folder));
        for (File file : folderFiles) {
            assertTrue(ioManager.exists(fileManager.getUri(organizationId, file)));
        }

        fileManager.create(studyFqn, new FileCreateParams()
                        .setType(File.Type.FILE)
                        .setFormat(File.Format.PLAIN)
                        .setPath("folder/subfolder/subsubfolder/my_staged.txt")
                        .setContent("bla bla"),
                true, ownerToken).first();

        Query query = new Query()
                .append(FileDBAdaptor.QueryParams.PATH.key(), "~^" + folder.getPath() + "*")
                .append(FileDBAdaptor.QueryParams.INTERNAL_STATUS_ID.key(), FileStatus.READY);
        setToPendingDelete(studyFqn, query);

        File fileTmp = fileManager.get(studyFqn, folder.getPath(), null, ownerToken).first();
        assertEquals("Folder name should not be modified", folder.getPath(), fileTmp.getPath());
        assertTrue(ioManager.exists(fileTmp.getUri()));

        for (File file : folderFiles) {
            fileTmp = fileManager.get(studyFqn, file.getPath(), null, ownerToken).first();
            assertEquals("File name should not be modified", file.getPath(), fileTmp.getPath());
            assertTrue("File uri: " + fileTmp.getUri() + " should exist", ioManager.exists(fileTmp.getUri()));
        }

    }

    // Deleted folders should be all put to TRASHED
    @Test
    public void deleteFolderTest2() throws CatalogException, IOException {
        List<File> folderFiles = new LinkedList<>();

        File folder = createBasicDirectoryFileTestEnvironment(folderFiles);

        IOManager ioManager = catalogManager.getIoManagerFactory().get(fileManager.getUri(organizationId, folder));
        for (File file : folderFiles) {
            assertTrue(ioManager.exists(fileManager.getUri(organizationId, file)));
        }

        // 1st we set the status to PENDING DELETE.
        setToPendingDelete(studyFqn, new Query(FileDBAdaptor.QueryParams.PATH.key(), "~^" + folder.getPath() + "*"));

        // Now we delete the files
        fileManager.delete(studyFqn, new Query(FileDBAdaptor.QueryParams.UID.key(), folder.getUid()), null, ownerToken);

        Query query = new Query()
                .append(FileDBAdaptor.QueryParams.UID.key(), folder.getUid());
        File fileTmp = fileManager.search(studyFqn, query, QueryOptions.empty(), ownerToken).first();

        assertEquals("Folder name should not be modified", folder.getPath(), fileTmp.getPath());
        assertEquals("Status should be to TRASHED", FileStatus.TRASHED, fileTmp.getInternal().getStatus().getId());
        assertEquals("Name should not have changed", folder.getName(), fileTmp.getName());
        assertTrue(ioManager.exists(fileTmp.getUri()));

        for (File file : folderFiles) {
            query.put(FileDBAdaptor.QueryParams.UID.key(), file.getUid());
            fileTmp = fileManager.search(studyFqn, query, QueryOptions.empty(), ownerToken).first();
            assertEquals("Folder name should not be modified", file.getPath(), fileTmp.getPath());
            assertEquals("Status should be to TRASHED", FileStatus.TRASHED, fileTmp.getInternal().getStatus().getId());
            assertEquals("Name should not have changed", file.getName(), fileTmp.getName());
            assertTrue("File uri: " + fileTmp.getUri() + " should exist", ioManager.exists(fileTmp.getUri()));
        }
    }

    // READY -> PENDING_DELETE
    @Test
    public void deleteFolderTest3() throws CatalogException, IOException {
        List<File> folderFiles = new LinkedList<>();

        File folder = createBasicDirectoryFileTestEnvironment(folderFiles);

        IOManager ioManager = catalogManager.getIoManagerFactory().get(fileManager.getUri(organizationId, folder));
        for (File file : folderFiles) {
            assertTrue(ioManager.exists(fileManager.getUri(organizationId, file)));
        }

        // 1st we set the status to PENDING DELETE.
        setToPendingDelete(studyFqn, new Query(FileDBAdaptor.QueryParams.PATH.key(), "~^" + folder.getPath() + "*"));

        Query query = new Query(FileDBAdaptor.QueryParams.PATH.key(), "~^" + folder.getPath() + "*");
        OpenCGAResult<File> results = fileManager.search(studyFqn, query,
                new QueryOptions(QueryOptions.INCLUDE, FileDBAdaptor.QueryParams.INTERNAL_STATUS.key()), ownerToken);
        assertEquals(9, results.getNumResults());
        for (File result : results.getResults()) {
            assertEquals(FileStatus.PENDING_DELETE, result.getInternal().getStatus().getId());
        }
    }

    // READY -> PENDING_DELETE -> DELETED
    @Test
    public void deleteFolderTest4() throws CatalogException, IOException {
        List<File> folderFiles = new LinkedList<>();

        File folder = createBasicDirectoryFileTestEnvironment(folderFiles);

        IOManager ioManager = catalogManager.getIoManagerFactory().get(fileManager.getUri(organizationId, folder));
        for (File file : folderFiles) {
            assertTrue(ioManager.exists(fileManager.getUri(organizationId, file)));
        }

        // 1st we set the status to PENDING DELETE.
        setToPendingDelete(studyFqn, new Query(FileDBAdaptor.QueryParams.PATH.key(), "~^" + folder.getPath() + "*"));

        // Now we delete the files
        QueryOptions params = new QueryOptions(Constants.SKIP_TRASH, true);
        fileManager.delete(studyFqn, new Query(FileDBAdaptor.QueryParams.UID.key(), folder.getUid()), params, ownerToken);

        Query query = new Query()
                .append(FileDBAdaptor.QueryParams.UID.key(), folder.getUid())
                .append(FileDBAdaptor.QueryParams.DELETED.key(), true);
        File fileTmp = fileManager.search(studyFqn, query, QueryOptions.empty(), ownerToken).first();

        assertEquals("Folder name should not be modified", folder.getPath(), fileTmp.getPath());
        assertEquals("Status should be to DELETED", FileStatus.DELETED, fileTmp.getInternal().getStatus().getId());
        assertEquals("Name should not have changed", folder.getName(), fileTmp.getName());
        assertFalse(ioManager.exists(fileTmp.getUri()));

        for (File file : folderFiles) {
            query.put(FileDBAdaptor.QueryParams.UID.key(), file.getUid());
            fileTmp = fileManager.search(studyFqn, query, QueryOptions.empty(), ownerToken).first();
            assertEquals("Folder name should not be modified", file.getPath(), fileTmp.getPath());
            assertEquals("Status should be to DELETED", FileStatus.DELETED, fileTmp.getInternal().getStatus().getId());
            assertEquals("Name should not have changed", file.getName(), fileTmp.getName());
            assertFalse("File uri: " + fileTmp.getUri() + " should not exist", ioManager.exists(fileTmp.getUri()));
        }
    }

    private File createBasicDirectoryFileTestEnvironment(List<File> folderFiles) throws CatalogException {
        File folder = fileManager.createFolder(studyFqn, Paths.get("folder").toString(), false,
                null, QueryOptions.empty(), ownerToken).first();
        folderFiles.add(
                fileManager.create(studyFqn,
                        new FileCreateParams()
                                .setPath("folder/my.txt")
                                .setType(File.Type.FILE)
                                .setContent(RandomStringUtils.randomAlphanumeric(200)),
                        false, ownerToken).first()
        );
        folderFiles.add(
                fileManager.create(studyFqn,
                        new FileCreateParams()
                                .setPath("folder/my2.txt")
                                .setType(File.Type.FILE)
                                .setContent(RandomStringUtils.randomAlphanumeric(200)),
                        false, ownerToken).first()
        );
        folderFiles.add(
                fileManager.create(studyFqn,
                        new FileCreateParams()
                                .setPath("folder/my3.txt")
                                .setType(File.Type.FILE)
                                .setContent(RandomStringUtils.randomAlphanumeric(200)),
                        false, ownerToken).first()
        );
        folderFiles.add(
                fileManager.create(studyFqn,
                        new FileCreateParams()
                                .setPath("folder/subfolder/my4.txt")
                                .setType(File.Type.FILE)
                                .setContent(RandomStringUtils.randomAlphanumeric(200)),
                        true, ownerToken).first()
        );
        folderFiles.add(
                fileManager.create(studyFqn,
                        new FileCreateParams()
                                .setPath("folder/subfolder/my5.txt")
                                .setType(File.Type.FILE)
                                .setContent(RandomStringUtils.randomAlphanumeric(200)),
                        false, ownerToken).first()
        );
        folderFiles.add(
                fileManager.create(studyFqn,
                        new FileCreateParams()
                                .setPath("folder/subfolder/subsubfolder/my6.txt")
                                .setType(File.Type.FILE)
                                .setContent(RandomStringUtils.randomAlphanumeric(200)),
                        true, ownerToken).first()
        );
        return folder;
    }

    @Test
    public void sendFolderToTrash() {

    }

    @Test
    public void getAllFilesInFolder() throws CatalogException {
        List<File> allFilesInFolder = fileManager.getFilesFromFolder(studyFqn, "/data/test/folder/", null,
                ownerToken).getResults();
        assertEquals(3, allFilesInFolder.size());
    }

    private void deleteFolderAndCheck(File deletable) throws CatalogException {
        List<File> allFilesInFolder;
        Study study = fileManager.getStudy(organizationId, deletable, ownerToken);

        // 1st, we set the status to PENDING_DELETE
        Query query = new Query(FileDBAdaptor.QueryParams.PATH.key(), "~^" + deletable.getPath() + "*");
        setToPendingDelete(study.getFqn(), query);

        fileManager.delete(study.getFqn(), query, null, ownerToken);

        query = new Query()
                .append(FileDBAdaptor.QueryParams.PATH.key(), deletable.getPath())
                .append(FileDBAdaptor.QueryParams.INTERNAL_STATUS_ID.key(), FileStatus.TRASHED);
        QueryOptions options = new QueryOptions(QueryOptions.INCLUDE, FileDBAdaptor.QueryParams.PATH.key());
        DataResult<File> fileDataResult = fileManager.search(study.getFqn(), query, options, ownerToken);
        assertEquals(1, fileDataResult.getNumResults());

        query = new Query()
                .append(FileDBAdaptor.QueryParams.DIRECTORY.key(), fileDataResult.first().getPath() + ".*")
                .append(FileDBAdaptor.QueryParams.INTERNAL_STATUS_ID.key(), FileStatus.TRASHED);
        allFilesInFolder = fileManager.search(study.getFqn(), query, null, ownerToken).getResults();

        for (File subFile : allFilesInFolder) {
            assertTrue(subFile.getInternal().getStatus().getId().equals(FileStatus.TRASHED));
        }
    }

    private void setToPendingDelete(String study, Query query) throws CatalogException {
        FileUpdateParams updateParams = new FileUpdateParams()
                .setInternal(new SmallFileInternal(new FileStatus(FileStatus.PENDING_DELETE)));
        fileManager.update(study, query, updateParams, QueryOptions.empty(), ownerToken);
    }

    @Test
    public void assignPermissionsRecursively() throws Exception {
        Path folderPath = Paths.get("data", "new", "folder");
        fileManager.createFolder(studyFqn, folderPath.toString(), true, null,
                QueryOptions.empty(), ownerToken).first();

        Path filePath = Paths.get("data", "file1.txt");
        fileManager.create(studyFqn,
                new FileCreateParams()
                        .setType(File.Type.FILE)
                        .setFormat(File.Format.UNKNOWN)
                        .setBioformat(File.Bioformat.UNKNOWN)
                        .setPath(filePath.toString())
                        .setDescription("")
                        .setContent("My content"),
                true, ownerToken);

        OpenCGAResult<AclEntryList<FilePermissions>> dataResult = fileManager.updateAcl(studyFqn, Arrays.asList("data/new/",
                filePath.toString()), normalUserId2, new FileAclParams(null, "VIEW"), ParamUtils.AclAction.SET, ownerToken);

        assertEquals(3, dataResult.getNumResults());
        for (AclEntryList<FilePermissions> result : dataResult.getResults()) {
            assertEquals(1, result.getAcl().size());
            assertEquals(normalUserId2, result.getAcl().get(0).getMember());
            assertEquals(1, result.getAcl().get(0).getPermissions().size());
            assertTrue(result.getAcl().get(0).getPermissions().contains(FilePermissions.VIEW));
        }
    }

    @Test
    public void testUpdateIndexStatus() throws CatalogException, URISyntaxException, IOException {
        Path sourcePath = Paths.get(getClass().getResource("/biofiles/variant-test-file.vcf.gz").toURI());

        DataResult<File> fileResult = fileManager.link(studyFqn, new FileLinkParams()
                        .setUri(sourcePath.toString())
                        .setPath("data/"),
                true, ownerToken);

        fileManager.updateFileInternalVariantIndex(studyFqn, fileResult.first(), FileInternalVariantIndex.init()
                .setStatus(new VariantIndexStatus(VariantIndexStatus.TRANSFORMED, null)), ownerToken);
        DataResult<File> read = fileManager.get(studyFqn, fileResult.first().getPath(), new QueryOptions(), ownerToken);
        assertEquals(VariantIndexStatus.TRANSFORMED, read.first().getInternal().getVariant().getIndex().getStatus().getId());
    }

    @Test
    public void testMoveAndRegister() throws URISyntaxException, CatalogException, IOException {
        Path sourcePath = Paths.get(getClass().getResource("/biofiles/variant-test-file.vcf.gz").toURI());
        Path copy = Paths.get("/tmp/variant-test-file.vcf.gz");
        if (Files.notExists(copy)) {
            Files.copy(sourcePath, copy);
        }
        if (Files.exists(Paths.get("/tmp/other"))) {
            catalogManager.getIoManagerFactory().getDefault().deleteDirectory(Paths.get("/tmp/other").toUri());
        }

        Study study = catalogManager.getStudyManager().resolveId(studyFqn, "user", organizationId);

        Path studyPath = Paths.get(study.getUri());
        // Register in workspace folder
        OpenCGAResult<File> result = fileManager.moveAndRegister(studyFqn, copy, studyPath.resolve("myFolder"), "myFolder", ownerToken);
        assertEquals("myFolder/variant-test-file.vcf.gz", result.first().getPath());
        assertEquals(studyPath.resolve("myFolder").resolve("variant-test-file.vcf.gz").toString(),
                Paths.get(result.first().getUri()).toString());
        assertTrue(Files.exists(studyPath.resolve("myFolder").resolve("variant-test-file.vcf.gz")));

        // We remove the file to start again
        Query query = new Query(FileDBAdaptor.QueryParams.UID.key(), result.first().getUid());
        setToPendingDelete(studyFqn, query);
        fileManager.delete(studyFqn, query, new QueryOptions(Constants.SKIP_TRASH, true), ownerToken);
        assertEquals(0, fileManager.search(studyFqn, query, QueryOptions.empty(), ownerToken).getNumResults());
        Files.copy(sourcePath, copy);

        // Register without passing the path
        result = fileManager.moveAndRegister(studyFqn, copy, studyPath.resolve("myFolder"), null, ownerToken);
        assertEquals("myFolder/variant-test-file.vcf.gz", result.first().getPath());
        assertEquals(studyPath.resolve("myFolder").resolve("variant-test-file.vcf.gz").toString(), Paths.get(result.first().getUri()).toString());
        assertTrue(Files.exists(studyPath.resolve("myFolder").resolve("variant-test-file.vcf.gz")));

        // We remove the file to start again
        query = new Query(FileDBAdaptor.QueryParams.UID.key(), result.first().getUid());
        setToPendingDelete(studyFqn, query);
        fileManager.delete(studyFqn, query, new QueryOptions(Constants.SKIP_TRASH, true), ownerToken);
        assertEquals(0, fileManager.search(studyFqn, query, QueryOptions.empty(), ownerToken).getNumResults());
        Files.copy(sourcePath, copy);

        // Register without passing the destiny path
        result = fileManager.moveAndRegister(studyFqn, copy, null, "myFolder", ownerToken);
        assertEquals("myFolder/variant-test-file.vcf.gz", result.first().getPath());
        assertEquals(studyPath.resolve("myFolder").resolve("variant-test-file.vcf.gz").toString(), Paths.get(result.first().getUri()).toString());
        assertTrue(Files.exists(studyPath.resolve("myFolder").resolve("variant-test-file.vcf.gz")));

        // We remove the file to start again
        query = new Query(FileDBAdaptor.QueryParams.UID.key(), result.first().getUid());
        setToPendingDelete(studyFqn, query);
        fileManager.delete(studyFqn, query, new QueryOptions(Constants.SKIP_TRASH, true), ownerToken);
        assertEquals(0, fileManager.search(studyFqn, query, QueryOptions.empty(), ownerToken).getNumResults());
        Files.copy(sourcePath, copy);

        // Register to an incorrect path
        try {
            fileManager.moveAndRegister(studyFqn, copy, studyPath.resolve("myFolder"), "otherFolder", ownerToken);
            fail("The method should have raised an error saying the path does not match the one corresponding to the uri. It should both "
                    + "point to myFolder or to otherFolder, but not to different paths.");
        } catch (CatalogException e) {
            assertTrue("Destination uri within the workspace and path do not match".equals(e.getMessage()));
        }

        // We grant permissions to normalUserId2 to the study
        catalogManager.getStudyManager().updateAcl(studyFqn, normalUserId2,
                new StudyAclParams("", "admin"), ParamUtils.AclAction.ADD, ownerToken);

        // Now, instead of moving it to the user's workspace, we will move it to an external path
        try {
            fileManager.moveAndRegister(studyFqn, copy, Paths.get("/tmp/other/"), "a/b/c/", normalToken2);
            fail("user2 should not have permissions to move to an external folder");
        } catch (CatalogAuthorizationException e) {
            assertTrue(e.getMessage().contains("owners or administrative users"));
        }

        // Now we add normalUserId2 to admins group
        catalogManager.getStudyManager().updateGroup(studyFqn, ParamConstants.ADMINS_GROUP, ParamUtils.BasicUpdateAction.ADD,
                new GroupUpdateParams(Collections.singletonList(normalUserId2)), ownerToken);

        // and try the same action again
        result = fileManager.moveAndRegister(studyFqn, copy, Paths.get("/tmp/other/"), "a/b/c/", normalToken2);
        assertEquals("a/b/c/variant-test-file.vcf.gz", result.first().getPath());
        assertEquals("/tmp/other/variant-test-file.vcf.gz", Paths.get(result.first().getUri()).toString());
        assertTrue(Files.exists(Paths.get("/tmp/other/variant-test-file.vcf.gz")));
        assertTrue(result.first().isExternal());
    }

//    @Test
//    public void testIndex() throws Exception {
//        URI uri = getClass().getResource("/biofiles/variant-test-file.vcf.gz").toURI();
//        File file = fileManager.link(studyFqn, uri, "", null, sessionIdUser).first();
//        assertEquals(4, file.getSamples().size());
//        assertEquals(File.Format.VCF, file.getFormat());
//        assertEquals(File.Bioformat.VARIANT, file.getBioformat());
//
//        Job job = fileManager.index(studyFqn, Collections.singletonList(file.getName()), "VCF", null, sessionIdUser).first();
//        assertEquals(file.getUid(), job.getInput().get(0).getUid());
//    }
//
//    @Test
//    public void testIndexFromAvro() throws Exception {
//        URI uri = getClass().getResource("/biofiles/variant-test-file.vcf.gz").toURI();
//        File file = fileManager.link(studyFqn, uri, "data", null, sessionIdUser).first();
//        fileManager.create(studyFqn, File.Type.FILE, File.Format.AVRO, null, "data/variant-test-file.vcf.gz.variants.avro.gz",
//                "description", new File.FileStatus(File.FileStatus.READY), 0, Collections.emptyList(), -1, Collections.emptyMap(), Collections.emptyMap(), true, "asdf", new QueryOptions(), sessionIdUser);
//        fileManager.link(studyFqn, getClass().getResource("/biofiles/variant-test-file.vcf.gz.file.json.gz").toURI(), "data", null,
//                sessionIdUser).first();
//
//        Job job = fileManager.index(studyFqn, Collections.singletonList("variant-test-file.vcf.gz.variants.avro.gz"), "VCF", null,
//                sessionIdUser).first();
//        assertEquals(file.getUid(), job.getInput().get(0).getUid());
//    }
//
//    @Test
//    public void testIndexFromAvroIncomplete() throws Exception {
//        URI uri = getClass().getResource("/biofiles/variant-test-file.vcf.gz").toURI();
//        File file = fileManager.link(studyFqn, uri, "data", null, sessionIdUser).first();
//        fileManager.create(studyFqn, File.Type.FILE, File.Format.AVRO, null, "data/variant-test-file.vcf.gz.variants.avro.gz",
//                "description", new File.FileStatus(File.FileStatus.READY), 0, Collections.emptyList(), -1, Collections.emptyMap(),
//                Collections.emptyMap(), true, "asdf", new QueryOptions(), sessionIdUser);
////        fileManager.link(getClass().getResource("/biofiles/variant-test-file.vcf.gz.file.json.gz").toURI(), "data", studyUid, null, s
////        essionIdUser).first();
//
//
//        thrown.expect(CatalogException.class);
//        thrown.expectMessage("The file variant-test-file.vcf.gz.variants.avro.gz is not a VCF file.");
//        fileManager.index(studyFqn, Collections.singletonList("variant-test-file.vcf.gz.variants.avro.gz"), "VCF", null, sessionIdUser).first();
//    }

//    @Test
//    public void testMassiveUpdateFileAcl() throws CatalogException {
//        List<String> fileIdList = new ArrayList<>();
//
//        // Create 2000 files
//        for (int i = 0; i < 10000; i++) {
//            fileIdList.add(String.valueOf(fileManager.createFile("user@1000G:phase1", "file_" + i + ".txt", "", false,
//                    "File " + i, sessionIdUser).first().getId()));
//        }
//
//        StopWatch watch = StopWatch.createStarted();
//        // Assign VIEW permissions to all those files
//        fileManager.updateAcl("user@1000G:phase1", fileIdList, "*,user2,user3", new File.FileAclParams("VIEW",
//                AclParams.Action.SET, null), sessionIdUser);
//        System.out.println("Time: " + watch.getTime(TimeUnit.MILLISECONDS) + " milliseconds");
//        System.out.println("Time: " + watch.getTime(TimeUnit.SECONDS) + " seconds");
//
//        watch.reset();
//        watch.start();
//        // Assign VIEW permissions to all those files
//        fileManager.updateAcl("user@1000G:phase1", fileIdList, "*,user2,user3", new File.FileAclParams("VIEW",
//                AclParams.Action.SET, null), sessionIdUser);
//        System.out.println("Time: " + watch.getTime(TimeUnit.MILLISECONDS) + " milliseconds");
//        System.out.println("Time: " + watch.getTime(TimeUnit.SECONDS) + " seconds");
//
//        watch.reset();
//        watch.start();
//        // Assign VIEW permissions to all those files
//        fileManager.updateAcl("user@1000G:phase1", fileIdList, "*,user2,user3", new File.FileAclParams("VIEW",
//                AclParams.Action.SET, null), sessionIdUser);
//        System.out.println("Time: " + watch.getTime(TimeUnit.MILLISECONDS) + " milliseconds");
//        System.out.println("Time: " + watch.getTime(TimeUnit.SECONDS) + " seconds");
//
//        watch.reset();
//        watch.start();
//        // Assign VIEW permissions to all those files
//        fileManager.updateAcl("user@1000G:phase1", fileIdList, "*,user2,user3", new File.FileAclParams("VIEW",
//                AclParams.Action.SET, null), sessionIdUser);
//        System.out.println("Time: " + watch.getTime(TimeUnit.MILLISECONDS) + " milliseconds");
//        System.out.println("Time: " + watch.getTime(TimeUnit.SECONDS) + " seconds");
//
//        watch.reset();
//        watch.start();
//        // Assign VIEW permissions to all those files
//        fileManager.updateAcl("user@1000G:phase1", fileIdList, "*,user2,user3", new File.FileAclParams("VIEW",
//                AclParams.Action.SET, null), sessionIdUser);
//        System.out.println("Time: " + watch.getTime(TimeUnit.MILLISECONDS) + " milliseconds");
//        System.out.println("Time: " + watch.getTime(TimeUnit.SECONDS) + " seconds");
//    }
}
