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

package org.opencb.opencga.server.rest;

import org.glassfish.jersey.media.multipart.FormDataBodyPart;
import org.glassfish.jersey.media.multipart.MultiPart;
import org.glassfish.jersey.media.multipart.file.StreamDataBodyPart;
import org.junit.*;
import org.junit.rules.ExpectedException;
import org.opencb.biodata.models.variant.Variant;
import org.opencb.commons.datastore.core.*;
import org.opencb.opencga.catalog.db.api.FileDBAdaptor;
import org.opencb.opencga.catalog.exceptions.CatalogException;
import org.opencb.opencga.catalog.managers.CatalogManagerTest;
import org.opencb.opencga.core.common.IOUtils;
import org.opencb.opencga.core.models.file.File;
import org.opencb.opencga.core.models.job.Job;
import org.opencb.opencga.storage.core.variant.VariantStorageOptions;

import javax.ws.rs.client.Entity;
import javax.ws.rs.client.WebTarget;
import javax.ws.rs.core.MediaType;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.assertEquals;

/**
 * Created by jacobo on 13/06/15.
 */
public class FileWSServerTest {

    private WebTarget webTarget;
    private static WSServerTestUtils serverTestUtils;
    private String sessionId;
    private String studyId = "user@1000G:phase1";
    public static final Path ROOT_DIR = Paths.get("/tmp/opencga-server-FileWSServerTest-folder");

    public FileWSServerTest() {
    }

    void setWebTarget(WebTarget webTarget) {
        this.webTarget = webTarget;
    }

    @Rule
    public ExpectedException thrown = ExpectedException.none();

    @BeforeClass
    static public void initServer() throws Exception {
//        System.setProperty(org.slf4j.impl.SimpleLogger.DEFAULT_LOG_LEVEL_KEY, "debug");
        serverTestUtils = new WSServerTestUtils();
        serverTestUtils.setUp();
        serverTestUtils.initServer();
    }

    @AfterClass
    static public void shutdownServer() throws Exception {
        serverTestUtils.shutdownServer();
    }

    @Before
    public void init() throws Exception {
        webTarget = serverTestUtils.getWebTarget();
        sessionId = OpenCGAWSServer.catalogManager.getUserManager().login("user", CatalogManagerTest.PASSWORD).getToken();

        if (ROOT_DIR.toFile().exists()) {
            IOUtils.deleteDirectory(ROOT_DIR);
        }
        Files.createDirectory(ROOT_DIR);
        CatalogManagerTest.createDebugFile(ROOT_DIR.resolve("file1.txt").toString());
        CatalogManagerTest.createDebugFile(ROOT_DIR.resolve("file2.txt").toString());
        Files.createDirectory(ROOT_DIR.resolve("data"));
        CatalogManagerTest.createDebugFile(ROOT_DIR.resolve("data").resolve("file2.txt").toString());
        String fileName = "variant-test-file.vcf.gz";
        Files.copy(this.getClass().getClassLoader().getResourceAsStream(fileName), ROOT_DIR.resolve("data").resolve(fileName));
        fileName = "HG00096.chrom20.small.bam";
        Files.copy(this.getClass().getClassLoader().getResourceAsStream(fileName), ROOT_DIR.resolve("data").resolve(fileName));
    }

    @After
    public void after() throws Exception {
        // It is here to avoid restarting the server again and again
        serverTestUtils.setUp();
    }

    @Test
    public void linkFolderTest() throws IOException {

        String path = "data/newFolder"; //Accepts ending or not ending with "/"
        String json = webTarget.path("files").path("link")
                .queryParam("sid", sessionId)
                .queryParam("studyId", studyId)
                .queryParam("path", path)
                .queryParam("parents", true)
                .queryParam("uri", ROOT_DIR.toUri()).request().get(String.class);

        QueryResponse<File> response = WSServerTestUtils.parseResult(json, File.class);
        File file = response.getResponse().get(0).first();
        assertEquals(path + "/" + ROOT_DIR.getFileName() + "/", file.getPath());
        assertEquals(ROOT_DIR.toUri(), file.getUri());

    }

    @Test
    public void linkFileTest() throws IOException {
        URI fileUri = ROOT_DIR.resolve("file1.txt").toUri();
        String json = webTarget.path("files").path("link")
                .queryParam("sid", sessionId)
                .queryParam("studyId", studyId)
                .queryParam("path", "data/")
                .queryParam("uri", fileUri).request().get(String.class);

        QueryResponse<File> response = WSServerTestUtils.parseResult(json, File.class);
        File file = response.getResponse().get(0).first();
        assertEquals("data/file1.txt", file.getPath());
        assertEquals(fileUri, file.getUri());
    }

    @Test
    public void linkFileTest2() throws IOException {
        URI fileUri = ROOT_DIR.resolve("file1.txt").toUri();
        String json = webTarget.path("files").path("link")
                .queryParam("sid", sessionId)
                .queryParam("studyId", studyId)
                .queryParam("path", "data")
                .queryParam("uri", fileUri).request().get(String.class);

        QueryResponse<File> response = WSServerTestUtils.parseResult(json, File.class);
        File file = response.getResponse().get(0).first();
        assertEquals("data/file1.txt", file.getPath());
        assertEquals(fileUri, file.getUri());


        fileUri = ROOT_DIR.resolve("file2.txt").toUri();
        json = webTarget.path("files").path(Long.toString(file.getUid())).path("relink")
                .queryParam("sid", sessionId)
                .queryParam("uri", fileUri).request().get(String.class);

        response = WSServerTestUtils.parseResult(json, File.class);
        file = response.getResponse().get(0).first();
        assertEquals("data/file1.txt", file.getPath());
        assertEquals(fileUri, file.getUri());
    }

    @Test
    public void updateFilePOST() throws Exception {
        File file = OpenCGAWSServer.catalogManager.getFileManager().search(String.valueOf(studyId), new Query(FileDBAdaptor.QueryParams.TYPE
                .key(), "FILE"), new QueryOptions(), sessionId).first();

        ObjectMap params = new ObjectMap(FileDBAdaptor.QueryParams.DESCRIPTION.key(), "Change description");
        String json = webTarget.path("files").path(Long.toString(file.getUid())).path("update")
                .queryParam("sid", sessionId).request().post(Entity.json(params), String.class);

        QueryResponse<Object> response = WSServerTestUtils.parseResult(json, Object.class);
        file = OpenCGAWSServer.catalogManager.getFileManager().get(file.getUid(), null, sessionId).first();
        assertEquals(params.getString(FileDBAdaptor.QueryParams.DESCRIPTION.key()), file.getDescription());
    }

    @Test
    public void searchFiles() throws Exception {
        String json = webTarget.path("files").path("search")
                .queryParam(QueryOptions.INCLUDE, "id,path")
                .queryParam("sid", sessionId)
                .queryParam("studyId", studyId)
                .queryParam("path", "data/").request().get(String.class);

        QueryResponse<File> response = WSServerTestUtils.parseResult(json, File.class);
        File file = response.getResponse().get(0).first();
        assertEquals(1, response.getResponse().get(0).getNumResults());
        assertEquals("data/", file.getPath());

        response = WSServerTestUtils.parseResult(webTarget.path("files").path("user@1000G:phase1:data:").path("update")
                .queryParam(QueryOptions.INCLUDE, "id,path")
                .queryParam("sid", sessionId)
                .request().post(Entity.json(
                                new ObjectMap("attributes",
                                        new ObjectMap("num", 2)
                                                .append("exists", true)
                                                .append("txt", "helloWorld"))),
                        String.class), File.class);
        System.out.println(response.getQueryOptions().toJson());

        WSServerTestUtils.parseResult(webTarget.path("files").path("user@1000G:phase1:analysis:").path("update")
//                .queryParam(QueryOptions.INCLUDE, "projects.studies.files.id,projects.studies.files.path")
                .queryParam("sid", sessionId)
                .request().post(Entity.json(
                                new ObjectMap("attributes",
                                        new ObjectMap("num", 3)
                                                .append("exists", true)
                                                .append("txt", "helloMundo"))),
                        String.class), File.class);

        response = WSServerTestUtils.parseResult(webTarget.path("files").path("search")
                .queryParam(QueryOptions.INCLUDE, "id,path")
                .queryParam(QueryOptions.LIMIT, "5")
                .queryParam("sid", sessionId)
                .queryParam("studyId", studyId)
                .queryParam("attributes.txt", "~hello").request().get(String.class), File.class);
        assertEquals(2, response.getResponse().get(0).getNumResults());

        System.out.println(response.getQueryOptions().toJson());
        response = WSServerTestUtils.parseResult(webTarget.path("files").path("search")
                .queryParam(QueryOptions.INCLUDE, "projects.studies.files.id,projects.studies.files.path")
                .queryParam("sid", sessionId)
                .queryParam("studyId", studyId)
                .queryParam("battributes.exists", true).request().get(String.class), File.class);
        assertEquals(2, response.getResponse().get(0).getNumResults());

        response = WSServerTestUtils.parseResult(webTarget.path("files").path("search")
                .queryParam(QueryOptions.INCLUDE, "projects.studies.files.id,projects.studies.files.path")
                .queryParam("sid", sessionId)
                .queryParam("studyId", studyId)
                .queryParam("nattributes.num", "<3").request().get(String.class), File.class);
        assertEquals(1, response.getResponse().get(0).getNumResults());

        response = WSServerTestUtils.parseResult(webTarget.path("files").path("search")
                .queryParam("bioformat", "NONE,DATAMATRIX_EXPRESSION")
                .queryParam("sid", sessionId)
                .queryParam("studyId", studyId)
                .request().get(String.class), File.class);
        assertEquals(7, response.getResponse().get(0).getNumResults());

        response = WSServerTestUtils.parseResult(webTarget.path("files").path("search")
                .queryParam("bioformat", "NONE")
                .queryParam("sid", sessionId)
                .queryParam("studyId", studyId)
                .request().get(String.class), File.class);
        assertEquals(6, response.getResponse().get(0).getNumResults());


    }

    public File uploadVcf(long studyId, String sessionId) throws IOException, CatalogException {
        String fileName = "variant-test-file.vcf.gz";
//        String fileName = "10k.chr22.phase3_shapeit2_mvncall_integrated_v5.20130502.genotypes.vcf.gz";
        InputStream is = this.getClass().getClassLoader().getResourceAsStream(fileName);
        return upload(studyId, fileName, File.Bioformat.VARIANT, is, sessionId);
    }

    public File uploadBam(long studyId, String sessionId) throws IOException, CatalogException {
        String fileName = "HG00096.chrom20.small.bam";
        InputStream is = this.getClass().getClassLoader().getResourceAsStream(fileName);

        return upload(studyId, fileName, File.Bioformat.ALIGNMENT, is, sessionId);
    }

    public File upload(long studyId, String fileName, File.Bioformat bioformat, InputStream is, String sessionId) throws IOException, CatalogException {
        System.out.println("\nTesting file upload...");
        System.out.println("------------------------");


//        File> queryResult = OpenCGAWSServer.catalogManager.createFile(studyId, File.Format.PLAIN, File.Bioformat.VARIANT, "data/" + fileName, "", true, -1, sessionId);
//        new CatalogFileUtils(OpenCGAWSServer.catalogManager).upload(is, queryResult.first(), sessionId, false, false, true);

//        return OpenCGAWSServer.catalogManager.getFile(queryResult.first().getId(), sessionId).first();


        int totalSize = is.available();
        int bufferSize = Math.min(totalSize/100+10, 100000);
        byte[] buffer = new byte[bufferSize];
        int size;
        int chunk_id = 0;
        String json = null;
        while((size = is.read(buffer)) > 0) {

            MultiPart multiPart = new MultiPart();
            multiPart.setMediaType(MediaType.MULTIPART_FORM_DATA_TYPE);
            multiPart.bodyPart(new StreamDataBodyPart("chunk_content", new ByteArrayInputStream(buffer, 0, size)));
            multiPart.bodyPart(new FormDataBodyPart("chunk_id", Integer.toString(chunk_id)));
            multiPart.bodyPart(new FormDataBodyPart("chunk_size", Integer.toString(size)));
            multiPart.bodyPart(new FormDataBodyPart("chunk_total", Integer.toString(totalSize)));
            multiPart.bodyPart(new FormDataBodyPart("last_chunk", Boolean.toString(is.available() == 0)));
            multiPart.bodyPart(new FormDataBodyPart("filename", fileName));
            multiPart.bodyPart(new FormDataBodyPart("studyId", Long.toString(studyId)));
            multiPart.bodyPart(new FormDataBodyPart("fileFormat", File.Format.PLAIN.toString()));
            multiPart.bodyPart(new FormDataBodyPart("bioFormat", bioformat.toString()));
            multiPart.bodyPart(new FormDataBodyPart("relativeFilePath", "data/" + fileName));
            multiPart.bodyPart(new FormDataBodyPart("parents", "true"));

            json = this.webTarget.path("files").path("upload").queryParam("sid", sessionId)
            .request().post(Entity.entity(multiPart, multiPart.getMediaType()), String.class);

            System.out.println("Chunk id " + chunk_id);
            chunk_id++;
        }
        System.out.println("size = " + size);


        System.out.println("\nJSON RESPONSE");
        System.out.println(json);
        QueryResponse<File> queryResponse = WSServerTestUtils.parseResult(json, File.class);
        assertEquals("Expected [], actual [" + queryResponse.getError() + "]", "", queryResponse.getError());
        File file = queryResponse.getResponse().get(0).first();

        System.out.println("Testing user creation finished");

        return file;

    }


    public Job index(long fileId, String sessionId) throws IOException, CatalogException {
        System.out.println("\nTesting file index...");
        System.out.println("---------------------");
        System.out.println("\nINPUT PARAMS");
        System.out.println("\tsid: " + sessionId);
        System.out.println("\tfileId: " + fileId);
        System.out.println("\t" + VariantStorageOptions.ANNOTATE.key() + ": " + true);

        String json = webTarget.path("files").path(String.valueOf(fileId)).path("index")
                .queryParam("sid", sessionId)
                .request().get(String.class);

        QueryResponse<Job> queryResponse = WSServerTestUtils.parseResult(json, Job.class);
        assertEquals("Expected [], actual [" + queryResponse.getError() + "]", "", queryResponse.getError());
        System.out.println("\nOUTPUT PARAMS");
        Job job = queryResponse.getResponse().get(0).first();

        System.out.println("\nJSON RESPONSE");
        System.out.println(json);

        return job;

    }

    public Job calculateVariantStats(long cohortId, long outdirId, String sessionId) throws IOException, CatalogException {

        String json = webTarget.path("cohorts").path(String.valueOf(cohortId)).path("stats")
                .queryParam("sid", sessionId)
                .queryParam("calculate", true)
                .queryParam("outdirId", outdirId)
                .queryParam("log", "debug")
                .request().get(String.class);

        QueryResponse<Job> queryResponse = WSServerTestUtils.parseResult(json, Job.class);
        assertEquals("Expected [], actual [" + queryResponse.getError() + "]", "", queryResponse.getError());
        System.out.println("\nOUTPUT PARAMS");
        Job job = queryResponse.getResponse().get(0).first();

        System.out.println("\nJSON RESPONSE");
        System.out.println(json);

        return job;

    }

    public List<Variant> fetchVariants(long fileId, String sessionId, QueryOptions queryOptions) throws IOException {
        System.out.println("\nTesting file fetch variants...");
        System.out.println("---------------------");
        System.out.println("\nINPUT PARAMS");
        System.out.println("\tsid: " + sessionId);
        System.out.println("\tfileId: " + fileId);

        WebTarget webTarget = this.webTarget.path("files").path(String.valueOf(fileId)).path("fetch")
                .queryParam("sid", sessionId);
        for (Map.Entry<String, Object> entry : queryOptions.entrySet()) {
            webTarget = webTarget.queryParam(entry.getKey(), entry.getValue());
            System.out.println("\t" + entry.getKey() + ": " + entry.getValue());

        }
        System.out.println("webTarget = " + webTarget);
        String json = webTarget.request().get(String.class);
        System.out.println("json = " + json);


        QueryResponse<Variant> queryResponse = WSServerTestUtils.parseResult(json, Variant.class);
        assertEquals("Expected [], actual [" + queryResponse.getError() + "]", "", queryResponse.getError());
        System.out.println("\nOUTPUT PARAMS");
        List<Variant> variants = queryResponse.getResponse().get(0).getResult();

        System.out.println("\nJSON RESPONSE");
        System.out.println(json);

        return variants;
    }

    public List<ObjectMap> fetchAlignments(long fileId, String sessionId, QueryOptions queryOptions) throws IOException {
        System.out.println("\nTesting file fetch alignments...");
        System.out.println("---------------------");
        System.out.println("\nINPUT PARAMS");
        System.out.println("\tsid: " + sessionId);
        System.out.println("\tfileId: " + fileId);

        WebTarget webTarget = this.webTarget.path("files").path(String.valueOf(fileId)).path("fetch")
                .queryParam("sid", sessionId);

        for (Map.Entry<String, Object> entry : queryOptions.entrySet()) {
            webTarget = webTarget.queryParam(entry.getKey(), entry.getValue());
            System.out.println("\t" + entry.getKey() + ": " + entry.getValue());
        }

        System.out.println("webTarget = " + webTarget);
        String json = webTarget.request().get(String.class);
        System.out.println("json = " + json);

        QueryResponse<ObjectMap> queryResponse = WSServerTestUtils.parseResult(json, ObjectMap.class);
        assertEquals("Expected [], actual [" + queryResponse.getError() + "]", "", queryResponse.getError());
        System.out.println("\nOUTPUT PARAMS");
        assertEquals("", queryResponse.getError());
        List<ObjectMap> alignments = queryResponse.getResponse().get(0).getResult();

        System.out.println("\nJSON RESPONSE");
        System.out.println(json);

        return alignments;
    }

}
