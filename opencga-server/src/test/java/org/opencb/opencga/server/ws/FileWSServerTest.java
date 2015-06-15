package org.opencb.opencga.server.ws;

import org.glassfish.jersey.media.multipart.FormDataBodyPart;
import org.glassfish.jersey.media.multipart.MultiPart;
import org.glassfish.jersey.media.multipart.file.StreamDataBodyPart;
import org.opencb.biodata.models.variant.Variant;
import org.opencb.datastore.core.QueryOptions;
import org.opencb.datastore.core.QueryResponse;
import org.opencb.datastore.core.QueryResult;
import org.opencb.opencga.analysis.AnalysisExecutionException;
import org.opencb.opencga.catalog.exceptions.CatalogException;
import org.opencb.opencga.catalog.models.File;
import org.opencb.opencga.catalog.models.Job;

import javax.ws.rs.client.Entity;
import javax.ws.rs.client.WebTarget;
import javax.ws.rs.core.MediaType;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.assertEquals;

/**
 * Created by jacobo on 13/06/15.
 */
public class FileWSServerTest {

    private final WebTarget webTarget;

    public FileWSServerTest(WebTarget webTarget) {
        this.webTarget = webTarget;
    }

    public File uploadVcf(int studyId, String sessionId) throws IOException, CatalogException {
        String fileName = "variant-test-file.vcf.gz";
//        String fileName = "10k.chr22.phase3_shapeit2_mvncall_integrated_v5.20130502.genotypes.vcf.gz";
        InputStream is = this.getClass().getClassLoader().getResourceAsStream(fileName);
        return upload(studyId, fileName, File.Bioformat.VARIANT, is, sessionId);
    }

    public File uploadBam(int studyId, String sessionId) throws IOException, CatalogException {
        String fileName = "HG00096.chrom20.small.bam";
        InputStream is = this.getClass().getClassLoader().getResourceAsStream(fileName);

        return upload(studyId, fileName, File.Bioformat.ALIGNMENT, is, sessionId);
    }

    public File upload(int studyId, String fileName, File.Bioformat bioformat, InputStream is, String sessionId) throws IOException, CatalogException {
        System.out.println("\nTesting file upload...");
        System.out.println("------------------------");


//        QueryResult<File> queryResult = OpenCGAWSServer.catalogManager.createFile(studyId, File.Format.PLAIN, File.Bioformat.VARIANT, "data/" + fileName, "", true, -1, sessionId);
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
            multiPart.bodyPart(new FormDataBodyPart("studyId", Integer.toString(studyId)));
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
        QueryResponse<QueryResult<File>> queryResponse = WSServerTestUtils.parseResult(json, File.class);
        assertEquals("Expected [], actual [" + queryResponse.getError() + "]", "", queryResponse.getError());
        File file = queryResponse.getResponse().get(0).first();

        System.out.println("Testing user creation finished");

        return file;

    }


    public Job index(int fileId, String sessionId) throws IOException, AnalysisExecutionException, CatalogException {
        System.out.println("\nTesting file index...");
        System.out.println("---------------------");
        System.out.println("\nINPUT PARAMS");
        System.out.println("\tsid: " + sessionId);
        System.out.println("\tfileId: " + fileId);

        String json = webTarget.path("files").path(String.valueOf(fileId)).path("index")
                .queryParam("sid", sessionId)
                .queryParam("annotate", false)
                .request().get(String.class);

        QueryResponse<QueryResult<Job>> queryResponse = WSServerTestUtils.parseResult(json, Job.class);
        assertEquals("Expected [], actual [" + queryResponse.getError() + "]", "", queryResponse.getError());
        System.out.println("\nOUTPUT PARAMS");
        Job job = queryResponse.getResponse().get(0).first();

        System.out.println("\nJSON RESPONSE");
        System.out.println(json);

        return job;

    }

    public List<Variant> fetchVariants(int fileId, String sessionId, QueryOptions queryOptions) throws IOException {
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


        QueryResponse<QueryResult<Variant>> queryResponse = WSServerTestUtils.parseResult(json, Variant.class);
        assertEquals("Expected [], actual [" + queryResponse.getError() + "]", "", queryResponse.getError());
        System.out.println("\nOUTPUT PARAMS");
        List<Variant> variants = queryResponse.getResponse().get(0).getResult();

        System.out.println("\nJSON RESPONSE");
        System.out.println(json);

        return variants;
    }

}
