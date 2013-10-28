package org.opencb.opencga.server;

import java.io.IOException;

import javax.servlet.http.HttpServletRequest;
import javax.ws.rs.DefaultValue;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.UriInfo;

import org.opencb.opencga.storage.datamanagers.bam.BamManager;

@Deprecated
@Path("/bam")
public class BamWSServer extends GenericWSServer {


    public BamWSServer(@Context UriInfo uriInfo, @Context HttpServletRequest httpServletRequest) throws IOException {
        super(uriInfo, httpServletRequest);
    }

    @GET
    @Path("/{filename}/{region}/region")
//	public Response getByRegion(final File inputSamOrBamFile, final File indexFile, final File outputSamOrBamFile) {
    public Response getByRegion(
            @DefaultValue("") @PathParam("filename") String filename,
            @DefaultValue("") @PathParam("region") String region
    ) throws IOException {

//		Boolean viewAsPairs = false;
//		if(params.get("view_as_pairs") != null){
//			viewAsPairs = true;
//		}
//		Boolean showSoftclipping = false;
//		if(params.get("show_softclipping") != null){
//			showSoftclipping = true;
//		}
//		
//		String chr = null;
//		int start = 0;
//		int end = 0;
//		
//		if(filename == ""){
//			return 	createErrorResponse("ERROR: filename path param is empty.");
//		}
//		
//		//comprobar si existe el fichero 
//		
//		if(region == "") {
//			return createErrorResponse("ERROR: region path param is empty.");
//		}
//		Region parsedRegion = Region.parseRegion(region);
//		if(parsedRegion == null){
//			return createErrorResponse("ERROR: region format not valid.");
//		}
//		
//		chr = parsedRegion.getChromosome();
//		start = parsedRegion.getStart();
//		end =parsedRegion.getEnd();
//		
//		BamManager bamManager = new BamManager();
//		
//		String filePath = config.getProperty("FILES.PATH");
//		String result = bamManager.getByRegion(filePath, filename, viewAsPairs, showSoftclipping, chr, start, end);
//		
//		return createOkResponse(result);
        return createOkResponse("");
    }

    @GET
    @Path("/list")
    public Response getFileList() throws IOException {
        String filePath = properties.getProperty("FILES.PATH");
        return createOkResponse(new BamManager().getFileList(filePath));
    }


    //TODO
//	public void testGetByRegion() {
////		File inputSamFile = new File("/tmp/input2.bam");
////		File indexFile = new File("/tmp/input2.bam.bai");
//		File outputSamFile = new File("/tmp/output.bam");
//
//		File inputSamFile = new File("/home/fsalavert/p01_pair1.remdup_bwa_bwa_cnag_mapped.sorted_rgfix.bam");
//		File indexFile = new File("/home/fsalavert/p01_pair1.remdup_bwa_bwa_cnag_mapped.sorted_rgfix.bam.bai");
//		
//		System.out.println("checking file: " + inputSamFile);
//		try {
//			FileUtils.checkFile(inputSamFile);
//			System.out.println("file " + inputSamFile + " exists");
//		} catch (IOException e) {
//			System.out.println("file " + inputSamFile + " not exists");
//			e.printStackTrace();
//		}					
//
//		long lStartTime = System.currentTimeMillis();
//		exampleSamUsage.getByRegion(inputSamFile, indexFile, outputSamFile);
//		long lEndTime = System.currentTimeMillis();
//		
//		long difference = lEndTime - lStartTime; //check different
//		System.out.println("done in "+ difference + " milliseconds");
//	}
}
