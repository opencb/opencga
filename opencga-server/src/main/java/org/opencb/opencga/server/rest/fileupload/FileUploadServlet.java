package org.opencb.opencga.server.rest.fileupload;

import org.glassfish.jersey.server.ParamException;
import org.opencb.commons.datastore.core.Event;
import org.opencb.commons.datastore.core.ObjectMap;
import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.opencga.catalog.exceptions.CatalogException;
import org.opencb.opencga.catalog.managers.CatalogManager;
import org.opencb.opencga.core.response.OpenCGAResult;
import org.opencb.opencga.core.response.RestResponse;
import org.opencb.opencga.server.rest.OpenCGAWSServer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.servlet.ServletException;
import javax.servlet.ServletOutputStream;
import javax.servlet.annotation.MultipartConfig;
import javax.servlet.annotation.WebServlet;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import javax.servlet.http.Part;
import javax.ws.rs.core.Response;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * MultipartConfig annotation supports the following optional attributes: location: An absolute path to a directory on the file system. The
 * location attribute does not support a path relative to the application context. This location is used to store files temporarily while
 * the parts are processed or when the size of the file exceeds the specified fileSizeThreshold setting. The default location is "".
 * <p>
 * fileSizeThreshold: The file size in bytes after which the file will be temporarily stored on disk. The default size is 0 bytes.
 * <p>
 * MaxFileSize: The maximum size allowed for uploaded files, in bytes. If the size of any uploaded file is greater than this size, the web
 * container will throw an exception (IllegalStateException). The default size is unlimited.
 * <p>
 * maxRequestSize: The maximum size allowed for a multipart/form-data request, in bytes. The web container will throw an exception if the
 * overall size of all uploaded files exceeds this threshold. The default size is unlimited.
 */

@WebServlet(name = "/FileUploadServlet", description = "This is my first annotated servlet",
        urlPatterns = "/webservices/UploadFileServlet/*")
@MultipartConfig(fileSizeThreshold = 1024 * 1024 * 2)   // 2MB size after which the file will be temporarily stored on disk
public class FileUploadServlet extends HttpServlet {
    /**
     * Name of the directory where uploaded files will be saved, relative to the web application directory.
     */
    private static final int DEFAULT_BUFFER_SIZE = 1024;

    private long startTime;
    private Logger logger = LoggerFactory.getLogger(this.getClass().toString());

    protected void doGet(HttpServletRequest request,
                         HttpServletResponse response) throws ServletException, IOException {
        logger.info("doGet :::: " + request);
        doPost(request, response);
    }

    protected void doPut(HttpServletRequest request, HttpServletResponse response)
            throws ServletException, IOException {
        logger.info("doPut :::: " + request);
        doPost(request, response);
    }

    /**
     * handles file upload
     */
    protected void doPost(HttpServletRequest request,
                          HttpServletResponse response) throws ServletException, IOException {
        // gets absolute path of the web application
        OpenCGAResult<org.opencb.opencga.core.models.file.File> result = null;
        // take the time for calculating the whole duration of the call
        startTime = System.currentTimeMillis();
        try {
            String token = request.getHeader("Authorization").replaceAll("Bearer ", "");
            String studyId = request.getHeader("study");
            String path = request.getHeader("relativeFilePath");
            logger.info("Trying to update file for study " + studyId);
            CatalogManager catalogManager = new CatalogManager(OpenCGAWSServer.getConfiguration());
            catalogManager.getStudyManager().get(studyId, QueryOptions.empty(), token);

            // Temporal folder
            Path temp = Files.createTempDirectory("temp");
            String savePath = temp + File.separator;

            // File source

            for (Part part : request.getParts()) {

                String fileName = extractFileName(part);
                // refines the fileName in case it is an absolute path
                File file = new File(savePath + fileName);
                InputStream fileContent = part.getInputStream();
                copyInputStreamToFile(fileContent, file);
                logger.info("File uploaded " + file.getAbsolutePath());
                result = catalogManager.getFileManager().moveAndRegister(studyId,
                        file.toPath(), null, path != null ? path : "/", token);
                logger.info("Registered file " + file.getName() + " to " + studyId);
            }

            response.setContentType("text/plain;charset=UTF-8");
            ServletOutputStream sout = response.getOutputStream();
            sout.print("File upload success");
        } catch (CatalogException e) {
            response.setContentType("text/plain;charset=UTF-8");
            ServletOutputStream sout = response.getOutputStream();
            sout.print("File upload fail");
        }
    }

    private void copyInputStreamToFile(InputStream inputStream, File file)
            throws IOException {

        // append = false
        try (FileOutputStream outputStream = new FileOutputStream(file, false)) {
            int read;
            byte[] bytes = new byte[DEFAULT_BUFFER_SIZE];
            while ((read = inputStream.read(bytes)) != -1) {
                outputStream.write(bytes, 0, read);
            }
        }
    }

    private Response createErrorResponse(CatalogException e, String uri) {
        RestResponse<ObjectMap> dataResponse = new RestResponse<>();
        //dataResponse.setApiVersion(apiVersion);
        //dataResponse.setParams(params);
        addErrorEvent(dataResponse, e);
        OpenCGAResult result = new OpenCGAResult();
        OpenCGAWSServer.setFederationServer(result, uri);
        dataResponse.setResponses(Arrays.asList(result));

        Response response =
                Response.fromResponse(OpenCGAWSServer.createJsonResponse(dataResponse)).status(Response.Status.INTERNAL_SERVER_ERROR).build();
        return response;
    }

    private static <T> void addErrorEvent(RestResponse<T> response, Throwable e) {
        if (response.getEvents() == null) {
            response.setEvents(new ArrayList<>());
        }
        String message;
        if (e instanceof ParamException.QueryParamException && e.getCause() != null) {
            message = e.getCause().getMessage();
        } else {
            message = e.getMessage();
        }
        response.getEvents().add(
                new Event(Event.Type.ERROR, 0, e.getClass().getName(), e.getClass().getSimpleName(), message));
    }

    /**
     * Extracts file name from HTTP header content-disposition
     */
    private String extractFileName(Part part) {
        String contentDisp = part.getHeader("content-disposition");
        String[] items = contentDisp.split(";");
        for (String s : items) {
            if (s.trim().startsWith("filename")) {
                return s.substring(s.indexOf("=") + 2, s.length() - 1);
            }
        }
        return "";
    }

    protected Response createOkResponse(Object obj, String uri) {
        RestResponse queryResponse = new RestResponse();
        queryResponse.setTime(new Long(System.currentTimeMillis() - startTime).intValue());
        //  queryResponse.setApiVersion(apiVersion);
        //  queryResponse.setParams(params);
        //  queryResponse.setEvents(events);

        // Guarantee that the RestResponse object contains a list of results
        List<OpenCGAResult<?>> list = new ArrayList<>();

        if (obj instanceof OpenCGAResult) {
            list.add(((OpenCGAResult) obj));
        }

        for (OpenCGAResult<?> openCGAResult : list) {
            OpenCGAWSServer.setFederationServer(openCGAResult, uri);
        }
        queryResponse.setResponses(list);

        Response response = OpenCGAWSServer.createJsonResponse(queryResponse);

        return response;
    }
}