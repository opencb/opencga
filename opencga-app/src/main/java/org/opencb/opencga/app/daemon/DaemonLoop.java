package org.opencb.opencga.app.daemon;

import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.servlet.ServletContextHandler;
import org.eclipse.jetty.servlet.ServletHolder;
import org.glassfish.jersey.server.ResourceConfig;
import org.glassfish.jersey.servlet.ServletContainer;
import org.opencb.datastore.core.ObjectMap;
import org.opencb.datastore.core.QueryOptions;
import org.opencb.datastore.core.QueryResult;
import org.opencb.opencga.analysis.AnalysisFileIndexer;
import org.opencb.opencga.catalog.CatalogManager;
import org.opencb.opencga.catalog.beans.File;
import org.opencb.opencga.catalog.beans.Index;
import org.opencb.opencga.catalog.beans.Job;
import org.opencb.opencga.catalog.db.CatalogManagerException;
import org.opencb.opencga.catalog.io.CatalogIOManager;
import org.opencb.opencga.catalog.io.CatalogIOManagerException;
import org.opencb.opencga.lib.SgeManager;
import org.opencb.opencga.lib.common.Config;
import org.opencb.opencga.lib.common.TimeUtils;
import org.slf4j.LoggerFactory;

import org.slf4j.Logger;

import java.io.IOException;
import java.net.URI;
import java.nio.file.*;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.LinkedList;
import java.util.List;
import java.util.Properties;

/**
 * Created by jacobo on 23/10/14.
 */
public class DaemonLoop implements Runnable {

    public static final String PORT     = "OPENCGA.APP.DAEMON.PORT";
    public static final String SLEEP    = "OPENCGA.APP.DAEMON.SLEEP";
    public static final String USER     = "OPENCGA.APP.DAEMON.USER";
    public static final String PASSWORD = "OPENCGA.APP.DAEMON.PASSWORD";

    private final Properties properties;

    private Server server;
    private Thread thread;
    private boolean exit = false;
    private CatalogManager catalogManager;

    private static Logger logger = LoggerFactory.getLogger(DaemonLoop.class);
    private final AnalysisFileIndexer analysisFileIndexer;
    private String sessionId;

    public DaemonLoop(Properties properties) {
        this.properties = properties;
        try {
            catalogManager = new CatalogManager(Config.getCatalogProperties());
        } catch (IOException | CatalogIOManagerException | CatalogManagerException e) {
            e.printStackTrace();
        }

        analysisFileIndexer = new AnalysisFileIndexer(catalogManager, Config.getAnalysisProperties());

        int port = Integer.parseInt(properties.getProperty(DaemonLoop.PORT, "61976"));

        ResourceConfig resourceConfig = new ResourceConfig();
        resourceConfig.packages(true, "org.opencb.opencga.app.daemon.rest");
        ServletContainer sc = new ServletContainer(resourceConfig);
        ServletHolder sh = new ServletHolder(sc);

        logger.info("Server in port : {}", port);
        server = new Server(port);
        ServletContextHandler context = new ServletContextHandler(server, null, ServletContextHandler.SESSIONS);
        context.addServlet(sh, "/opencga/rest/*");

        thread = new Thread(this);
    }


    @Override
    public void run() {
        int sleep = Integer.parseInt(properties.getProperty(SLEEP, "4000"));
        sessionId = null;
        try {
            QueryResult<ObjectMap> login = catalogManager.login(properties.getProperty(USER), properties.getProperty(PASSWORD), "daemon");
            sessionId = login.getResult().get(0).getString("sessionId");
        } catch (CatalogManagerException | IOException e) {
            e.printStackTrace();
            exit = true;
        }

        while(!exit) {
            try {
                Thread.sleep(sleep);
            } catch (InterruptedException e) {
                if(!exit) {
                    e.printStackTrace();
                }
            }
            logger.info("----- WakeUp {} -----", TimeUtils.getTimeMillis());

            logger.info("----- Pending jobs -----");
            try {
                QueryResult<Job> unfinishedJobs = catalogManager.getUnfinishedJobs(sessionId);
                for (Job job : unfinishedJobs.getResult()) {
//                    System.out.println("job = " + job);
                    String status = SgeManager.status("*"+job.getName());
                    System.out.println("job : {id: " + job.getId() + ", status: '" + job.getStatus() + "', name: '" + job.getName() + "'}, sgeStatus : " + status);
                    switch(status) {
                        case SgeManager.FINISHED:
                            //TODO: Finish job
                            finishJob(job);
                            break;
                        case SgeManager.ERROR:
                        case SgeManager.EXECUTION_ERROR:
                            //TODO: Handle error
                            break;
                        case SgeManager.QUEUED:
                            catalogManager.modifyJob(job.getId(), new ObjectMap("status", Job.QUEUED), sessionId);
                            break;
                        case SgeManager.RUNNING:
                            catalogManager.modifyJob(job.getId(), new ObjectMap("status", Job.RUNNING), sessionId);
                            break;
                        case SgeManager.TRANSFERRED:
                            break;
                        case SgeManager.UNKNOWN:
                            break;
                    }
                }
            } catch (Exception e) {
                e.printStackTrace();
            }

            logger.info("----- Pending index -----");
            try {
                QueryResult<File> files = catalogManager.searchFile(-1, new QueryOptions("indexState", Index.PENDING), sessionId);
                for (File file : files.getResult()) {
                    System.out.println("file = " + file);
                    for (Index index : file.getIndices()) {
                        if(index.getState().equals(Index.PENDING)) {
                            switch(SgeManager.status("*"+index.getJobId())) {
                                case SgeManager.FINISHED:
                                    analysisFileIndexer.finishIndex(index.getJobId(), sessionId);
                                    break;
                                case SgeManager.EXECUTION_ERROR:
                                    break;
                                case SgeManager.ERROR:
                                    //TODO: Handle error
                                    break;
                                case SgeManager.UNKNOWN:
                                case SgeManager.QUEUED:
                                case SgeManager.RUNNING:
                                case SgeManager.TRANSFERRED:
                                    break;
                            }

                        }
                    }
                }
            } catch (Exception e) {
                e.printStackTrace();
            }
        }

        if(sessionId != null) {
            try {
                catalogManager.logout(properties.getProperty(USER), sessionId);
            } catch (CatalogManagerException | IOException e) {
                e.printStackTrace();
            }
            sessionId = null;
        }

        try {
            Thread.sleep(200);
            server.stop();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    /**
     *
     * 1º Read generated files.
     * 2º Add generated files to catalog. Status: File.UPLOADING
     * 3º Calculate checksum
     * 4º Add checksum to catalog
     * 5º Copy
     * 6º Calculate checksum
     * 7º Compare.
     *      If equals, delete and status: File.READY
     *      Else, pray
     */
    private void finishJob(final Job job) {
        final URI outDirUri;
        final URI tmpOutdirUri;
        final int studyId = 1;
        List<Integer> fileIds = null;
        try {
            File tmpDir = catalogManager.getFile(job.getTmpOutDirId(), new QueryOptions("path", true), sessionId).getResult().get(0);
            tmpOutdirUri = catalogManager.getFileUri(tmpDir);
            File outDir = catalogManager.getFile(job.getOutDirId(), new QueryOptions("path", true), sessionId).getResult().get(0);
            outDirUri = catalogManager.getFileUri(outDir);
        } catch (CatalogIOManagerException | IOException | CatalogManagerException e) {
            e.printStackTrace();
            return;
        }

        try {//1º Read generated files.
            //CatalogIOManager catalogIOManager = catalogManager.getCatalogIOManagerFactory().get(tmpOutdirUri.getScheme());
            switch (tmpOutdirUri.getScheme()) {
                case "file": {
                    fileIds = walkFileTree(job, outDirUri, tmpOutdirUri, studyId);
                    break;
                }
                default:
                    System.out.println("Unsupported scheme");
                    return;
            }
        } catch (IOException e) {
            e.printStackTrace();
        }

        try {
            ObjectMap parameters = new ObjectMap("status", Job.READY);
            parameters.put("output", fileIds);
            parameters.put("endTime", System.currentTimeMillis());
            catalogManager.modifyJob(job.getId(), parameters, sessionId);
        } catch (CatalogManagerException e) {
            e.printStackTrace(); //TODO: Handle exception
        }
    }

    private List<Integer> walkFileTree(final Job job, final URI outDirUri, final URI tmpOutdirUri, final int studyId) throws IOException {
        final List<Integer> fileIds;
        fileIds = new LinkedList<>();
        FileVisitor<Path> fileVisitor = new SimpleFileVisitor<Path>() {
            @Override
            public FileVisitResult visitFile(Path file, BasicFileAttributes attrs) throws IOException {

                String generatedFile = file.toAbsolutePath().toString().substring(tmpOutdirUri.getPath().length());
                int fileId = addResultFile(job, generatedFile, tmpOutdirUri, outDirUri, studyId, attrs);
                fileIds.add(fileId);

                return super.visitFile(file, attrs);
            }
        };
        Files.walkFileTree(Paths.get(tmpOutdirUri.getPath()), fileVisitor);
        Files.delete(Paths.get(tmpOutdirUri));    //TODO: Check empty folder!
        return fileIds;
    }

    /**
     *
     *
     * @param job            Job
     * @param generatedFile  Generated file path
     * @param originOutDir   Original ourDir where files were created.
     * @param targetOutDir   Destination folder URI.
     * @param studyId        StudyID
     * @param attrs          File attributes
     * @return               new FileID
     *
     * @throws IOException
     */
    private int addResultFile(Job job, String generatedFile , URI originOutDir, URI targetOutDir, int studyId, BasicFileAttributes attrs)
            throws IOException {
//        System.out.println("DaemonLoop.addResultFile");
//        System.out.println("job = [" + job + "], generatedFile = [" + generatedFile + "], originOutDir = [" + originOutDir + "], targetOutDir = [" + targetOutDir + "], studyId = [" + studyId + "], attrs = [" + attrs + "]");

        URI originFileUri = originOutDir.resolve(generatedFile);
        URI targetFileUri = targetOutDir.resolve(generatedFile);

        final CatalogIOManager originIOManager;
        final CatalogIOManager destIOManager;
        try {
            originIOManager = catalogManager.getCatalogIOManagerFactory().get(originOutDir.getScheme());
            destIOManager = catalogManager.getCatalogIOManagerFactory().get(targetOutDir.getScheme());
        } catch (CatalogIOManagerException e) {
            e.printStackTrace();
            return -1;
        }

        //2º Add generated files to catalog. Status: File.UPLOADING
        final File catalogFile;
        try {
            String filePath = Paths.get(job.getOutDir(), generatedFile).toString();
            QueryResult<File> result = catalogManager.createFile(studyId, File.FILE, "txt", filePath,
                    "Generated from job " + job.getId(), true, job.getId(), sessionId);
            catalogFile = result.getResult().get(0);
        } catch (CatalogManagerException | CatalogIOManagerException | InterruptedException e) {
            e.printStackTrace();
            return -1;
        }

        //3º Calculate checksum
        final String checksum;
        try {
            checksum = originIOManager.calculateChecksum(originFileUri);
        } catch (CatalogIOManagerException e) {
            e.printStackTrace();
            return -1;
        }

        //4º Add checksum to catalog
        try {
            QueryOptions parameters = new QueryOptions();
            parameters.put("jobId", job.getId());
            parameters.put("diskUsage", attrs.size());
            parameters.put("attributes", new ObjectMap("checksum", checksum));
            catalogManager.modifyFile(catalogFile.getId(), parameters, sessionId);
        } catch (CatalogManagerException e) {
            e.printStackTrace();
            return -1;
        }

        //5º Copy   //TODO: Copy with the multi_FS_Manager!
        Files.copy(Paths.get(originFileUri), Paths.get(targetFileUri));


        //6º Calculate checksum
        final String checksumDest;
        try {
            checksumDest = destIOManager.calculateChecksum(targetFileUri);
        } catch (CatalogIOManagerException e) {
            e.printStackTrace();
            return -1;
        }

        //7º Compare
        if (checksum.equals(checksumDest)) {
            logger.info("Checksum matches. Deleting origin file.");
            logger.info(checksum + " == " + checksumDest);
            originIOManager.deleteFile(originFileUri);
            try {
                QueryOptions parameters = new QueryOptions("status", File.READY);
                catalogManager.modifyFile(catalogFile.getId(), parameters, sessionId);
            } catch (CatalogManagerException e) {
                e.printStackTrace();
                return -1;
            }
        } else {
            System.out.println("Checksum mismatches!");
            return -1;
        }

        return catalogFile.getId();
    }

    public void start() throws Exception {
        //Start services
        server.start();
        thread.start();
    }

    public int join() {
        //Join services
        try {
            logger.info("Join to Server");
            server.join();
            logger.info("Join to Thread");
            thread.join();
        } catch (InterruptedException e) {
            e.printStackTrace();
            return 2;
        }
        return 0;
    }

    synchronized public void stop() {
        exit = true;
        thread.interrupt();
    }
}
