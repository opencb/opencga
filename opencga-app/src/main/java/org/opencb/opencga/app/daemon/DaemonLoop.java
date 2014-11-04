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
import org.opencb.opencga.analysis.AnalysisJobExecuter;
import org.opencb.opencga.catalog.CatalogManager;
import org.opencb.opencga.catalog.beans.File;
import org.opencb.opencga.catalog.beans.Index;
import org.opencb.opencga.catalog.beans.Job;
import org.opencb.opencga.catalog.db.CatalogManagerException;
import org.opencb.opencga.catalog.io.CatalogIOManagerException;
import org.opencb.opencga.catalog.io.CatalogIOManagerFactory;
import org.opencb.opencga.lib.SgeManager;
import org.opencb.opencga.lib.common.Config;
import org.opencb.opencga.lib.common.TimeUtils;
import org.slf4j.LoggerFactory;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.nio.file.*;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.HashMap;
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
                            catalogManager.modifyJob(job.getId(), new ObjectMap("status", Job.READY), sessionId);
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
        final Path outDirPath;
        final Path tmpOutdir;
        try {
            File tmpDir = catalogManager.getFile(job.getTmpOutDirId(), new QueryOptions("path", true), sessionId).getResult().get(0);
            tmpOutdir = Paths.get(catalogManager.getFileUri(tmpDir).getPath());
            File outDir = catalogManager.getFile(job.getOutDirId(), new QueryOptions("path", true), sessionId).getResult().get(0);
            outDirPath = Paths.get(outDir.getPath());
        } catch (CatalogIOManagerException | IOException | CatalogManagerException e) {
            e.printStackTrace();
            return;
        }
        FileVisitor <Path> fileVisitor = new SimpleFileVisitor<Path>(){
            @Override
            public FileVisitResult visitFile(Path file, BasicFileAttributes attrs) throws IOException {
                addResultFile(file, job, tmpOutdir, outDirPath, attrs);
                return super.visitFile(file, attrs);
            }
        };

        try {//1º Read generated files.
            Files.walkFileTree(tmpOutdir,fileVisitor);
            Files.delete(tmpOutdir);    //TODO: Check empty folder!
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
    private boolean addResultFile(Path file, Job job, Path originOutDir, Path tarjetOutDir, BasicFileAttributes attrs) throws IOException {
        String generatedFile = file.toString().substring(originOutDir.toString().length() + 1);
        String filePath = tarjetOutDir.resolve(generatedFile).toString();
        CatalogIOManagerFactory catalogIOManagerFactory = catalogManager.getCatalogIOManagerFactory();
        System.out.println("filePath = " + filePath);

        //2º Add generated files to catalog. Status: File.UPLOADING
        QueryResult<File> result;
        try {
            result = catalogManager.createFile(1, File.FILE, "txt", filePath, "Generated from job " + job.getId(), true, job.getId(), sessionId);
        } catch (CatalogManagerException | CatalogIOManagerException | InterruptedException e) {
            System.out.println("e.getMessage() = " + e.getMessage());
            return true;
//                    e.printStackTrace();
        }

        //3º Calculate checksum
        String checksum = calculateChecksum(file);

        //4º Add checksum to catalog
        QueryOptions parameters = new QueryOptions();
        parameters.put("jobId", job.getId());
        parameters.put("diskUsage", attrs.size());
        parameters.put("attributes", new ObjectMap("checksum", checksum));
        try {
            catalogManager.modifyFile(result.getResult().get(0).getId(), parameters, sessionId);
        } catch (CatalogManagerException e) {
            e.printStackTrace();
            return true;
        }

        //5º Copy
        Path target;
        try {
            target = Paths.get(catalogManager.getFileUri(result.getResult().get(0)).getPath());
            Files.copy(file, target);
        } catch (CatalogManagerException | CatalogIOManagerException e) {
            e.printStackTrace();
            return true;
        }

        //6º Calculate checksum
        String checksumDest = calculateChecksum(target);

        //7º Compare
        if (checksum.equals(checksumDest)) {
            logger.info("Checksum matches. Deleting origin file.");
            logger.info(checksum + " == " + checksumDest);
            Files.delete(file);
        } else {
            System.out.println("Checksum mismatches!");
        }
        return false;
    }
    private String calculateChecksum(Path file) throws IOException {
        Process p = Runtime.getRuntime().exec("md5sum " + file.toString());
        BufferedReader br = new BufferedReader(new InputStreamReader(p.getInputStream()));
        String checksum = br.readLine();

        try {
            if (p.waitFor() != 0) {
                //TODO: Handle error in checksum
            }
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        return checksum.split(" ")[0];
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
