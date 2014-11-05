package org.opencb.opencga.analysis;

import org.opencb.datastore.core.ObjectMap;
import org.opencb.datastore.core.QueryOptions;
import org.opencb.datastore.core.QueryResult;
import org.opencb.opencga.catalog.CatalogManager;
import org.opencb.opencga.catalog.beans.File;
import org.opencb.opencga.catalog.beans.Index;
import org.opencb.opencga.catalog.beans.Job;
import org.opencb.opencga.catalog.db.CatalogManagerException;
import org.opencb.opencga.catalog.io.CatalogIOManager;
import org.opencb.opencga.catalog.io.CatalogIOManagerException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.URI;
import java.nio.file.*;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.LinkedList;
import java.util.List;

/**
 * Created by jacobo on 4/11/14.
 *
 *  Scans the output directory from a job or index to find all files.
 *  Records the output in CatalogManager
 *  Moves the file to the read output
 */

public class AnalysisOutputRecorder {


    private static Logger logger = LoggerFactory.getLogger(AnalysisOutputRecorder.class);
    private final CatalogManager catalogManager;
    private final String sessionId;

    public AnalysisOutputRecorder(CatalogManager catalogManager, String sessionId) {
        this.catalogManager = catalogManager;
        this.sessionId = sessionId;
    }


    /**
     *
     */
    public void recordJobOutput(final Job job) {
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
//                    fileIds = __walkFileTree(job, outDirUri, tmpOutdirUri, studyId);
                    fileIds = walkFileTree(new JobFileVisitor(job, outDirUri, tmpOutdirUri, studyId), tmpOutdirUri);
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



    public void recordIndexOutput(Index index) throws CatalogManagerException, IOException, CatalogIOManagerException {
        QueryResult<File> fileResult = catalogManager.getFileByIndexJobId(index.getJobId()); //TODO: sessionId¿?¿?
        if(fileResult.getResult().isEmpty()) {
            return;
        }
        File file = fileResult.getResult().get(0);
        int studyId = catalogManager.getStudyIdByFileId(file.getId());
        List<Integer> fileIds = null;

        //6º Find files
        try {
            File outDir = catalogManager.getFile(index.getOutDir(), sessionId).getResult().get(0);
            URI outDirUri = catalogManager.getFileUri(file);
            URI tmpOutdirUri = URI.create(index.getTmpOutDirUri());
            String scheme = tmpOutdirUri.getScheme();
            if (scheme == null) {
                logger.info("Using 'file://' as default scheme. " + index);
                scheme = "file";
            }
            switch (scheme) {
                case "file": {
//                    fileIds = __walkFileTree(index, outDirUri, tmpOutdirUri, studyId);
                    fileIds = walkFileTree(new IndexFileVisitor(index, outDirUri, tmpOutdirUri, studyId), tmpOutdirUri);
                    break;
                }
                default:
                    System.out.println("Unsupported scheme");
                    return;
            }
        } catch (IOException e) {
            e.printStackTrace();
        }

        //7º Update file.attributes
        for (Index auxIndex: file.getIndices()) {
            if (auxIndex.getJobId().equals(index.getJobId())) {
                auxIndex.setJobId("");
                auxIndex.setState(Index.INDEXED);
                auxIndex.setOutput(fileIds);
                catalogManager.setIndexFile(file.getId(), auxIndex.getBackend(), auxIndex, sessionId);
            }
        }
    }

    private List<Integer> walkFileTree(FileVisitorRecorder fileVisitor, URI tmpOutdirUri) throws IOException {
        Files.walkFileTree(Paths.get(tmpOutdirUri.getPath()), fileVisitor);
//        Files.delete(Paths.get(tmpOutdirUri));    //TODO: Check empty folder!

        return fileVisitor.getFileIds();
    }


    abstract class FileVisitorRecorder extends SimpleFileVisitor<Path> {
        abstract List<Integer> getFileIds();
    }
    class IndexFileVisitor  extends FileVisitorRecorder {
        private List<Integer> fileIds = new LinkedList<>();
        private Index index;
        private URI outDirUri;
        private URI tmpOutdirUri;
        private int studyId;

        IndexFileVisitor(Index index, URI outDirUri, URI tmpOutdirUri, int studyId) {
            this.index = index;
            this.outDirUri = outDirUri;
            this.tmpOutdirUri = tmpOutdirUri;
            this.studyId = studyId;
        }

        @Override
        public FileVisitResult visitFile(Path file, BasicFileAttributes attrs) throws IOException {

            String generatedFile = file.toAbsolutePath().toString().substring(tmpOutdirUri.getPath().length());

            int fileId = addResultFile(generatedFile, tmpOutdirUri, outDirUri, studyId, attrs, -1,
                    "Generated from index " + index.getJobId(), index.getOutDirName(), index.getUserId());

            fileIds.add(fileId);

            return super.visitFile(file, attrs);
        }

        @Override
        public List<Integer> getFileIds() {
            return fileIds;
        }
    }

    class JobFileVisitor extends FileVisitorRecorder {
        private List<Integer> fileIds = new LinkedList<>();
        private Job job;
        private URI outDirUri;
        private URI tmpOutdirUri;
        private int studyId;

        JobFileVisitor(Job job, URI outDirUri, URI tmpOutdirUri, int studyId) {
            this.job = job;
            this.outDirUri = outDirUri;
            this.tmpOutdirUri = tmpOutdirUri;
            this.studyId = studyId;
        }

        @Override
        public FileVisitResult visitFile(Path file, BasicFileAttributes attrs) throws IOException {

            String generatedFile = file.toAbsolutePath().toString().substring(tmpOutdirUri.getPath().length());
            int fileId = addResultFile(generatedFile, tmpOutdirUri, outDirUri, studyId, attrs, job.getId(),
                    "Generated from job " + job.getId() , job.getOutDir(), job.getUserId());
            fileIds.add(fileId);

            return super.visitFile(file, attrs);
        }

        @Override
        public List<Integer> getFileIds() {
            return fileIds;
        }
    }



    /**
     * 1º Get generated file
     * 2º Add generated files to catalog. Status: File.UPLOADING
     * 3º Calculate checksum
     * 4º Add checksum to catalog
     * 5º Copy
     * 6º Calculate checksum
     * 7º Compare.
     *      If equals, delete and status: File.READY
     *      Else, repeat
     *
     * @param generatedFile  Generated file path
     * @param originOutDir   Original ourDir where files were created.
     * @param targetOutDir   Destination folder URI.
     * @param studyId        StudyID
     * @param attrs          File attributes
     * @return               new FileID
     *
     * @throws IOException
     */
    private int addResultFile(String generatedFile , URI originOutDir, URI targetOutDir, int studyId, BasicFileAttributes attrs,
                              int jobId, String description, String relativeOutdir, String userId)
            throws IOException {
        System.out.println("generatedFile = [" + generatedFile + "], originOutDir = [" + originOutDir + "], targetOutDir = ["
                + targetOutDir + "], studyId = [" + studyId + "], attrs = [" + attrs + "], jobId = [" + jobId + "], description = ["
                + description + "], relativeOutdir = [" + relativeOutdir + "], userId = [" + userId + "]");

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
            String filePath = Paths.get(relativeOutdir, generatedFile).toString();
            QueryResult<File> result = catalogManager.createFile(studyId, File.FILE, "", filePath,
                            description, true, jobId, sessionId);
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
            ObjectMap parameters = new ObjectMap();
            parameters.put("jobId", jobId);
            parameters.put("creatorId", userId);
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




}
