package org.opencb.opencga.analysis;

import com.fasterxml.jackson.dataformat.xml.XmlMapper;
import com.fasterxml.jackson.dataformat.xml.annotation.JacksonXmlText;
import org.opencb.datastore.core.ObjectMap;
import org.opencb.datastore.core.QueryOptions;
import org.opencb.datastore.core.QueryResult;
import org.opencb.opencga.analysis.beans.AnalysisBioformatDetect;
import org.opencb.opencga.catalog.CatalogException;
import org.opencb.opencga.catalog.CatalogFileManager;
import org.opencb.opencga.catalog.CatalogManager;
import org.opencb.opencga.catalog.beans.File;
import org.opencb.opencga.catalog.beans.Job;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileInputStream;
import java.io.IOException;
import java.net.URI;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;

/**
 * Created by jacobo on 4/11/14.
 * <p/>
 * Scans the output directory from a job or index to find all files.
 * Records the output in CatalogManager
 * Moves the file to the read output
 */

public class AnalysisOutputRecorder {


    private static Logger logger = LoggerFactory.getLogger(AnalysisOutputRecorder.class);
    private final CatalogManager catalogManager;
    private final String sessionId;
    private final String policy = "delete";
    private final boolean calculateChecksum = false;

    public AnalysisOutputRecorder(CatalogManager catalogManager, String sessionId) {
        this.catalogManager = catalogManager;
        this.sessionId = sessionId;
    }

    public void recordJobOutput(Job job) {

        List<Integer> fileIds = new LinkedList<>();
        CatalogFileManager catalogFileManager = new CatalogFileManager(catalogManager);

        try {
            URI tmpOutDirUri = job.getTmpOutDirUri();
            File outDir = catalogManager.getFile(job.getOutDirId(), new QueryOptions("path", true), sessionId).getResult().get(0);
            List<URI> uris = catalogManager.getCatalogIOManagerFactory().get(tmpOutDirUri).listFiles(tmpOutDirUri);

//            int studyId = catalogManager.getAnalysisIdByJobId(job.getId());
            int studyId = catalogManager.getStudyIdByJobId(job.getId());

//            List<ResultXMLElem> resultXMLElems = new ArrayList<>();
//
//            for (URI uri : uris) {
//                String filename = Paths.get(uri).getFileName().toString();
//                if (filename.equals("result.xml")) {
//                    resultXMLElems = parseResultXML(Paths.get(uri).toFile());
//                }
//            }

            for (URI uri : uris) {

                String generatedFile = Paths.get(uri).toAbsolutePath().toString().substring(tmpOutDirUri.getPath().length());
                String filePath = Paths.get(outDir.getPath(), generatedFile).toString();
                QueryResult<File> searchFile = catalogManager.searchFile(studyId, new QueryOptions("path", filePath), sessionId);
                if (searchFile.getNumResults() != 0) {
                    File file = searchFile.getResult().get(0);
                    switch (policy) {
                        case "delete":
                            catalogManager.deleteFile(file.getId(), sessionId);
                            break;
                        case "rename":
                            throw new UnsupportedOperationException("Unimplemented policy 'rename'");
                        case "doError":
                            throw new UnsupportedOperationException("Unimplemented policy 'error'");
                    }
                }

                QueryResult<File> fileQueryResult;

                String fileName = Paths.get(uri).getFileName().toString();

                File.Bioformat bioformat = File.Bioformat.NONE;

//                ResultXMLElem resultElem;
//                if ((resultElem = getResultElem(resultXMLElems, fileName)) != null) {
//                    bioformat = resultElem.getTag();
//                }

                bioformat = AnalysisBioformatDetect.detect(Paths.get(uri).toString());

                fileQueryResult = catalogManager.createFile(
                        studyId, File.Format.PLAIN, bioformat, filePath, "Generated from job " + job.getId(),
                        true, job.getId(), sessionId);


                File file = fileQueryResult.getResult().get(0);
                fileIds.add(file.getId());
                catalogFileManager.upload(uri, file, null, sessionId, false, false, true, calculateChecksum);
            }
        } catch (CatalogException | IOException e) {
            e.printStackTrace();
            logger.error("Error while processing Job", e);
            return;
        }

        try {
            switch (Job.Type.valueOf(job.getResourceManagerAttributes().get(Job.TYPE).toString())) {
                case INDEX:
                    Integer indexedFileId = (Integer) job.getResourceManagerAttributes().get(Job.INDEXED_FILE_ID);
                    fileIds.add(indexedFileId);
                    ObjectMap parameters = new ObjectMap("status", File.Status.READY);
                    catalogManager.modifyFile(indexedFileId, parameters, sessionId);
                    break;
                case ANALYSIS:
                default:
                    break;
            }
            ObjectMap parameters = new ObjectMap("status", Job.Status.READY);
            parameters.put("output", fileIds);
            parameters.put("endTime", System.currentTimeMillis());
            catalogManager.modifyJob(job.getId(), parameters, sessionId);

            //TODO: "input" files could be modified by the tool. Have to be scanned, calculate the new Checksum and

        } catch (CatalogException e) {
            e.printStackTrace(); //TODO: Handle exception
        }
    }

    private ResultXMLElem getResultElem(List<ResultXMLElem> resultXMLElems, String fileName) {

        for (ResultXMLElem elem : resultXMLElems) {
            if (elem.getType().equals("DATA") && elem.getFileName().equals(fileName)) {
                return elem;
            }
        }

        return null;
    }

    private List<ResultXMLElem> parseResultXML(java.io.File filename) {
        List<ResultXMLElem> list = new ArrayList<>();

        XmlMapper mapper = new XmlMapper();
        try {
            result value = mapper.readValue(new FileInputStream(filename), result.class);

            for (item item : value.output) {
                ResultXMLElem resElem = new ResultXMLElem(item.type, item.value, item.tags);
                list.add(resElem);
            }

        } catch (IOException e) {
            e.printStackTrace();
        }


        return list;
    }

    private static class result {
        public List<item> metadata;
        public List<item> input;
        public List<item> output;

        public result() {
        }

        @Override
        public String toString() {
            return "result{" +
                    "metadata=" + metadata +
                    ", input=" + input +
                    ", output=" + output +
                    '}';
        }
    }


    private static class item {
        public String name;
        public String title;
        public String type;
        public String tags;
        public String style;
        public String group;
        public String context;

        @JacksonXmlText
        public String value;

        public item() {
        }

        @Override
        public String toString() {
            return "item{" +
                    "name='" + name + '\'' +
                    ", title='" + title + '\'' +
                    ", type='" + type + '\'' +
                    ", tags='" + tags + '\'' +
                    ", style='" + style + '\'' +
                    ", group='" + group + '\'' +
                    ", context='" + context + '\'' +
                    '}';
        }
    }


    /**
     * public void recordIndexOutput(Index index) throws CatalogManagerException, IOException, CatalogIOManagerException {
     * QueryResult<File> fileResult = catalogManager.getFileByIndexJobId(index.getJobId()); //TODO: sessionId¿?¿?
     * if(fileResult.getResult().isEmpty()) {
     * return;
     * }
     * File indexedFile = fileResult.getResult().get(0);
     * <p/>
     * List<Integer> fileIds = new LinkedList<>();
     * CatalogFileManager catalogFileManager = new CatalogFileManager(catalogManager);
     * <p/>
     * try {
     * File outDir = catalogManager.getFile(index.getOutDirId(), sessionId).getResult().get(0);
     * URI tmpOutDirUri = URI.create(index.getTmpOutDirUri());
     * List<URI> uris = catalogManager.getCatalogIOManagerFactory().get(tmpOutDirUri.getScheme()).listFiles(tmpOutDirUri);
     * int studyId = catalogManager.getStudyIdByFileId(indexedFile.getId());
     * <p/>
     * for (URI uri : uris) {
     * String generatedFile = Paths.get(uri).toAbsolutePath().toString().substring(tmpOutDirUri.getPath().length());
     * String filePath = Paths.get(outDir.getPath(), generatedFile).toString();
     * QueryResult<File> searchFile = catalogManager.searchFile(studyId, new QueryOptions("path", filePath), sessionId);
     * if(searchFile.getNumResults() != 0) {
     * File file = searchFile.getResult().get(0);
     * switch (policy) {
     * case "delete":
     * catalogManager.deleteFile(file.getId(), sessionId);
     * break;
     * case "rename":
     * throw new UnsupportedOperationException("Unimplemented policy 'rename'");
     * case "doError":
     * throw new UnsupportedOperationException("Unimplemented policy 'error'");
     * }
     * }
     * QueryResult<File> fileQueryResult = catalogManager.createFile(
     * studyId, File.TYPE_FILE, "", filePath, "Generated from indexing file " + indexedFile.getId(),
     * true, sessionId);
     * File resultFile = fileQueryResult.getResult().get(0);
     * fileIds.add(resultFile.getId());
     * catalogFileManager.upload(uri, resultFile, null, sessionId, false, false, true, true);
     * }
     * } catch (CatalogManagerException | InterruptedException | IOException | CatalogIOManagerException e) {
     * e.printStackTrace();
     * logger.error("Error while processing Job", e);
     * return;
     * }
     * <p/>
     * <p/>
     * // Update file.attributes
     * for (Index auxIndex: indexedFile.getIndices()) {
     * if (auxIndex.getJobId().equals(index.getJobId())) {
     * //                auxIndex.setJobId(""); //Clear the jobId
     * auxIndex.setStatus(Index.INDEXED);
     * //                auxIndex.setOutput(fileIds);
     * catalogManager.setIndexFile(indexedFile.getId(), auxIndex.getStorageEngine(), auxIndex, sessionId);
     * }
     * }
     * }
     */


    private void log(Object msg) {
        System.out.println("\n\n\nAleman: - " + msg.toString() + "\n\n\n");
    }

    private class ResultXMLElem {
        private String type;
        private String fileName;
        private File.Bioformat tag;

        public ResultXMLElem(String type, String fileName, String tag) {

            this.type = type;
            this.fileName = fileName;

            if (type.equals("DATA")) {
                this.tag = this.parseTags(tag);

            } else {
                this.tag = File.Bioformat.NONE;
            }
        }

        private File.Bioformat parseTags(String tag) {

            File.Bioformat res;

            switch (tag) {
                case "datamatrix,expression":
                    res = File.Bioformat.DATAMATRIX_EXPRESSION;
                    break;
                default:
                    res = File.Bioformat.NONE;
            }

            return res;

        }

        public String getType() {
            return type;
        }

        public void setType(String type) {
            this.type = type;
        }

        public String getFileName() {
            return fileName;
        }

        public void setFileName(String fileName) {
            this.fileName = fileName;
        }

        public File.Bioformat getTag() {
            return tag;
        }

        public void setTag(File.Bioformat tag) {
            this.tag = tag;
        }

        @Override
        public String toString() {
            return "ResultXMLElem{" +
                    "type='" + type + '\'' +
                    ", fileName='" + fileName + '\'' +
                    ", tag='" + tag + '\'' +
                    '}';
        }
    }
}
