package org.opencb.opencga.catalog.core.beans;

/**
 * Created by jacobo on 11/09/14.
 */
public class File {
    private int fileId;
    private String fileName;
    private String fileType;
    private String fileFormat;
    private String fileBioType;
    private String path;
    private String uri;
    private String submissionDate;
    private long size;

    private String source;

    public File() {
    }

    public File(int fileId, String fileName, String fileType, String fileFormat, String fileBioType, String path, String uri, String submissionDate, long size, String source) {
        this.fileId = fileId;
        this.fileName = fileName;
        this.fileType = fileType;
        this.fileFormat = fileFormat;
        this.fileBioType = fileBioType;
        this.path = path;
        this.uri = uri;
        this.submissionDate = submissionDate;
        this.size = size;
        this.source = source;
    }

    public String getSource() {
        return source;
    }

    public void setSource(String source) {
        this.source = source;
    }

    public long getSize() {
        return size;
    }

    public void setSize(long size) {
        this.size = size;
    }

    public String getSubmissionDate() {
        return submissionDate;
    }

    public void setSubmissionDate(String submissionDate) {
        this.submissionDate = submissionDate;
    }

    public String getUri() {
        return uri;
    }

    public void setUri(String uri) {
        this.uri = uri;
    }

    public String getPath() {
        return path;
    }

    public void setPath(String path) {
        this.path = path;
    }

    public String getFileBioType() {
        return fileBioType;
    }

    public void setFileBioType(String fileBioType) {
        this.fileBioType = fileBioType;
    }

    public String getFileFormat() {
        return fileFormat;
    }

    public void setFileFormat(String fileFormat) {
        this.fileFormat = fileFormat;
    }

    public String getFileType() {
        return fileType;
    }

    public void setFileType(String fileType) {
        this.fileType = fileType;
    }

    public String getFileName() {
        return fileName;
    }

    public void setFileName(String fileName) {
        this.fileName = fileName;
    }

    public int getFileId() {
        return fileId;
    }

    public void setFileId(int fileId) {
        this.fileId = fileId;
    }
}
