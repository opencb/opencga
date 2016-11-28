package org.opencb.opencga.storage.core.local.models;

import java.nio.file.Path;

/**
 * Created by pfurio on 24/11/16.
 */
public class FileInfo {

    private String name;
    private Path path; // Physical path to the file or folder (equivalent to URI in catalog)
    private long fileId;


    public FileInfo() {
        this.fileId = -1;
    }

    public String getName() {
        return name;
    }

    public FileInfo setName(String name) {
        this.name = name;
        return this;
    }

    public Path getPath() {
        return path;
    }

    public FileInfo setPath(Path path) {
        this.path = path;
        return this;
    }

    public long getFileId() {
        return fileId;
    }

    public FileInfo setFileId(long fileId) {
        this.fileId = fileId;
        return this;
    }
}
