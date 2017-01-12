package org.opencb.opencga.storage.core.manager.models;

import org.opencb.opencga.catalog.models.File;

import java.nio.file.Path;

/**
 * Created by pfurio on 24/11/16.
 */
public class FileInfo {

    private String name;
    private Path path; // Physical path to the file or folder (equivalent to URI in catalog)
    private long fileId;
    private File.Bioformat bioformat;
    private File.Format format;


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

    public File.Bioformat getBioformat() {
        return bioformat;
    }

    public FileInfo setBioformat(File.Bioformat bioformat) {
        this.bioformat = bioformat;
        return this;
    }

    public File.Format getFormat() {
        return format;
    }

    public FileInfo setFormat(File.Format format) {
        this.format = format;
        return this;
    }
}
