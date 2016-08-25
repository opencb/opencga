package org.opencb.opencga.catalog.models;

import java.util.Collections;
import java.util.List;

/**
 * Created by pfurio on 28/07/16.
 */
public class FileTree {

    private File file;
    private List<FileTree> children;

    public FileTree(File file) {
        this.file = file;
        this.children = Collections.emptyList();
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("FileTree{");
        sb.append("file=").append(file);
        sb.append(", children=").append(children);
        sb.append('}');
        return sb.toString();
    }

    public File getFile() {
        return file;
    }

    public FileTree setFile(File file) {
        this.file = file;
        return this;
    }

    public List<FileTree> getChildren() {
        return children;
    }

    public FileTree setChildren(List<FileTree> children) {
        this.children = children;
        return this;
    }
}
