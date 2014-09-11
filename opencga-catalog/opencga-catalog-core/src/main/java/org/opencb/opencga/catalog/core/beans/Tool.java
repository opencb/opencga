package org.opencb.opencga.catalog.core.beans;

/**
 * Created by jacobo on 11/09/14.
 */
public class Tool {
    private String name;
    private String ownerId;
    private String description;
    private Object manifest;
    private Object result;
    private String path;

    public Tool() {
    }

    public Tool(String name, String ownerId, String description, Object manifest, Object result, String path) {
        this.name = name;
        this.ownerId = ownerId;
        this.description = description;
        this.manifest = manifest;
        this.result = result;
        this.path = path;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getOwnerId() {
        return ownerId;
    }

    public void setOwnerId(String ownerId) {
        this.ownerId = ownerId;
    }

    public String getDescription() {
        return description;
    }

    public void setDescription(String description) {
        this.description = description;
    }

    public Object getManifest() {
        return manifest;
    }

    public void setManifest(Object manifest) {
        this.manifest = manifest;
    }

    public Object getResult() {
        return result;
    }

    public void setResult(Object result) {
        this.result = result;
    }

    public String getPath() {
        return path;
    }

    public void setPath(String path) {
        this.path = path;
    }
}
