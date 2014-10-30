package org.opencb.opencga.catalog.beans;

import java.util.LinkedList;
import java.util.List;

/**
 * Created by jacobo on 11/09/14.
 */
public class Tool {

    private int id;
    private String alias;
    private String name;
    private String description;
    private Object manifest;
    private Object result;
    private String path;
    private List<Acl> acl;

    public Tool() {
        this("", "", "", null, null, "");
    }

    public Tool(String alias, String name, String description, Object manifest, Object result, String path) {
        this(-1 , alias, name, description, manifest, result, path, new LinkedList());
    }
    public Tool(int id, String alias, String name, String description, Object manifest, Object result, String path, List<Acl> acl) {
        this.id = id;
        this.alias = alias;
        this.name = name;
        this.description = description;
        this.manifest = manifest;
        this.result = result;
        this.path = path;
        this.acl = acl;
    }

    @Override
    public String toString() {
        return "Tool{" +
                "id=" + id +
                "alias='" + alias + '\'' +
                ", name='" + name + '\'' +
                ", description='" + description + '\'' +
                ", manifest=" + manifest +
                ", result=" + result +
                ", path='" + path + '\'' +
                ", acl=" + acl +
                '}';
    }

    public int getId() {
        return id;
    }

    public void setId(int id) {
        this.id = id;
    }

    public String getAlias() {
        return alias;
    }

    public void setAlias(String alias) {
        this.alias = alias;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
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


    public List<Acl> getAcl() {
        return acl;
    }

    public void setAcl(List<Acl> acl) {
        this.acl = acl;
    }

}
