package org.opencb.opencga.analysis.beans;

import java.util.List;

public class Analysis {
    private Author author;
    private String version, id, name, description, website, publication;
    private Icon icon;
    private List<Option> globalParams;
    private List<Execution> executions;
    private List<Example> examples;
    private List<Acl> acl;

    public Analysis(Author author, String version, String id, String name, String description,
                    String website, String publication, Icon icon, List<Option> globalParams,
                    List<Execution> executions, List<Example> examples, List<Acl> acl) {
        this.author = author;
        this.version = version;
        this.id = id;
        this.name = name;
        this.description = description;
        this.website = website;
        this.publication = publication;
        this.icon = icon;
        this.globalParams = globalParams;
        this.executions = executions;
        this.examples = examples;
        this.acl = acl;
    }

    public Author getAuthor() {
        return author;
    }

    public void setAuthor(Author author) {
        this.author = author;
    }

    public String getVersion() {
        return version;
    }

    public void setVersion(String version) {
        this.version = version;
    }

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
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

    public String getWebsite() {
        return website;
    }

    public void setWebsite(String website) {
        this.website = website;
    }

    public String getPublication() {
        return publication;
    }

    public void setPublication(String publication) {
        this.publication = publication;
    }

    public Icon getIcon() {
        return icon;
    }

    public void setIcon(Icon icon) {
        this.icon = icon;
    }

    public List<Option> getGlobalParams() {
        return globalParams;
    }

    public void setGlobalParams(List<Option> globalParams) {
        this.globalParams = globalParams;
    }

    public List<Execution> getExecutions() {
        return executions;
    }

    public void setExecutions(List<Execution> executions) {
        this.executions = executions;
    }

    public List<Example> getExamples() {
        return examples;
    }

    public void setExamples(List<Example> examples) {
        this.examples = examples;
    }

    public List<Acl> getAcl() {
        return acl;
    }

    public void setAcl(List<Acl> acl) {
        this.acl = acl;
    }
}
