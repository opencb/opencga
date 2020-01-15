package org.opencb.opencga.core.models.file;

public class FileLinkRelatedFileParams {

    private String file;
    private File.RelatedFile.Relation relation;

    public FileLinkRelatedFileParams() {
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("FileLinkRelatedFileParams{");
        sb.append("file='").append(file).append('\'');
        sb.append(", relation=").append(relation);
        sb.append('}');
        return sb.toString();
    }

    public String getFile() {
        return file;
    }

    public FileLinkRelatedFileParams setFile(String file) {
        this.file = file;
        return this;
    }

    public File.RelatedFile.Relation getRelation() {
        return relation;
    }

    public FileLinkRelatedFileParams setRelation(File.RelatedFile.Relation relation) {
        this.relation = relation;
        return this;
    }
}
