package org.opencb.opencga.core.models.file;

import org.apache.commons.lang3.StringUtils;

public class SmallRelatedFileParams {

    private String file;
    private File.RelatedFile.Relation relation;

    public SmallRelatedFileParams() {
    }

    public SmallRelatedFileParams(String file, File.RelatedFile.Relation relation) {
        this.file = file;
        this.relation = relation;
    }

    public static SmallRelatedFileParams of(File.RelatedFile file) {
        String fileStr = null;
        if (file.getFile() != null) {
            if (StringUtils.isNotEmpty(file.getFile().getPath())) {
                fileStr = file.getFile().getPath();
            } else {
                fileStr = file.getFile().getUuid();
            }
        }
        return new SmallRelatedFileParams(fileStr, file.getRelation());
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

    public SmallRelatedFileParams setFile(String file) {
        this.file = file;
        return this;
    }

    public File.RelatedFile.Relation getRelation() {
        return relation;
    }

    public SmallRelatedFileParams setRelation(File.RelatedFile.Relation relation) {
        this.relation = relation;
        return this;
    }
}
