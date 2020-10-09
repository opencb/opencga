package org.opencb.opencga.core.models.clinical;

import org.opencb.biodata.models.clinical.ClinicalComment;

import java.util.List;

public class ClinicalCommentParam {

    private String message;
    private List<String> tags;

    public ClinicalCommentParam() {
    }

    public ClinicalCommentParam(String message, List<String> tags) {
        this.message = message;
        this.tags = tags;
    }

    public ClinicalComment toClinicalComment() {
        return new ClinicalComment("", message, tags, "");
    }

    public static ClinicalCommentParam of(ClinicalComment comment) {
        return new ClinicalCommentParam(comment.getMessage(), comment.getTags());
    }

    public String getMessage() {
        return message;
    }

    public ClinicalCommentParam setMessage(String message) {
        this.message = message;
        return this;
    }

    public List<String> getTags() {
        return tags;
    }

    public ClinicalCommentParam setTags(List<String> tags) {
        this.tags = tags;
        return this;
    }
}
