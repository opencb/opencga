package org.opencb.opencga.core.models.clinical;

import org.opencb.biodata.models.clinical.ClinicalComment;

import java.util.List;

public class ClinicalCommentParam {

    private String message;
    private List<String> tags;
    private String date;

    public ClinicalCommentParam() {
    }

    public ClinicalCommentParam(String message, List<String> tags) {
        this.message = message;
        this.tags = tags;
    }

    public ClinicalCommentParam(String message, List<String> tags, String date) {
        this.message = message;
        this.tags = tags;
        this.date = date;
    }

    public ClinicalComment toClinicalComment() {
        return new ClinicalComment("", message, tags, "");
    }

    public static ClinicalCommentParam of(ClinicalComment comment) {
        return new ClinicalCommentParam(comment.getMessage(), comment.getTags(), comment.getDate());
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

    public String getDate() {
        return date;
    }

    public ClinicalCommentParam setDate(String date) {
        this.date = date;
        return this;
    }
}
