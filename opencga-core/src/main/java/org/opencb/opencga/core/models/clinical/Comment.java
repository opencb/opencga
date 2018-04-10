package org.opencb.opencga.core.models.clinical;

public class Comment {

    private String author;
    private String type;
    private String text;
    private String date;

    public Comment() {
    }

    public Comment(String author, String type, String text, String date) {
        this.author = author;
        this.type = type;
        this.text = text;
        this.date = date;
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("Comment{");
        sb.append("author='").append(author).append('\'');
        sb.append(", type='").append(type).append('\'');
        sb.append(", text='").append(text).append('\'');
        sb.append(", date='").append(date).append('\'');
        sb.append('}');
        return sb.toString();
    }

    public String getAuthor() {
        return author;
    }

    public Comment setAuthor(String author) {
        this.author = author;
        return this;
    }

    public String getType() {
        return type;
    }

    public Comment setType(String type) {
        this.type = type;
        return this;
    }

    public String getText() {
        return text;
    }

    public Comment setText(String text) {
        this.text = text;
        return this;
    }

    public String getDate() {
        return date;
    }

    public Comment setDate(String date) {
        this.date = date;
        return this;
    }

}
