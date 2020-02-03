package org.opencb.opencga.core.models.clinical;

public class Alert {

    private String author;
    private String date;
    private String message;
    private Risk risk;

    public Alert() {
    }

    public Alert(String author, String date, String message, Risk risk) {
        this.author = author;
        this.date = date;
        this.message = message;
        this.risk = risk;
    }


    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("Alert{");
        sb.append("author='").append(author).append('\'');
        sb.append(", date='").append(date).append('\'');
        sb.append(", message='").append(message).append('\'');
        sb.append(", risk=").append(risk);
        sb.append('}');
        return sb.toString();
    }

    public String getAuthor() {
        return author;
    }

    public Alert setAuthor(String author) {
        this.author = author;
        return this;
    }

    public String getDate() {
        return date;
    }

    public Alert setDate(String date) {
        this.date = date;
        return this;
    }

    public String getMessage() {
        return message;
    }

    public Alert setMessage(String message) {
        this.message = message;
        return this;
    }

    public Risk getRisk() {
        return risk;
    }

    public Alert setRisk(Risk risk) {
        this.risk = risk;
        return this;
    }

    public enum Risk {
        HIGH,
        MEDIUM,
        LOW
    }

}
