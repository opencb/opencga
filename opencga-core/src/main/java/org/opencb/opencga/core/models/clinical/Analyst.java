package org.opencb.opencga.core.models.clinical;

public class Analyst {

    private String author;
    private String email;
    private String company;

    public Analyst() {
    }

    public Analyst(String author, String email, String company) {
        this.author = author;
        this.email = email;
        this.company = company;
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("Analyst{");
        sb.append("author='").append(author).append('\'');
        sb.append(", email='").append(email).append('\'');
        sb.append(", company='").append(company).append('\'');
        sb.append('}');
        return sb.toString();
    }

    public String getAuthor() {
        return author;
    }

    public Analyst setAuthor(String author) {
        this.author = author;
        return this;
    }

    public String getEmail() {
        return email;
    }

    public Analyst setEmail(String email) {
        this.email = email;
        return this;
    }

    public String getCompany() {
        return company;
    }

    public Analyst setCompany(String company) {
        this.company = company;
        return this;
    }
}
