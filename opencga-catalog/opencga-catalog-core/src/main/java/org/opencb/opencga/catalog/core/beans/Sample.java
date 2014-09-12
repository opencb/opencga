package org.opencb.opencga.catalog.core.beans;

import java.util.LinkedList;
import java.util.List;

/**
 * Created by jacobo on 11/09/14.
 */
public class Sample {

    private int id;
    private String name;
    private String source;
    private Individual individual;
    private String description;

    private List<SampleAnnotation> sampleAnnotations;

    public Sample() {
    }

    public Sample(int id, String name, String source, Individual individual, String description) {
        this(id, name, source, individual, description, new LinkedList<SampleAnnotation>());
    }

    public Sample(int id, String name, String source, Individual individual, String description,
                  List<SampleAnnotation> sampleAnnotations) {
        this.id = id;
        this.name = name;
        this.source = source;
        this.individual = individual;
        this.description = description;
        this.sampleAnnotations = sampleAnnotations;
    }

    @Override
    public String toString() {
        return "Sample{" +
                "id='" + id + '\'' +
                ", name='" + name + '\'' +
                ", source='" + source + '\'' +
                ", individual=" + individual +
                ", description='" + description + '\'' +
                ", annotations=" + sampleAnnotations +
                '}';
    }

    public int getId() {
        return id;
    }

    public void setId(int id) {
        this.id = id;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getSource() {
        return source;
    }

    public void setSource(String source) {
        this.source = source;
    }

    public Individual getIndividual() {
        return individual;
    }

    public void setIndividual(Individual individual) {
        this.individual = individual;
    }

    public String getDescription() {
        return description;
    }

    public void setDescription(String description) {
        this.description = description;
    }

    public List<SampleAnnotation> getSampleAnnotations() {
        return sampleAnnotations;
    }

    public void setSampleAnnotations(List<SampleAnnotation> sampleAnnotations) {
        this.sampleAnnotations = sampleAnnotations;
    }
}
