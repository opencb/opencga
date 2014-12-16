package org.opencb.opencga.catalog.beans;

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
    private int studyId;

    private String description;

    private List<AnnotationSet> annotationSets;

    public Sample() {
    }

    public Sample(int id, String name, String source, Individual individual, int studyId, String description) {
        this(id, name, source, individual, studyId, description, new LinkedList<AnnotationSet>());
    }

    public Sample(int id, String name, String source, Individual individual, int studyId, String description,
                  List<AnnotationSet> annotationSets) {
        this.id = id;
        this.name = name;
        this.source = source;
        this.individual = individual;
        this.studyId = studyId;
        this.description = description;
        this.annotationSets = annotationSets;
    }

    @Override
    public String toString() {
        return "Sample{" +
                "id=" + id +
                ", name='" + name + '\'' +
                ", source='" + source + '\'' +
                ", individual=" + individual +
                ", studyId=" + studyId +
                ", description='" + description + '\'' +
                ", annotationSets=" + annotationSets +
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

    public int getStudyId() {
        return studyId;
    }

    public void setStudyId(int studyId) {
        this.studyId = studyId;
    }

    public String getDescription() {
        return description;
    }

    public void setDescription(String description) {
        this.description = description;
    }

    public List<AnnotationSet> getAnnotationSets() {
        return annotationSets;
    }

    public void setAnnotationSets(List<AnnotationSet> annotationSets) {
        this.annotationSets = annotationSets;
    }
}
