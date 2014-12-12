package org.opencb.opencga.catalog.beans;

import java.util.Map;
import java.util.Set;

/**
 * Created by imedina on 25/11/14.
 */
public class AnnotationSet {

    private String name;
    private Set<Annotation> values;
    private String date;

    private Map<String, Object> attributes;


    public AnnotationSet() {
    }

    public AnnotationSet(String name, Set<Annotation> values) {
        this.name = name;
        this.values = values;
    }

    public AnnotationSet(String name, Set<Annotation> values, String date) {
        this.name = name;
        this.values = values;
        this.date = date;
    }

    public AnnotationSet(String name, Set<Annotation> values, String date, Map<String, Object> attributes) {
        this.name = name;
        this.values = values;
        this.date = date;
        this.attributes = attributes;
    }

    @Override
    public String toString() {
        return "SampleAnnotationSet{" +
                "name='" + name + '\'' +
                ", values=" + values +
                ", date='" + date + '\'' +
                ", attributes=" + attributes +
                '}';
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public Set<Annotation> getValues() {
        return values;
    }

    public void setValues(Set<Annotation> values) {
        this.values = values;
    }

    public String getDate() {
        return date;
    }

    public void setDate(String date) {
        this.date = date;
    }

    public Map<String, Object> getAttributes() {
        return attributes;
    }

    public void setAttributes(Map<String, Object> attributes) {
        this.attributes = attributes;
    }
}
