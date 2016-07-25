package org.opencb.opencga.catalog.models;

import java.util.List;

/**
 * Created by pfurio on 07/07/16.
 */
public abstract class Annotable {

    protected List<AnnotationSet> annotationSets;


    public List<AnnotationSet> getAnnotationSets() {
        return annotationSets;
    }

    public Annotable setAnnotationSets(List<AnnotationSet> annotationSets) {
        this.annotationSets = annotationSets;
        return this;
    }
}
