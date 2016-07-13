package org.opencb.opencga.catalog.models;

import java.util.List;

/**
 * Created by pfurio on 07/07/16.
 */
public interface Annotable {

    List<AnnotationSet> getAnnotationSets();

    Annotable setAnnotationSets(List<AnnotationSet> annotationSets);

}
