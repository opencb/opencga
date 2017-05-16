package org.opencb.opencga.server.rest;

import org.opencb.opencga.catalog.exceptions.CatalogException;
import org.opencb.opencga.catalog.managers.AbstractManager;
import org.opencb.opencga.catalog.managers.api.IStudyManager;
import org.opencb.opencga.catalog.models.Annotation;
import org.opencb.opencga.catalog.models.AnnotationSet;

import java.util.HashSet;
import java.util.Map;
import java.util.Set;

/**
 * Created by pfurio on 12/05/17.
 */
public class CommonModels {

    public static class AnnotationSetParams {
        public String name;
        public String variableSetId;
        public Map<String, Object> annotations;
        public Map<String, Object> attributes;

        public AnnotationSet toAnnotationSet(String studyStr, IStudyManager studyManager, String sessionId) throws CatalogException {
            AbstractManager.MyResourceId resource = studyManager.getVariableSetId(this.variableSetId, studyStr, sessionId);
            Set<Annotation> annotationSet = new HashSet<>();
            for (Map.Entry<String, Object> entry : annotations.entrySet()) {
                annotationSet.add(new Annotation(entry.getKey(), entry.getValue()));
            }
            return new AnnotationSet(name, resource.getResourceId(), annotationSet, attributes);
        }
    }

}
