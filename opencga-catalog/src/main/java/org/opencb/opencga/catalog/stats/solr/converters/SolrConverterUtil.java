package org.opencb.opencga.catalog.stats.solr.converters;

import org.apache.commons.collections.map.HashedMap;
import org.opencb.opencga.core.models.AnnotationSet;
import org.opencb.opencga.core.models.OntologyTerm;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * Created by wasim on 03/07/18.
 */
public class SolrConverterUtil {


    public static Map<String, Object> populateAnnotations(List<AnnotationSet> annotationSets) {
        Map<String, Object> result = new HashedMap();
        for (AnnotationSet annotationSet : annotationSets) {
            for (String annotationKey : annotationSet.getAnnotations().keySet()) {
                Object value = annotationSet.getAnnotations().get(annotationKey);
                if (!type(value).equals("__o__")) {
                    result.put("annotations" + type(value) + annotationSet.getName() + "__" + annotationSet.getVariableSetId()
                            + "__" + annotationKey, value);
                }
            }
        }
        return result;
    }

    public static List<String> populatePhenotypes(List<OntologyTerm> phenotypes) {
        List<String> phenotypesIds = new ArrayList<>();
        for (OntologyTerm ontologyTerm : phenotypes) {
            phenotypesIds.add(ontologyTerm.getId());
        }
        return phenotypesIds;
    }

    public static String type(Object object) {

        if (object instanceof Boolean) {
            return "__b__";
        } else if (object instanceof Integer) {
            return "__i__";
        } else if (object instanceof String) {
            return "__s__";
        } else if (object instanceof Double) {
            return "__d__";
        } else if (object instanceof Object) {
            return "__o__";
        }
        return "__o__";
    }
}
