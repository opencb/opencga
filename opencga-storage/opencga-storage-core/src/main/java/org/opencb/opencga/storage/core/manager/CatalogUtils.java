package org.opencb.opencga.storage.core.manager;

import org.opencb.commons.datastore.core.Query;
import org.opencb.commons.datastore.core.QueryParam;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.function.Function;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Created by pfurio on 02/12/16.
 */
public class CatalogUtils {

    /**
     * @see {@link org.opencb.opencga.catalog.db.mongodb.MongoDBUtils#ANNOTATION_PATTERN}
     */
    public static final Pattern ANNOTATION_PATTERN = Pattern.compile("^([a-zA-Z\\\\.]+)([\\^=<>~!$]+.*)$");

    /**
     * Parse a generic string with comma separated key=values and obtain a query understandable by catalog.
     *
     * @param myString String of the kind age>20;ontologies=hpo:123,hpo:456;name=smith
     * @param getParam Get param function that will return null if the key string is not one of the accepted keys in catalog. For those
     *                 cases, they will be treated as annotations.
     * @return A query object.
     */
    protected static Query parseQuery(String myString, Function<String, QueryParam> getParam) {

        org.opencb.commons.datastore.core.Query query = new Query();

        List<String> annotationList = new ArrayList<>();
        List<String> params = Arrays.asList(myString.replaceAll("\\s+", "").split(";"));
        for (String param : params) {
            Matcher matcher = ANNOTATION_PATTERN.matcher(param);
            String key;
            if (matcher.find()) {
                key = matcher.group(1);
                if (getParam.apply(key) != null && !key.startsWith("annotation")) {
                    query.put(key, matcher.group(2));
                } else {
                    // Annotation
                    String myKey = key;
                    if (!key.startsWith("annotation.")) {
                        myKey = "annotation." + key;
                    }
                    annotationList.add(myKey + matcher.group(2));
                }
            }
        }

        query.put("annotation", annotationList);

        return query;
    }
}
