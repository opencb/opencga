package org.opencb.opencga.app.cli.main.executors.commons;

import org.codehaus.jackson.map.ObjectMapper;
import org.opencb.commons.datastore.core.ObjectMap;
import org.opencb.commons.datastore.core.QueryResponse;
import org.opencb.opencga.app.cli.main.options.commons.AnnotationCommandOptions;
import org.opencb.opencga.catalog.models.AnnotationSet;
import org.opencb.opencga.client.rest.commons.AnnotationClient;

import java.io.File;
import java.io.IOException;

/**
 * Created by pfurio on 28/07/16.
 */
public class AnnotationCommandExecutor<T,U> {

    public QueryResponse<AnnotationSet> createAnnotationSet(
            AnnotationCommandOptions.AnnotationSetsCreateCommandOptions createCommandOptions, AnnotationClient<T,U> client)
            throws IOException {

        ObjectMapper mapper = new ObjectMapper();
        ObjectMap obj = mapper.readValue(new File(createCommandOptions.annotations), ObjectMap.class);
        return client.createAnnotationSet(createCommandOptions.id, createCommandOptions.variableSetId,
                createCommandOptions.annotationSetName, obj);
    }

    public QueryResponse<AnnotationSet> getAllAnnotationSets(
            AnnotationCommandOptions.AnnotationSetsAllInfoCommandOptions infoCommandOptions, AnnotationClient<T,U> client)
            throws IOException {

        ObjectMap params = new ObjectMap();
        params.putIfNotNull("asMap", infoCommandOptions.asMap);
        return client.getAllAnnotationSets(infoCommandOptions.id, params);
    }

    public QueryResponse<AnnotationSet> getAnnotationSet(AnnotationCommandOptions.AnnotationSetsInfoCommandOptions infoCommandOptions,
                                     AnnotationClient<T,U> client) throws IOException {

        ObjectMap params = new ObjectMap();
        params.putIfNotNull("asMap", infoCommandOptions.asMap);
        return client.getAnnotationSet(infoCommandOptions.id, infoCommandOptions.annotationSetName, params);
    }

    public QueryResponse<AnnotationSet> searchAnnotationSets(
            AnnotationCommandOptions.AnnotationSetsSearchCommandOptions searchCommandOptions, AnnotationClient<T,U> client)
            throws IOException {
        ObjectMap params = new ObjectMap();
        params.putIfNotNull("variableSetId", searchCommandOptions.variableSetId);
        params.putIfNotNull("annotation", searchCommandOptions.annotation);
        params.putIfNotNull("asMap", searchCommandOptions.asMap);
        return client.searchAnnotationSets(searchCommandOptions.id, params);
    }

    public QueryResponse<AnnotationSet> deleteAnnotationSet(
            AnnotationCommandOptions.AnnotationSetsDeleteCommandOptions deleteCommandOptions, AnnotationClient<T,U> client)
            throws IOException {
        ObjectMap params = new ObjectMap();
        params.putIfNotNull("annotations", deleteCommandOptions.annotations);
        return client.deleteAnnotationSet(deleteCommandOptions.id, deleteCommandOptions.annotationSetName, params);
    }

    public QueryResponse<AnnotationSet> updateAnnotationSet(
            AnnotationCommandOptions.AnnotationSetsUpdateCommandOptions updateCommandOptions, AnnotationClient<T,U> client)
            throws IOException {

        ObjectMapper mapper = new ObjectMapper();
        ObjectMap obj = mapper.readValue(new File(updateCommandOptions.annotations), ObjectMap.class);

        return client.updateAnnotationSet(updateCommandOptions.id, updateCommandOptions.annotationSetName, obj);
    }
}
