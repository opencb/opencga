package org.opencb.opencga.app.cli.main.executors.commons;

import org.opencb.commons.datastore.core.ObjectMap;
import org.opencb.commons.datastore.core.QueryResponse;
import org.opencb.opencga.app.cli.main.options.commons.AnnotationCommandOptions;
import org.opencb.opencga.catalog.models.AnnotationSet;
import org.opencb.opencga.client.rest.commons.AnnotationClient;

import java.io.IOException;

/**
 * Created by pfurio on 28/07/16.
 */
public class AnnotationCommandExecutor<T,U> {

    public QueryResponse<AnnotationSet> createAnnotationSet(
            AnnotationCommandOptions.AnnotationSetsCreateCommandOptions createCommandOptions, AnnotationClient<T,U> client)
            throws IOException {
        return client.createAnnotationSet(createCommandOptions.id, createCommandOptions.variableSetId,
                createCommandOptions.annotationSetName, createCommandOptions.annotations);
    }

    public QueryResponse<AnnotationSet> getAllAnnotationSets(
            AnnotationCommandOptions.AnnotationSetsAllInfoCommandOptions infoCommandOptions, AnnotationClient<T,U> client)
            throws IOException {
        return client.getAllAnnotationSets(infoCommandOptions.id, null);
    }

    public QueryResponse<AnnotationSet> getAnnotationSet(AnnotationCommandOptions.AnnotationSetsInfoCommandOptions infoCommandOptions,
                                     AnnotationClient<T,U> client) throws IOException {
        return client.getAnnotationSet(infoCommandOptions.id, infoCommandOptions.annotationSetName, null);
    }

    public QueryResponse<AnnotationSet> searchAnnotationSets(
            AnnotationCommandOptions.AnnotationSetsSearchCommandOptions searchCommandOptions, AnnotationClient<T,U> client)
            throws IOException {
        ObjectMap params = new ObjectMap();
        params.putIfNotNull("variableSetId", searchCommandOptions.variableSetId);
        params.putIfNotNull("annotation", searchCommandOptions.annotation);
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
        return client.updateAnnotationSet(updateCommandOptions.id,
                updateCommandOptions.annotationSetName, updateCommandOptions.annotations);
    }
}
