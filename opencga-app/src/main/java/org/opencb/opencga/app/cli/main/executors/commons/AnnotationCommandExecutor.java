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

    public void createAnnotationSet(AnnotationCommandOptions.AnnotationSetsCreateCommandOptions createCommandOptions,
                                    AnnotationClient<T,U> client) throws IOException {
        QueryResponse<AnnotationSet> annotationSet = client.createAnnotationSet(createCommandOptions.id, createCommandOptions.variableSetId,
                createCommandOptions.annotationSetName, createCommandOptions.annotations);
        System.out.println(annotationSet.toString());
    }

    public void getAllAnnotationSets(AnnotationCommandOptions.AnnotationSetsAllInfoCommandOptions infoCommandOptions,
                                     AnnotationClient<T,U> client) throws IOException {
        QueryResponse<AnnotationSet> annotationSet = client.getAllAnnotationSets(infoCommandOptions.id, null);
        System.out.println(annotationSet.toString());
    }

    public void getAnnotationSet(AnnotationCommandOptions.AnnotationSetsInfoCommandOptions infoCommandOptions,
                                     AnnotationClient<T,U> client) throws IOException {
        QueryResponse<AnnotationSet> annotationSet = client.getAnnotationSet(infoCommandOptions.id, infoCommandOptions.annotationSetName,
                null);
        System.out.println(annotationSet.toString());
    }

    public void searchAnnotationSets(AnnotationCommandOptions.AnnotationSetsSearchCommandOptions searchCommandOptions,
                                     AnnotationClient<T,U> client) throws IOException {
        ObjectMap params = new ObjectMap();
        params.putIfNotNull("variableSetId", searchCommandOptions.variableSetId);
        params.putIfNotNull("annotation", searchCommandOptions.annotation);
        QueryResponse<AnnotationSet> annotationSet = client.searchAnnotationSets(searchCommandOptions.id, params);
        System.out.println(annotationSet.toString());
    }

    public void deleteAnnotationSet(AnnotationCommandOptions.AnnotationSetsDeleteCommandOptions deleteCommandOptions,
                                     AnnotationClient<T,U> client) throws IOException {
        ObjectMap params = new ObjectMap();
        params.putIfNotNull("annotations", deleteCommandOptions.annotations);
        QueryResponse<AnnotationSet> annotationSet = client.deleteAnnotationSet(deleteCommandOptions.id,
                deleteCommandOptions.annotationSetName, params);
        System.out.println(annotationSet.toString());
    }

    public void updateAnnotationSet(AnnotationCommandOptions.AnnotationSetsUpdateCommandOptions updateCommandOptions,
                                     AnnotationClient<T,U> client) throws IOException {
        QueryResponse<AnnotationSet> annotationSet = client.updateAnnotationSet(updateCommandOptions.id,
                updateCommandOptions.annotationSetName, updateCommandOptions.annotations);
        System.out.println(annotationSet.toString());
    }
}
