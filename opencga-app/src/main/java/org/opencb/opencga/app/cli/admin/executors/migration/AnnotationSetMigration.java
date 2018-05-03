package org.opencb.opencga.app.cli.admin.executors.migration;

import com.mongodb.client.MongoCursor;
import org.bson.Document;
import org.opencb.commons.datastore.core.ObjectMap;
import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.commons.datastore.core.QueryResult;
import org.opencb.commons.datastore.mongodb.GenericDocumentComplexConverter;
import org.opencb.commons.datastore.mongodb.MongoDBCollection;
import org.opencb.opencga.catalog.db.mongodb.AnnotationMongoDBAdaptor;
import org.opencb.opencga.catalog.db.mongodb.MongoDBAdaptorFactory;
import org.opencb.opencga.catalog.exceptions.CatalogDBException;
import org.opencb.opencga.catalog.exceptions.CatalogException;
import org.opencb.opencga.catalog.utils.CatalogAnnotationsValidator;
import org.opencb.opencga.core.config.Configuration;
import org.opencb.opencga.core.models.AnnotationSet;
import org.opencb.opencga.core.models.VariableSet;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

/**
 * Executes all migration scripts related with the issue #772
 * Created on 31/01/18.
 *
 * @author Pedro Furio;
 */
public class AnnotationSetMigration {

    private Map<Long, VariableSet> variableSetMap;
    private MongoDBAdaptorFactory dbAdaptorFactory;
    private GenericDocumentComplexConverter<AnnotableForMigration> converter;
    private final Logger logger = LoggerFactory.getLogger(AnnotationSetMigration.class);

    public AnnotationSetMigration(Configuration configuration) throws CatalogDBException {
        this.dbAdaptorFactory = new MongoDBAdaptorFactory(configuration);
        this.variableSetMap = new HashMap<>();
        this.converter = new GenericDocumentComplexConverter<>(AnnotableForMigration.class);
    }

    public void migrate() throws CatalogException {
        logger.info("Starting migration of annotations from samples...");
        MongoCursor<Document> iterator = getAnnotatedDocuments(dbAdaptorFactory.getCatalogSampleDBAdaptor().getSampleCollection(), true);
        migrateAnnotations(dbAdaptorFactory.getCatalogSampleDBAdaptor(),
                dbAdaptorFactory.getCatalogSampleDBAdaptor().getSampleCollection(), iterator);

        logger.info("Starting migration of annotations from individuals...");
        iterator = getAnnotatedDocuments(dbAdaptorFactory.getCatalogIndividualDBAdaptor().getIndividualCollection(), true);
        migrateAnnotations(dbAdaptorFactory.getCatalogIndividualDBAdaptor(),
                dbAdaptorFactory.getCatalogIndividualDBAdaptor().getIndividualCollection(), iterator);

        logger.info("Starting migration of annotations from cohorts...");
        iterator = getAnnotatedDocuments(dbAdaptorFactory.getCatalogCohortDBAdaptor().getCohortCollection(), false);
        migrateAnnotations(dbAdaptorFactory.getCatalogCohortDBAdaptor(),
                dbAdaptorFactory.getCatalogCohortDBAdaptor().getCohortCollection(), iterator);

        logger.info("Starting migration of annotations from families...");
        iterator = getAnnotatedDocuments(dbAdaptorFactory.getCatalogFamilyDBAdaptor().getFamilyCollection(), true);
        migrateAnnotations(dbAdaptorFactory.getCatalogFamilyDBAdaptor(),
                dbAdaptorFactory.getCatalogFamilyDBAdaptor().getFamilyCollection(), iterator);
    }

    private void migrateAnnotations(AnnotationMongoDBAdaptor dbAdaptor, MongoDBCollection collection, MongoCursor<Document> iterator)
            throws CatalogException {

        Document update = new Document("$unset", new Document("annotationSets", ""));

        while(iterator.hasNext()) {
            Document next = iterator.next();
            AnnotableForMigration annotable = converter.convertToDataModelType(next);

            // Remove old annotationSets field from the entry
            Document query = new Document("id", annotable.getId());
            collection.update(query, update, new QueryOptions("multi", true));

            List<ObjectMap> annotationSetAsMap = annotable.getAnnotationSetAsMap();

            try {
                for (ObjectMap annotationSetMap : annotationSetAsMap) {
                    String annotationSetName = annotationSetMap.getString("name");
                    long variableSetId = annotationSetMap.getLong("variableSetId");
                    Map<String, Object> annotations = annotationSetMap.getMap("annotations");

                    VariableSet variableSet = getVariableSet(variableSetId);

                    AnnotationSet annotationSet = new AnnotationSet(annotationSetName, variableSetId, annotations, Collections.emptyMap());
                    CatalogAnnotationsValidator.checkAnnotationSet(variableSet, annotationSet, null);

                    QueryResult<AnnotationSet> annotationSetQR = dbAdaptor.createAnnotationSet(annotable.getId(), variableSet,
                            annotationSet);


                    if (annotationSetQR.getNumResults() == 0) {
                        throw new CatalogException("Migration of annotation set " + annotationSetName + " from entry " + annotable.getId()
                                + "failed.");
                    }
                }
            } catch (CatalogException e) {
                update = new Document()
                        .append("$set", new Document("annotationSets", next.get("annotationSets")))
                        .append("$unset", AnnotationMongoDBAdaptor.AnnotationSetParams.ANNOTATION_SETS.key());
                // Restore annotations
                collection.update(query, update, new QueryOptions("multi", true));

                throw e;
            }

        }
    }

    private VariableSet getVariableSet(long variableSetId) throws CatalogDBException {
        if (!this.variableSetMap.containsKey(variableSetId)) {
            QueryResult<VariableSet> variableSet = dbAdaptorFactory.getCatalogStudyDBAdaptor().getVariableSet(variableSetId,
                    new QueryOptions());
            if (variableSet.getNumResults() == 0) {
                throw new CatalogDBException("Variable set " + variableSetId + " not found. Migration of annotationSets failed");
            }
            variableSetMap.put(variableSetId, variableSet.first());
        }
        return variableSetMap.get(variableSetId);
    }

    private MongoCursor<Document> getAnnotatedDocuments(MongoDBCollection sampleCollection, boolean version) {
        Document queryDocument = new Document("annotationSets", new Document("$exists", true).append("$ne", Collections.emptyList()));
        if (version) {
            queryDocument.append("_lastOfVersion", true);
        }
        QueryOptions options = new QueryOptions(QueryOptions.INCLUDE, Arrays.asList("annotationSets", "id", "_studyId"));
        return sampleCollection.nativeQuery().find(queryDocument, options).iterator();
    }

}
