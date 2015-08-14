package org.opencb.opencga.catalog.db.mongodb;

import com.mongodb.BasicDBObject;
import com.mongodb.BasicDBObjectBuilder;
import com.mongodb.DBObject;
import com.mongodb.WriteResult;
import org.opencb.datastore.core.ObjectMap;
import org.opencb.datastore.core.QueryOptions;
import org.opencb.datastore.core.QueryResult;
import org.opencb.datastore.mongodb.MongoDBCollection;
import org.opencb.opencga.catalog.db.api.CatalogDBAdaptor;
import org.opencb.opencga.catalog.db.api.CatalogDBAdaptorFactory;
import org.opencb.opencga.catalog.db.api.CatalogSampleDBAdaptor;
import org.opencb.opencga.catalog.exceptions.CatalogDBException;
import org.opencb.opencga.catalog.models.*;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.function.Function;
import java.util.stream.Collectors;

import static org.opencb.opencga.catalog.db.mongodb.CatalogMongoDBAdaptor.*;
import static org.opencb.opencga.catalog.db.mongodb.CatalogMongoDBUtils.*;
import static org.opencb.opencga.catalog.db.mongodb.CatalogMongoDBUtils.getDbObject;
import static org.opencb.opencga.catalog.db.mongodb.CatalogMongoDBUtils.parseObjects;

/**
 * Created by hpccoll1 on 14/08/15.
 */
public class CatalogMongoSampleDBAdaptor extends CatalogDBAdaptor implements CatalogSampleDBAdaptor {


    private final CatalogDBAdaptorFactory dbAdaptorFactory;
    private final MongoDBCollection metaCollection;
    private final MongoDBCollection sampleCollection;
    private MongoDBCollection studyCollection;

    public CatalogMongoSampleDBAdaptor(CatalogDBAdaptorFactory dbAdaptorFactory, MongoDBCollection metaCollection,
                                       MongoDBCollection sampleCollection, MongoDBCollection studyCollection) {
        super(LoggerFactory.getLogger(CatalogSampleDBAdaptor.class));
        this.dbAdaptorFactory = dbAdaptorFactory;
        this.metaCollection = metaCollection;
        this.sampleCollection = sampleCollection;
        this.studyCollection = studyCollection;
    }

    /**
     * Samples methods
     * ***************************
     */

    @Override
    public boolean sampleExists(int sampleId) {
        DBObject query = new BasicDBObject(_ID, sampleId);
        QueryResult<Long> count = sampleCollection.count(query);
        return count.getResult().get(0) != 0;
    }

    @Override
    public QueryResult<Sample> createSample(int studyId, Sample sample, QueryOptions options) throws CatalogDBException {
        long startTime = startQuery();

        dbAdaptorFactory.getCatalogStudyDBAdaptor().checkStudyId(studyId);
        QueryResult<Long> count = sampleCollection.count(
                new BasicDBObject("name", sample.getName()).append(_STUDY_ID, studyId));
        if (count.getResult().get(0) > 0) {
            throw new CatalogDBException("Sample { name: '" + sample.getName() + "'} already exists.");
        }

        int sampleId = getNewAutoIncrementId(metaCollection);
        sample.setId(sampleId);
        sample.setAnnotationSets(Collections.<AnnotationSet>emptyList());
        //TODO: Add annotationSets
        DBObject sampleObject = getDbObject(sample, "sample");
        sampleObject.put(_STUDY_ID, studyId);
        sampleObject.put(_ID, sampleId);
        sampleCollection.insert(sampleObject, null);

        return endQuery("createSample", startTime, getSample(sampleId, options));
    }


    @Override
    public QueryResult<Sample> getSample(int sampleId, QueryOptions options) throws CatalogDBException {
        long startTime = startQuery();
        QueryOptions filteredOptions = filterOptions(options, FILTER_ROUTE_SAMPLES);
        DBObject query = new BasicDBObject(_ID, sampleId);

        QueryResult<DBObject> queryResult = sampleCollection.find(query, filteredOptions);
        List<Sample> samples = parseSamples(queryResult);

        if(samples.isEmpty()) {
            throw CatalogDBException.idNotFound("Sample", sampleId);
        }

        return endQuery("getSample", startTime, samples);
    }

    @Override
    public QueryResult<Sample> getAllSamples(QueryOptions options) throws CatalogDBException {
        int variableSetId = options.getInt("variableSetId");
        Map<String, Variable> variableMap = null;
        if (variableSetId > 0) {
            variableMap = getVariableSet(variableSetId, null).first()
                    .getVariables().stream().collect(Collectors.toMap(Variable::getId, Function.identity()));
        }
        return getAllSamples(variableMap, options);
    }

    @Override
    public QueryResult<Sample> getAllSamples(Map<String, Variable> variableMap, QueryOptions options) throws CatalogDBException {
        long startTime = startQuery();
        String warning = "";

        QueryOptions filteredOptions = filterOptions(options, FILTER_ROUTE_SAMPLES);

        List<DBObject> mongoQueryList = new LinkedList<>();
        List<DBObject> annotationSetFilter = new LinkedList<>();
        for (Map.Entry<String, Object> entry : options.entrySet()) {
            String key = entry.getKey().split("\\.")[0];
            try {
                if (isDataStoreOption(key) || isOtherKnownOption(key)) {
                    continue;   //Exclude DataStore options
                }
                SampleFilterOption option = SampleFilterOption.valueOf(key);
                switch (option) {
                    case id:
                        addCompQueryFilter(option, option.name(), options, _ID, mongoQueryList);
                        break;
                    case studyId:
                        addCompQueryFilter(option, option.name(), options, _STUDY_ID, mongoQueryList);
                        break;
                    case annotationSetId:
                        addCompQueryFilter(option, option.name(), options, "id", annotationSetFilter);
                        break;
                    case variableSetId:
                        addCompQueryFilter(option, option.name(), options, option.getKey(), annotationSetFilter);
                        break;
                    case annotation:
                        addAnnotationQueryFilter(option.name(), options, annotationSetFilter, variableMap);
                        break;
                    default:
                        String optionsKey = entry.getKey().replaceFirst(option.name(), option.getKey());
                        addCompQueryFilter(option, entry.getKey(), options, optionsKey, mongoQueryList);
                        break;
                }
            } catch (IllegalArgumentException e) {
                throw new CatalogDBException(e);
            }
        }

        DBObject query = new BasicDBObject();

        if (!annotationSetFilter.isEmpty()) {
            query.put("annotationSets", new BasicDBObject("$elemMatch", new BasicDBObject("$and", annotationSetFilter)));
        }
        if (!mongoQueryList.isEmpty()) {
            query.put("$and", mongoQueryList);
        }
        logger.debug("GetAllSamples query: {}", query);

        QueryResult<DBObject> queryResult = sampleCollection.find(query, filteredOptions);
        List<Sample> samples = parseSamples(queryResult);

        QueryResult<Sample> result = endQuery("getAllSamples", startTime, samples, null, warning.isEmpty() ? null : warning);
        result.setNumTotalResults(queryResult.getNumTotalResults());
        return result;
    }

    @Override
    public QueryResult<Sample> modifySample(int sampleId, QueryOptions parameters) throws CatalogDBException {
        long startTime = startQuery();

        Map<String, Object> sampleParams = new HashMap<>();

        String[] acceptedParams = {"source", "description"};
        filterStringParams(parameters, sampleParams, acceptedParams);

        String[] acceptedIntParams = {"individualId"};
        filterIntParams(parameters, sampleParams, acceptedIntParams);

        String[] acceptedMapParams = {"attributes"};
        filterMapParams(parameters, sampleParams, acceptedMapParams);

        if (sampleParams.containsKey("individualId")) {
            if (!dbAdaptorFactory.getCatalogIndividualDBAdaptor().individualExists(parameters.getInt("individualId"))) {
                throw CatalogDBException.idNotFound("Individual", parameters.getInt("individualId"));
            }
        }

        if(!sampleParams.isEmpty()) {
            QueryResult<WriteResult> update = sampleCollection.update(new BasicDBObject(_ID , sampleId),
                    new BasicDBObject("$set", sampleParams), null);
            if (update.getResult().isEmpty() || update.getResult().get(0).getN() == 0) {
                throw CatalogDBException.idNotFound("Sample", sampleId);
            }
        }

        return endQuery("Modify cohort", startTime, getSample(sampleId, parameters));
    }

    @Override
    public QueryResult<Sample> deleteSample(int sampleId) throws CatalogDBException {
        long startTime = startQuery();

        QueryResult<Sample> sampleQueryResult = getSample(sampleId, null);

        checkInUse(sampleId);
        WriteResult id = sampleCollection.remove(new BasicDBObject(_ID, sampleId), null).getResult().get(0);
        if (id.getN() == 0) {
            throw CatalogDBException.idNotFound("Sample", sampleId);
        } else {
            return endQuery("delete sample", startTime, sampleQueryResult);
        }
    }

    public void checkInUse(int sampleId) throws CatalogDBException {
        int studyId = getStudyIdBySampleId(sampleId);

        QueryOptions query = new QueryOptions(FileFilterOption.sampleIds.toString(), sampleId);
        QueryOptions queryOptions = new QueryOptions("include", Arrays.asList("projects.studies.files.id", "projects.studies.files.path"));
        QueryResult<File> fileQueryResult = dbAdaptorFactory.getCatalogFileDBAdaptor().getAllFiles(query, queryOptions);
        if (fileQueryResult.getNumResults() != 0) {
            String msg = "Can't delete Sample " + sampleId + ", still in use in \"sampleId\" array of files : " +
                    fileQueryResult.getResult().stream()
                            .map(file -> "{ id: " + file.getId() + ", path: \"" + file.getPath() + "\" }")
                            .collect(Collectors.joining(", ", "[", "]"));
            throw new CatalogDBException(msg);
        }


        queryOptions = new QueryOptions(CohortFilterOption.samples.toString(), sampleId)
                .append("include", Arrays.asList("projects.studies.cohorts.id", "projects.studies.cohorts.name"));
        QueryResult<Cohort> cohortQueryResult = getAllCohorts(studyId, queryOptions);
        if (cohortQueryResult.getNumResults() != 0) {
            String msg = "Can't delete Sample " + sampleId + ", still in use in cohorts : " +
                    cohortQueryResult.getResult().stream()
                            .map(cohort -> "{ id: " + cohort.getId() + ", name: \"" + cohort.getName() + "\" }")
                            .collect(Collectors.joining(", ", "[", "]"));
            throw new CatalogDBException(msg);
        }

    }


    public int getStudyIdBySampleId(int sampleId) throws CatalogDBException {
        DBObject query = new BasicDBObject(_ID, sampleId);
        BasicDBObject projection = new BasicDBObject(_STUDY_ID, true);
        QueryResult<DBObject> queryResult = sampleCollection.find(query, projection, null);
        if (!queryResult.getResult().isEmpty()) {
            Object studyId = queryResult.getResult().get(0).get(_STUDY_ID);
            return studyId instanceof Number ? ((Number) studyId).intValue() : (int) Double.parseDouble(studyId.toString());
        } else {
            throw CatalogDBException.idNotFound("Sample", sampleId);
        }
    }

    /*
     * Cohorts methods
     * ***************************
     */

    @Override
    public QueryResult<Cohort> createCohort(int studyId, Cohort cohort) throws CatalogDBException {
        long startTime = startQuery();
        dbAdaptorFactory.getCatalogStudyDBAdaptor().checkStudyId(studyId);

        QueryResult<Long> count = studyCollection.count(BasicDBObjectBuilder
                .start(_ID, studyId)
                .append("cohorts.name", cohort.getName())
                .get());

        if(count.getResult().get(0) > 0) {
            throw new CatalogDBException("Cohort { name: \"" + cohort.getName() + "\" } already exists in this study.");
        }

        int newId = getNewAutoIncrementId(metaCollection);;
        cohort.setId(newId);

        DBObject cohortObject = getDbObject(cohort, "Cohort");
        QueryResult<WriteResult> update = studyCollection.update(
                new BasicDBObject(_ID, studyId),
                new BasicDBObject("$push", new BasicDBObject("cohorts", cohortObject)), null);

        if (update.getResult().get(0).getN() == 0) {
            throw CatalogDBException.idNotFound("Study", studyId);
        }

        return endQuery("createCohort", startTime, getCohort(newId));
    }

    @Override
    public QueryResult<Cohort> getCohort(int cohortId) throws CatalogDBException {
        long startTime = startQuery();

        BasicDBObject query = new BasicDBObject("cohorts.id", cohortId);
        BasicDBObject projection = new BasicDBObject("cohorts", new BasicDBObject("$elemMatch", new BasicDBObject("id", cohortId)));
        QueryResult<DBObject> queryResult = studyCollection.find(query, projection, null);

        List<Study> studies = parseStudies(queryResult);
        if(studies == null || studies.get(0).getCohorts().isEmpty()) {
            throw CatalogDBException.idNotFound("Cohort", cohortId);
        } else {
            return endQuery("getCohort", startTime, studies.get(0).getCohorts());
        }
    }

    @Override
    public QueryResult<Cohort> getAllCohorts(int studyId, QueryOptions options) throws CatalogDBException {
        long startTime = startQuery();

        List<DBObject> mongoQueryList = new LinkedList<>();
        options.put(CohortFilterOption.studyId.toString(), studyId);
        for (Map.Entry<String, Object> entry : options.entrySet()) {
            String key = entry.getKey().split("\\.")[0];
            try {
                if (isDataStoreOption(key) || isOtherKnownOption(key)) {
                    continue;   //Exclude DataStore options
                }
                CohortFilterOption option = CohortFilterOption.valueOf(key);
                switch (option) {
                    case studyId:
                        addCompQueryFilter(option, option.name(), options, _ID, mongoQueryList);
                        break;
                    default:
                        String optionsKey = "cohorts." + entry.getKey().replaceFirst(option.name(), option.getKey());
                        addCompQueryFilter(option, entry.getKey(), options, optionsKey, mongoQueryList);
                        break;
                }
            } catch (IllegalArgumentException e) {
                throw new CatalogDBException(e);
            }
        }
//        System.out.println("match = " + new BasicDBObject(_ID, studyId).append("$and", mongoQueryList));
        QueryResult<DBObject> queryResult = studyCollection.aggregate(Arrays.<DBObject>asList(
                new BasicDBObject("$match", new BasicDBObject(_ID, studyId)),
                new BasicDBObject("$project", new BasicDBObject("cohorts", 1)),
                new BasicDBObject("$unwind", "$cohorts"),
                new BasicDBObject("$match", new BasicDBObject("$and", mongoQueryList))
        ), filterOptions(options, FILTER_ROUTE_STUDIES));

        List<Cohort> cohorts = parseObjects(queryResult, Study.class).stream().map((study) -> study.getCohorts().get(0)).collect(Collectors.toList());

        return endQuery("getAllCohorts", startTime, cohorts);
    }

    @Override
    public QueryResult<Cohort> modifyCohort(int cohortId, ObjectMap parameters) throws CatalogDBException {
        long startTime = startQuery();

        Map<String, Object> cohortParams = new HashMap<>();

        String[] acceptedParams = {"description", "name", "creationDate"};
        filterStringParams(parameters, cohortParams, acceptedParams);

        Map<String, Class<? extends Enum>> acceptedEnums = Collections.singletonMap("type", Cohort.Type.class);
        filterEnumParams(parameters, cohortParams, acceptedEnums);

        String[] acceptedIntegerListParams = {"samples"};
        filterIntegerListParams(parameters, cohortParams, acceptedIntegerListParams);
        if (parameters.containsKey("samples")) {
            for (Integer sampleId : parameters.getAsIntegerList("samples")) {
                if (!sampleExists(sampleId)) {
                    throw CatalogDBException.idNotFound("Sample", sampleId);
                }
            }
        }

        String[] acceptedMapParams = {"attributes", "stats"};
        filterMapParams(parameters, cohortParams, acceptedMapParams);

        Map<String, Class<? extends Enum>> acceptedEnumParams = Collections.singletonMap("status", Cohort.Status.class);
        filterEnumParams(parameters, cohortParams, acceptedEnumParams);

        if(!cohortParams.isEmpty()) {
            HashMap<Object, Object> studyRelativeCohortParameters = new HashMap<>();
            for (Map.Entry<String, Object> entry : cohortParams.entrySet()) {
                studyRelativeCohortParameters.put("cohorts.$." + entry.getKey(), entry.getValue());
            }
            QueryResult<WriteResult> update = studyCollection.update(new BasicDBObject("cohorts.id" , cohortId),
                    new BasicDBObject("$set", studyRelativeCohortParameters), null);
            if (update.getResult().isEmpty() || update.getResult().get(0).getN() == 0) {
                throw CatalogDBException.idNotFound("Cohort", cohortId);
            }
        }

        return endQuery("Modify cohort", startTime, getCohort(cohortId));
    }

    @Override
    public QueryResult<Cohort> deleteCohort(int cohortId, ObjectMap queryOptions) throws CatalogDBException {
        long startTime = startQuery();

//        checkCohortInUse(cohortId);
        int studyId = getStudyIdByCohortId(cohortId);
        QueryResult<Cohort> cohort = getCohort(cohortId);

        QueryResult<WriteResult> update = studyCollection.update(new BasicDBObject(_ID, studyId), new BasicDBObject("$pull", new BasicDBObject("cohorts", new BasicDBObject("id", cohortId))), null);

        if (update.first().getN() == 0) {
            throw CatalogDBException.idNotFound("Cohhort", cohortId);
        }

        return endQuery("Delete Cohort", startTime, cohort);

    }

    @Override
    public int getStudyIdByCohortId(int cohortId) throws CatalogDBException {
        BasicDBObject query = new BasicDBObject("cohorts.id", cohortId);
        QueryResult<DBObject> queryResult = studyCollection.find(query, new BasicDBObject("id", true), null);
        if(queryResult.getResult().isEmpty() || !queryResult.getResult().get(0).containsField("id")) {
            throw CatalogDBException.idNotFound("Cohort", cohortId);
        } else {
            Object id = queryResult.getResult().get(0).get("id");
            return id instanceof Number ? ((Number) id).intValue() : (int) Double.parseDouble(id.toString());
        }
    }

    /*
     * Variables Methods
     * ***************************
     */

    @Override
    public QueryResult<VariableSet> createVariableSet(int studyId, VariableSet variableSet) throws CatalogDBException {
        long startTime = startQuery();

        QueryResult<Long> count = studyCollection.count(
                new BasicDBObject("variableSets.name", variableSet.getName()).append("id", studyId));
        if (count.getResult().get(0) > 0) {
            throw new CatalogDBException("VariableSet { name: '" + variableSet.getName() + "'} already exists.");
        }

        int variableSetId = getNewAutoIncrementId(metaCollection);
        variableSet.setId(variableSetId);
        DBObject object = getDbObject(variableSet, "VariableSet");
        DBObject query = new BasicDBObject(_ID, studyId);
        DBObject update = new BasicDBObject("$push", new BasicDBObject("variableSets", object));

        QueryResult<WriteResult> queryResult = studyCollection.update(query, update, null);

        return endQuery("createVariableSet", startTime, getVariableSet(variableSetId, null));
    }

    @Override
    public QueryResult<VariableSet> getVariableSet(int variableSetId, QueryOptions options) throws CatalogDBException {
        long startTime = startQuery();

        QueryOptions filteredOptions = filterOptions(options, FILTER_ROUTE_STUDIES);
        DBObject query = new BasicDBObject("variableSets.id", variableSetId);
        DBObject projection = new BasicDBObject(
                "variableSets",
                new BasicDBObject(
                        "$elemMatch",
                        new BasicDBObject("id", variableSetId)
                )
        );
        QueryResult<DBObject> queryResult = studyCollection.find(query, projection, filteredOptions);
        List<Study> studies = parseStudies(queryResult);
        if(studies.isEmpty() || studies.get(0).getVariableSets().isEmpty()) {
            throw new CatalogDBException("VariableSet {id: " + variableSetId + "} does not exist");
        }

        return endQuery("", startTime, studies.get(0).getVariableSets());
    }

    @Override
    public QueryResult<VariableSet> getAllVariableSets(int studyId, QueryOptions options) throws CatalogDBException {
        long startTime = startQuery();

        List<DBObject> mongoQueryList = new LinkedList<>();


        for (Map.Entry<String, Object> entry : options.entrySet()) {
            String key = entry.getKey().split("\\.")[0];
            try {
                if (isDataStoreOption(key) || isOtherKnownOption(key)) {
                    continue;   //Exclude DataStore options
                }
                VariableSetFilterOption option = VariableSetFilterOption.valueOf(key);
                switch (option) {
                    case studyId:
                        addCompQueryFilter(option, option.name(), options, _ID, mongoQueryList);
                        break;
                    default:
                        String optionsKey = "variableSets." + entry.getKey().replaceFirst(option.name(), option.getKey());
                        addCompQueryFilter(option, entry.getKey(), options, optionsKey, mongoQueryList);
                        break;
                }
            } catch (IllegalArgumentException e) {
                throw new CatalogDBException(e);
            }
        }

        QueryResult<DBObject> queryResult = studyCollection.aggregate(Arrays.<DBObject>asList(
                new BasicDBObject("$match", new BasicDBObject(_ID, studyId)),
                new BasicDBObject("$project", new BasicDBObject("variableSets", 1)),
                new BasicDBObject("$unwind", "$variableSets"),
                new BasicDBObject("$match", new BasicDBObject("$and", mongoQueryList))
        ), filterOptions(options, FILTER_ROUTE_STUDIES));

        List<VariableSet> variableSets = parseObjects(queryResult, Study.class).stream().map(study -> study.getVariableSets().get(0)).collect(Collectors.toList());

        return endQuery("", startTime, variableSets);
    }

    @Override
    public QueryResult<VariableSet> deleteVariableSet(int variableSetId, QueryOptions queryOptions) throws CatalogDBException {
        long startTime = startQuery();

        checkVariableSetInUse(variableSetId);
        int studyId = getStudyIdByVariableSetId(variableSetId);
        QueryResult<VariableSet> variableSet = getVariableSet(variableSetId, queryOptions);

        QueryResult<WriteResult> update = studyCollection.update(new BasicDBObject(_ID, studyId), new BasicDBObject("$pull", new BasicDBObject("variableSets", new BasicDBObject("id", variableSetId))), null);

        if (update.first().getN() == 0) {
            throw CatalogDBException.idNotFound("VariableSet", variableSetId);
        }

        return endQuery("Delete VariableSet", startTime, variableSet);

    }


    public void checkVariableSetInUse(int variableSetId) throws CatalogDBException {
        QueryResult<Sample> samples = getAllSamples(new QueryOptions(SampleFilterOption.variableSetId.toString(), variableSetId));
        if (samples.getNumResults() != 0) {
            String msg = "Can't delete VariableSetId, still in use as \"variableSetId\" of samples : [";
            for (Sample sample : samples.getResult()) {
                msg += " { id: " + sample.getId() + ", name: \"" + sample.getName() + "\" },";
            }
            msg += "]";
            throw new CatalogDBException(msg);
        }
    }


    /*
     * Annotations Methods
     * ***************************
     */

    @Override
    public QueryResult<AnnotationSet> annotateSample(int sampleId, AnnotationSet annotationSet) throws CatalogDBException {
        long startTime = startQuery();

        QueryResult<Long> count = sampleCollection.count(
                new BasicDBObject("annotationSets.id", annotationSet.getId()).append(_ID, sampleId));
        if (count.getResult().get(0) > 0) {
            throw CatalogDBException.alreadyExists("AnnotationSet", "id", annotationSet.getId());
        }

        DBObject object = getDbObject(annotationSet, "AnnotationSet");

        DBObject query = new BasicDBObject(_ID, sampleId);
        DBObject update = new BasicDBObject("$push", new BasicDBObject("annotationSets", object));

        QueryResult<WriteResult> queryResult = sampleCollection.update(query, update, null);

        return endQuery("", startTime, Arrays.asList(annotationSet));
    }


    @Override
    public int getStudyIdByVariableSetId(int variableSetId) throws CatalogDBException {
        DBObject query = new BasicDBObject("variableSets.id", variableSetId);

        QueryResult<DBObject> queryResult = studyCollection.find(query, new BasicDBObject("id", true), null);

        if (!queryResult.getResult().isEmpty()) {
            Object id = queryResult.getResult().get(0).get("id");
            return id instanceof Number ? ((Number) id).intValue() : (int) Double.parseDouble(id.toString());
        } else {
            throw CatalogDBException.idNotFound("VariableSet", variableSetId);
        }
    }
}
