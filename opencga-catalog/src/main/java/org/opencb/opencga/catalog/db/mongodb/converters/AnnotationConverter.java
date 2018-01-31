package org.opencb.opencga.catalog.db.mongodb.converters;

import org.apache.commons.lang3.StringUtils;
import org.bson.Document;
import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.opencga.catalog.db.mongodb.AnnotationMongoDBAdaptor;
import org.opencb.opencga.catalog.utils.Constants;
import org.opencb.opencga.core.models.AnnotationSet;
import org.opencb.opencga.core.models.Variable;
import org.opencb.opencga.core.models.VariableSet;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.stream.Collectors;

public class AnnotationConverter {

    static final String ID = AnnotationMongoDBAdaptor.AnnotationSetParams.ID.key();
    static final String VALUE = AnnotationMongoDBAdaptor.AnnotationSetParams.VALUE.key();
    static final String ARRAY_LEVEL = AnnotationMongoDBAdaptor.AnnotationSetParams.ARRAY_LEVEL.key();
    static final String COUNT_ELEMENTS = AnnotationMongoDBAdaptor.AnnotationSetParams.COUNT_ELEMENTS.key();
    static final String VARIABLE_SET = AnnotationMongoDBAdaptor.AnnotationSetParams.VARIABLE_SET_ID.key();
    static final String ANNOTATION_SET_NAME = AnnotationMongoDBAdaptor.AnnotationSetParams.ANNOTATION_SET_NAME.key();

    private static final String INTERNAL_DELIMITER = "__";

    private final Logger logger;

    public AnnotationConverter() {
        this.logger = LoggerFactory.getLogger(AnnotationConverter.class);
    }


    public List<Document> annotationToDB(VariableSet variableSet, AnnotationSet annotationSet) {
        return annotationToDB(variableSet, annotationSet.getName(), annotationSet.getAnnotations());
    }

    public List<Document> annotationToDB(VariableSet variableSet, String annotationSetName, Map<String, Object> annotations) {
        if (variableSet == null || variableSet.getVariables().isEmpty()) {
            logger.error("VariableSet cannot be null or empty");
            throw new RuntimeException("VariableSet cannot be null or empty");
        }

        if (annotations == null || annotations.isEmpty()) {
            return Collections.emptyList();
        }

        // We create a queue to manage all the different levels
        Queue<VariableLevel> queue = new LinkedList<>();

        // We add the first level to the queue first
        for (Variable variable : variableSet.getVariables()) {
            List<Integer> arrayElems;
            if (variable.isMultiValue()) {
                arrayElems = Collections.singletonList(0);
            } else {
                arrayElems = Collections.emptyList();
            }
            queue.add(new VariableLevel(variable, Collections.singletonList(variable.getName()), arrayElems));
        }

        List<Document> documentList = new ArrayList<>();
        // Process the queue
        while (!queue.isEmpty()) {
            VariableLevel variableLevel = queue.remove();

            switch (variableLevel.getVariable().getType()) {
                case BOOLEAN:
                case CATEGORICAL:
                case INTEGER:
                case DOUBLE:
                case TEXT:
                    Document document = createAnnotationDocument(variableLevel, annotations);
                    addDocumentIfNotEmpty(variableSet, annotationSetName, variableLevel, document, documentList);
                    break;
                case OBJECT:
                    Variable variable = variableLevel.getVariable();

                    // Add the new nested variables to the queue to be processed
                    for (Variable tmpVariable : variable.getVariableSet()) {
                        List<Integer> arrayLevel = new ArrayList<>(variableLevel.getArrayLevel());
                        if (tmpVariable.isMultiValue()) {
                            arrayLevel.add(variableLevel.getKeys().size());
                        }

                        List<String> keys = new ArrayList<>(variableLevel.getKeys());
                        keys.add(tmpVariable.getName());

                        if (arrayLevel.size() > 2) {
                            VariableLevel auxVariableLevel = new VariableLevel(tmpVariable, keys, variableLevel.getArrayLevel());
                            // We don't attempt to flatten the arrays anymore
                            document = createAnnotationDocument(auxVariableLevel, annotations);
                            addDocumentIfNotEmpty(variableSet, annotationSetName, auxVariableLevel, document, documentList);
                        } else {
                            queue.add(new VariableLevel(tmpVariable, keys, arrayLevel));
                        }
                    }
                    break;
                default:
                    logger.error("Found a variable with no variable type");
                    throw new RuntimeException("Found a variable with no variable type");
            }
        }

        return documentList;
    }

    public List<AnnotationSet> fromDBToAnnotation(List<Document> annotationList, QueryOptions options) {
        if (annotationList == null || annotationList.isEmpty()) {
            return Collections.emptyList();
        }
        if (options == null) {
            options = new QueryOptions();
        }

        // Store the include y exclude in HashSet to search efficiently
        Set<String> includeSet = new HashSet<>(options.getAsStringList(QueryOptions.INCLUDE, ","));
        Set<String> excludeSet = new HashSet<>(options.getAsStringList(QueryOptions.EXCLUDE, ","));

        Map<String, AnnotationSet> annotationSetMap = new HashMap<>();

        for (Document annotationDocument : annotationList) {
            if (passTheFilters(annotationDocument, includeSet, excludeSet)) {
                Queue<FromDBToMap> myQueue = new LinkedList<>();

                String[] split = StringUtils.split(annotationDocument.getString(ID), ".");
                List<Integer> arrayLevel = annotationDocument.get(ARRAY_LEVEL) != null
                        ? (List<Integer>) annotationDocument.get(ARRAY_LEVEL) : Collections.emptyList();
                List<Object> countElems = annotationDocument.get(COUNT_ELEMENTS) != null
                        ? (List<Object>) annotationDocument.get(COUNT_ELEMENTS) : Collections.emptyList();

                // We create a new annotation set if the map doesn't still contain the key
                String annSetName = (String) annotationDocument.get(AnnotationMongoDBAdaptor.AnnotationSetParams.ANNOTATION_SET_NAME.key());
                long variableSetId = (Long) annotationDocument.get(AnnotationMongoDBAdaptor.AnnotationSetParams.VARIABLE_SET_ID.key());
                String compoundKey = annSetName + INTERNAL_DELIMITER + variableSetId;

                if (!annotationSetMap.containsKey(compoundKey)) {
                    annotationSetMap.put(compoundKey, new AnnotationSet(annSetName, variableSetId, new HashMap<>(),
                            Collections.emptyMap()));
                }

                // We provide the map from the annotation set in order to be automatically filled in
                myQueue.add(new FromDBToMap(Arrays.asList(split), arrayLevel, countElems, annotationDocument.get(VALUE), 0,
                        annotationSetMap.get(compoundKey).getAnnotations()));

                while (!myQueue.isEmpty()) {
                    FromDBToMap fromDBToMap = myQueue.remove();
                    Map<String, Object> annotation = fromDBToMap.getAnnotation();

                    String key = fromDBToMap.getKeys().get(0);
                    if (fromDBToMap.getArrayLevel().size() > 0 && fromDBToMap.getArrayLevel().get(0) == fromDBToMap.getDepth()) {
                        // It is an array
                        if (fromDBToMap.getKeys().size() == 1) { // It is the last key
                            Object value = getAnnotationValue(fromDBToMap);
                            if (value != null) {
                                annotation.put(key, value);
                            }
                        } else {
                            if (!annotation.containsKey(key)) {
                                annotation.put(key, new ArrayList<>());
                            }
                            // Initialize the required maps inside the array
                            for (int i = 0; i < ((List) fromDBToMap.getCount()).size(); i++) {
                                Map<String, Object> auxMap;
                                if (((List) annotation.get(key)).size() < ((List) fromDBToMap.getCount()).size()) {
                                    // It is the first time the array is initialised so we create new empty maps
                                    auxMap = new HashMap<>();
                                    // We add the map to the list
                                    ((List) annotation.get(key)).add(auxMap);
                                } else {
                                    // We get the map from the position i
                                    auxMap = (Map<String, Object>) ((List) annotation.get(key)).get(i);
                                }

                                Object value;
                                List annotationValue = (List) fromDBToMap.getAnnotationValue();

                                Object count = ((List) fromDBToMap.getCount()).get(i);
                                // count can be a futher list of integers or an integer containing the number of elements to be
                                // retrieved from the annotation value list

                                if (count instanceof List) {
                                    // count is a list of integers indicating the number of elements from the annotation value array
                                    // that goes to every different element of the array
                                    int total = 0;
                                    for (Object o : ((List) count)) {
                                        total += (int) o;
                                    }
                                    count = total;
                                }
                                int numberOfElements = (Integer) count;

                                if (numberOfElements > 0) {
                                    if (fromDBToMap.getArrayLevel().size() > 1) {
                                        value = annotationValue.subList(0, numberOfElements);
                                        fromDBToMap.setAnnotationValue(annotationValue.subList(numberOfElements, annotationValue.size()));
                                    } else {
                                        if (numberOfElements > 1) {
                                            logger.warn("Weird behaviour detected. numberOfElements should never have a value above 1: {}",
                                                    fromDBToMap);
                                        }
                                        value = annotationValue.get(0);
                                        fromDBToMap.setAnnotationValue(annotationValue.subList(1, annotationValue.size()));
                                    }

                                    myQueue.add(new FromDBToMap(fromDBToMap.getKeys().subList(1, fromDBToMap.getKeys().size()),
                                            fromDBToMap.getArrayLevel().subList(1, fromDBToMap.getArrayLevel().size()),
                                            ((List) fromDBToMap.getCount()).get(i), value, fromDBToMap.getDepth() + 1, auxMap));

                                }

                            }
                        }
                    } else {
                        // This is the last key
                        if (fromDBToMap.getKeys().size() == 1) {
                            Object value = getAnnotationValue(fromDBToMap);
                            if (value != null) {
                                annotation.put(key, value);
                            }
                        } else {
                            if (!annotation.containsKey(key)) {
                                annotation.put(key, new HashMap<>());
                            }
                            myQueue.add(new FromDBToMap(fromDBToMap.getKeys().subList(1, fromDBToMap.getKeys().size()),
                                    fromDBToMap.getArrayLevel(), fromDBToMap.getCount(), fromDBToMap.getAnnotationValue(),
                                    fromDBToMap.getDepth() + 1, (Map<String, Object>) annotation.get(key)));
                        }
                    }
                }

            }
        }

        return annotationSetMap.entrySet().stream().map(Map.Entry::getValue).collect(Collectors.toList());
    }

    public List<AnnotationSet> fromDBToFlattenedAnnotation(List<Document> annotationList, QueryOptions options) {
        Map<String, AnnotationSet> annotationSetMap = new HashMap<>();

        if (options == null) {
            options = new QueryOptions();
        }

        // Store the include y exclude in HashSet to search efficiently
        Set<String> includeSet = new HashSet<>(options.getAsStringList(QueryOptions.INCLUDE, ","));
        Set<String> excludeSet = new HashSet<>(options.getAsStringList(QueryOptions.EXCLUDE, ","));

        for (Document annotationDocument : annotationList) {
            if (passTheFilters(annotationDocument, includeSet, excludeSet)) {
                String annSetName = (String) annotationDocument.get(AnnotationMongoDBAdaptor.AnnotationSetParams.ANNOTATION_SET_NAME.key());
                long variableSetId = (Long) annotationDocument.get(AnnotationMongoDBAdaptor.AnnotationSetParams.VARIABLE_SET_ID.key());
                String compoundKey = annSetName + INTERNAL_DELIMITER + variableSetId;

                if (!annotationSetMap.containsKey(compoundKey)) {
                    annotationSetMap.put(compoundKey, new AnnotationSet(annSetName, variableSetId, new HashMap<>(),
                            Collections.emptyMap()));
                }

                annotationSetMap.get(compoundKey).getAnnotations().put(annotationDocument.getString(ID), annotationDocument.get(VALUE));
            }
        }

        return annotationSetMap.entrySet().stream().map(Map.Entry::getValue).collect(Collectors.toList());
    }

    private Document createAnnotationDocument(VariableLevel variableLevel, Map<String, Object> annotations) {
        Document document = new Document();
        if (annotations == null) {
            return document;
        }
        List<String> keys = variableLevel.getKeys();
        List<Integer> arrayLevel = variableLevel.getArrayLevel();
        Variable variable = variableLevel.getVariable();

        // There are no arrays in this annotation
        if (arrayLevel.isEmpty()) {
            Object annotation = annotations;
            for (int i = 0; i < keys.size(); i++) {
                annotation = ((Map<String, Object>) annotation).get(keys.get(i));
                if (annotation == null) {
                    if (variable.getDefaultValue() != null && variable.isRequired()) {
                        // If there is a default annotation, we must put it
                        return new Document(VALUE, variable.getDefaultValue());
                    } else {
                        return document; // No annotation found for the variable
                    }
                }
            }
            document.put(VALUE, annotation);
        } else {
            Object annotation = annotations;
            for (int i = 0; i < arrayLevel.get(0) + 1; i++) {
                annotation = ((Map<String, Object>) annotation).get(keys.get(i));
                if (annotation == null) {
                    if (variable.getDefaultValue() != null && variable.isRequired()) {
                        // If there is a default annotation, we must put it
                        return new Document(VALUE, variable.getDefaultValue());
                    } else {
                        return document; // No annotation found for the variable
                    }
                }
                if (!arrayLevel.isEmpty() && arrayLevel.get(0) <= i) {
//                            arrayLevel.isEmpty() || arrayLevel.get(0) > i) {

                    // 1st level of the array found
                    List<Object> values = new ArrayList<>();
                    List<Object> numberElements = new ArrayList<>();

                    List annotationLevel1 = (List) annotation; // a.b[]

                    if (arrayLevel.get(0) + 1 == keys.size()) {
                        // The last element is an array of the primitive value
                        values.addAll(annotationLevel1);
                        numberElements.add(annotationLevel1.size());
                    } else {
                        // It is an array of objects
                        for (Object annotation1 : annotationLevel1) {
                            annotation = annotation1;
                            for (int j = arrayLevel.get(0) + 1;
                                 j < Math.min(keys.size(), arrayLevel.size() == 1 ? Integer.MAX_VALUE : (arrayLevel.get(1) + 1)); j++) {
                                annotation = ((Map<String, Object>) annotation).get(keys.get(j));
                                if (annotation == null) {
                                    numberElements.add(0); // No annotation found for the variable inside the array
                                    break;
                                }
                                if (arrayLevel.size() > 1 && arrayLevel.get(1) <= j) {

                                    List<Integer> numberElementsLevel2 = new ArrayList<>();
                                    List annotationLevel2 = (List) annotation; // a.b.$1.c.d[]

                                    if (arrayLevel.get(1) + 1 == keys.size()) {
                                        // The last element is an array of the primitive value
                                        values.addAll(annotationLevel2);
                                        numberElementsLevel2.add(annotationLevel2.size());
                                    } else {
                                        // It is an array of objects

                                        for (Object annotation2 : annotationLevel2) {
                                            annotation = annotation2;

                                            for (int w = arrayLevel.get(1) + 1; w < keys.size(); w++) {
                                                annotation = ((Map<String, Object>) annotation).get(keys.get(w));
                                                if (annotation == null) {
                                                    numberElementsLevel2.add(0); // No annotation found for the variable inside the array
                                                    break;
                                                }

                                                if (w + 1 == keys.size()) {
                                                    // We found a result
                                                    values.add(annotation);
                                                    numberElementsLevel2.add(1);
                                                }
                                            }

                                        }
                                    }

                                    numberElements.add(numberElementsLevel2);
                                } else if (j + 1 == keys.size()) {
                                    // We found a result
                                    values.add(annotation);
                                    numberElements.add(1);
                                }
                            }
                        }
                    }

                    if (values.size() > 0) {
                        document.put(VALUE, values);
                        document.put(COUNT_ELEMENTS, numberElements);
                    }
                }
            }

        }

        return document;
    }

    private Object getAnnotationValue(FromDBToMap fromDBToMap) {
        if (fromDBToMap.getArrayLevel().size() == 0) {
            return fromDBToMap.getAnnotationValue();
        } else {
            int count = ((List<Integer>) fromDBToMap.getCount()).get(0);
            if (count == 0) {
                return null;
            }
            List<Object> objectList = (List<Object>) fromDBToMap.getAnnotationValue();

            // We modify the annotation so the next element in the array does not find the extracted elements from the array
            fromDBToMap.setAnnotationValue(objectList.subList(count, objectList.size()));

            return objectList.subList(0, count);
        }
    }

    private void addDocumentIfNotEmpty(VariableSet variableSet, String annotationSetName, VariableLevel variableLevel, Document document,
                                       List<Document> documentList) {
        if (document != null && !document.isEmpty()) {
            if (variableLevel.getArrayLevel().size() > 0) {
                document.put(ARRAY_LEVEL, variableLevel.getArrayLevel());
            }
            document.put(ID, StringUtils.join(variableLevel.getKeys(), "."));
            document.put(VARIABLE_SET, variableSet.getId());
            document.put(ANNOTATION_SET_NAME, annotationSetName);
            documentList.add(document);
        }
    }

//    /**
//     * Method that will check if the annotation from the document passes the filters given in the query options.
//     *
//     * @param document Annotation as it comes from the database.
//     * @param options QueryOptions object containing the annotations to be included in the final result.
//     * @return true if the annotation passes the filters from the options object, false otherwise.
//     */
//    private boolean passTheFilters(Document document, QueryOptions options) {
//        if (options == null) {
//            return true;
//        }
//
//        if (options.containsKey(QueryOptions.INCLUDE)) {
//            List<String> includeStringList = options.getAsStringList(QueryOptions.INCLUDE, ",");
//            if (includeStringList != null && includeStringList.size() > 0) {
//                for (String includeString : includeStringList) {
//                    if (checkAnnotationInProjection(document, includeString)) {
//                        return true;
//                    }
//                }
//                return false;
//            }
//        } else if (options.containsKey(QueryOptions.EXCLUDE)) {
//            List<String> excludeStringList = options.getAsStringList(QueryOptions.EXCLUDE, ",");
//            if (excludeStringList != null && excludeStringList.size() > 0) {
//                for (String excludeString : excludeStringList) {
//                    if (checkAnnotationInProjection(document, excludeString)) {
//                        return false;
//                    }
//                }
//                return true;
//            }
//        }
//
//        // There are no projections in the query options
//        return true;
//    }

    private boolean passTheFilters(Document document, Set<String> include, Set<String> exclude) {
        Set<String> projection = Collections.emptySet();
        // passProjectionFilter is a boolean that will be true if the filter to be applied is a include, false if it is exclude.
        // It will be used to return the expected boolean value depending on the operation being applied (include/exclude)
        boolean passProjectionFilter = true;
        if (!include.isEmpty()) {
            projection = include;
        } else if (!exclude.isEmpty()) {
            projection = exclude;
            passProjectionFilter = false;
        }

        // We check the annotation
        String annotation = Constants.ANNOTATION + "." + document.getString(AnnotationMongoDBAdaptor.AnnotationSetParams.ID.key());
        if (projection.contains(annotation)) {
            return passProjectionFilter;
        }

        // We check the annotation set name
        String annotationSetName = Constants.ANNOTATION_SET_NAME + "."
                + document.getString(AnnotationMongoDBAdaptor.AnnotationSetParams.ANNOTATION_SET_NAME.key());
        if (projection.contains(annotationSetName)) {
            return passProjectionFilter;
        }

        // We check the variable set id
        String variableSetId = Constants.VARIABLE_SET + "."
                + document.getLong(AnnotationMongoDBAdaptor.AnnotationSetParams.VARIABLE_SET_ID.key());
        if (projection.contains(variableSetId)) {
            return passProjectionFilter;
        }

        return include.isEmpty();
    }

//    private boolean checkAnnotationInProjection(Document document, String projectionString) {
//        // We check the annotation
//        if (projectionString.startsWith(Constants.ANNOTATION + ".")) {
//            String annotationKey = projectionString.replace(Constants.ANNOTATION + ".", "");
//            if (!annotationKey.isEmpty()
//                    && annotationKey.equals(document.getString(AnnotationMongoDBAdaptor.AnnotationSetParams.ID.key()))) {
//                return true;
//            }
//        }
//
//        // We check the annotation set name
//        if (projectionString.startsWith(Constants.ANNOTATION_SET_NAME + ".")) {
//            String annotationName = projectionString.replace(Constants.ANNOTATION_SET_NAME + ".", "");
//            if (!annotationName.isEmpty()
//                    && annotationName.equals(document.getString(AnnotationMongoDBAdaptor.AnnotationSetParams.ANNOTATION_SET_NAME.key())))
// {
//                return true;
//            }
//        }
//
//        // We check the variable set id
//        if (projectionString.startsWith(Constants.VARIABLE_SET + ".")) {
//            String variableSetString = projectionString.replace(Constants.VARIABLE_SET + ".", "");
//            if (!variableSetString.isEmpty() && StringUtils.isNumeric(variableSetString)
//                    && Long.parseLong(variableSetString) == document.getLong(AnnotationMongoDBAdaptor.AnnotationSetParams.VARIABLE_SET_ID
//                    .key())) {
//                return true;
//            }
//        }
//        return false;
//    }

    private class VariableLevel {

        private Variable variable;
        private List<String> keys;
        private List<Integer> arrayLevel;

        VariableLevel() {
        }

        VariableLevel(Variable variable, List<String> keys, List<Integer> arrayLevel) {
            this.variable = variable;
            this.keys = keys;
            this.arrayLevel = arrayLevel;
        }

        public Variable getVariable() {
            return variable;
        }

        public List<String> getKeys() {
            return keys;
        }

        public List<Integer> getArrayLevel() {
            return arrayLevel;
        }
    }

    private class FromDBToMap {

        private List<String> keys;
        private List<Integer> arrayLevel;
        private Object count;
        private Object annotationValue;
        private int depth;

        private Map<String, Object> annotation;

        FromDBToMap(List<String> keys, List<Integer> arrayLevel, Object count, Object annotationValue, int depth,
                    Map<String, Object> annotation) {
            this.keys = keys;
            this.arrayLevel = arrayLevel;
            this.count = count;
            this.annotationValue = annotationValue;
            this.depth = depth;
            this.annotation = annotation;
        }

        @Override
        public String toString() {
            final StringBuilder sb = new StringBuilder("FromDBToMap{");
            sb.append("keys=").append(keys);
            sb.append(", arrayLevel=").append(arrayLevel);
            sb.append(", count=").append(count);
            sb.append(", annotationValue=").append(annotationValue);
            sb.append(", depth=").append(depth);
            sb.append(", annotation=").append(annotation);
            sb.append('}');
            return sb.toString();
        }

        public List<String> getKeys() {
            return keys;
        }

        public List<Integer> getArrayLevel() {
            return arrayLevel;
        }

        public Object getCount() {
            return count;
        }

        public Object getAnnotationValue() {
            return annotationValue;
        }

        public int getDepth() {
            return depth;
        }

        public Map<String, Object> getAnnotation() {
            return annotation;
        }

        public FromDBToMap setAnnotationValue(Object annotationValue) {
            this.annotationValue = annotationValue;
            return this;
        }
    }

}
