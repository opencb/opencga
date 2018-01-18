package org.opencb.opencga.catalog.db.mongodb.converters;

import org.apache.commons.lang3.StringUtils;
import org.bson.Document;
import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.opencga.core.models.Variable;
import org.opencb.opencga.core.models.VariableSet;

import java.util.*;

public class AnnotationConverter {

    static final String ID = "id";
    static final String VALUE = "value";
    static final String ARRAY_LEVEL = "_al";
    static final String COUNT_ELEMENTS = "_c";
    static final String VARIABLE_SET = "_vs";

    private static final String INTERNAL_DELIMITER = "__";

    public List<Document> annotationToDB(VariableSet variableSet, Map<String, Object> annotations) {
        if (variableSet == null || variableSet.getVariables().isEmpty()) {
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
                    Document document = createAnnotation(variableLevel, annotations);
                    addDocumentIfNotEmpty(variableSet, variableLevel, document, documentList);
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
                            document = createAnnotation(auxVariableLevel, annotations);
                            addDocumentIfNotEmpty(variableSet, auxVariableLevel, document, documentList);
                        } else {
                            queue.add(new VariableLevel(tmpVariable, keys, arrayLevel));
                        }
                    }

                    break;
            }
        }

        return documentList;
    }

    public Map<String, Object> fromDBToAnnotation (List<Document> annotationList, QueryOptions options) {
        Map<String, Object> annotationMap = new HashMap<>();
        for (Document annotationDocument : annotationList) {
            if (passTheFilters(annotationDocument, options)) {
                Queue<FromDBToMap> myQueue = new LinkedList<>();

                String[] split = StringUtils.split(String.valueOf(annotationDocument.get(ID)).split(INTERNAL_DELIMITER, 2)[1], ".");
                List<Integer> arrayLevel = annotationDocument.get(ARRAY_LEVEL) != null
                        ? (List<Integer>) annotationDocument.get(ARRAY_LEVEL) : Collections.emptyList();
                List<Object> countElems = annotationDocument.get(COUNT_ELEMENTS) != null
                        ? (List<Object>) annotationDocument.get(COUNT_ELEMENTS) : Collections.emptyList();

                myQueue.add(new FromDBToMap(Arrays.asList(split), arrayLevel, countElems, annotationDocument.get(VALUE), 0, annotationMap));

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
                                            System.out.println("Something weird...");
                                        }
                                        value = annotationValue.get(0);
                                        fromDBToMap.setAnnotationValue(annotationValue.subList(1, annotationValue.size()));
                                    }
//                                } else {
//                                    if (annotationValue.size() > 0)
//                                    value = annotationValue.get(0);
//                                    fromDBToMap.setAnnotationValue(annotationValue.subList(1, annotationValue.size()));
//                                }

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
        return annotationMap;
    }

    public Map<String, Object> fromDBToFlattenedAnnotation (List<Document> annotationList, QueryOptions options) {
        Map<String, Object> annotationMap = new HashMap<>();
        for (Document annotationDocument : annotationList) {
            if (passTheFilters(annotationDocument, options)) {
                annotationMap.put(
                        String.valueOf(annotationDocument.get(ID)).split(INTERNAL_DELIMITER, 2)[1],
                        annotationDocument.get(VALUE)
                );
            }
        }
        return annotationMap;
    }

    private Document createAnnotation (VariableLevel variableLevel, Map<String, Object> annotations) {
        Document document = new Document();
        if (annotations == null) {
            return document;
        }
        List<String> keys = variableLevel.getKeys();
        List<Integer> arrayLevel = variableLevel.getArrayLevel();

        // There are no arrays in this annotation
        if (arrayLevel.isEmpty()) {
            Object annotation = annotations;
            for (int i = 0; i < keys.size(); i++) {
                annotation = ((Map<String, Object>) annotation).get(keys.get(i));
                if (annotation == null) {
                    return document; // No annotation found for the variable
                }
            }
            document.put(VALUE, annotation);
        } else {
            Object annotation = annotations;
            for (int i = 0; i < arrayLevel.get(0) + 1; i++) {
                annotation = ((Map<String, Object>) annotation).get(keys.get(i));
                if (annotation == null) {
                    return document; // No annotation found for the variable
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

    private void addDocumentIfNotEmpty(VariableSet variableSet, VariableLevel variableLevel, Document document,
                                       List<Document> documentList) {
        if (document != null && !document.isEmpty()) {
            if (variableLevel.getArrayLevel().size() > 0) {
                document.put(ARRAY_LEVEL, variableLevel.getArrayLevel());
            }
            document.put(ID, String.valueOf(variableSet.getId()) + INTERNAL_DELIMITER + StringUtils.join(variableLevel.getKeys(), "."));
            document.put(VARIABLE_SET, variableSet.getId());
            documentList.add(document);
        }
    }

    /**
     * Method that will check if the annotation from the document passes the filters given in the query options.
     *
     * @param document Annotation as it comes from the database.
     * @param options QueryOptions object containing the annotations to be included in the final result.
     * @return true if the annotation passes the filters from the options object, false otherwise.
     */
    private boolean passTheFilters(Document document, QueryOptions options) {
        return true;
    }

    private class VariableLevel {

        private Variable variable;
        private List<String> keys;
        private List<Integer> arrayLevel;

        public VariableLevel() {
        }

        public VariableLevel(Variable variable, List<String> keys, List<Integer> arrayLevel) {
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

        public FromDBToMap(List<String> keys, List<Integer> arrayLevel, Object count, Object annotationValue, int depth,
                           Map<String, Object> annotation) {
            this.keys = keys;
            this.arrayLevel = arrayLevel;
            this.count = count;
            this.annotationValue = annotationValue;
            this.depth = depth;
            this.annotation = annotation;
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
