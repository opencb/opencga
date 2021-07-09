package org.opencb.opencga.catalog.templates;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import org.apache.commons.lang3.StringUtils;
import org.opencb.commons.datastore.core.ObjectMap;
import org.opencb.commons.datastore.core.QueryParam;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.lang.reflect.Field;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.*;
import java.util.stream.Collectors;

public class TemplateEntryIterator<T> implements Iterator<T>, AutoCloseable {

    private final Path path;
    private final Class<T> clazz;
    private final Logger logger;

    private final Format format;
    private BufferedReader br;
    private ObjectMapper objectMapper;
    private T next;

    // Fields only necessary for the TXT parser
    private Map<String, Type> allowedFields;
    private Map<Integer, String> fieldPosition;
    private ObjectMap additionalParams;

    enum Format {
        JSON,
        YAML,
        TXT,
        NONE
    };

    public TemplateEntryIterator(Path path, String entity, Class<T> clazz) {
        this.clazz = clazz;
        this.logger = LoggerFactory.getLogger(TemplateEntryIterator.class);

        if (path.resolve(entity + ".json").toFile().exists()) {
            this.path = path.resolve(entity + ".json");
            this.format = Format.JSON;
        } else if (path.resolve(entity + ".yaml").toFile().exists()) {
            this.path = path.resolve(entity + ".yaml");
            this.format = Format.YAML;
        } else if (path.resolve(entity + ".txt").toFile().exists()) {
            this.path = path.resolve(entity + ".txt");
            this.format = Format.TXT;
        } else {
            this.path = null;
            this.format = Format.NONE;
        }
    }

    @Override
    public boolean hasNext() {
        if (format == Format.NONE) {
            return false;
        }
        if (br == null) {
            initBufferedReader();
        }
        try {
            return processNext();
        } catch (IOException e) {
            logger.error(e.getMessage(), e);
            return false;
        }
    }

    private void initBufferedReader() {
        try {
            br = new BufferedReader(new FileReader(path.toFile()));
        } catch (FileNotFoundException e) {
            logger.error("Could not initialised BufferedReader: '{}'", e.getMessage(), e);
        }

        switch (format) {
            case JSON:
            case TXT:
                this.objectMapper = new ObjectMapper();
                break;
            case YAML:
                this.objectMapper = new ObjectMapper(new YAMLFactory());
                break;
            default:
                throw new IllegalStateException("Unexpected format '" + format + "'");
        }
    }

    private boolean processNext() throws IOException {
        if (br == null) {
            return false;
        }
        switch (format) {
            case JSON:
                this.next = readNextJsonLine();
                break;
            case YAML:
                this.next = readNextYamlLine();
                break;
            case TXT:
                this.next = readNextTxtLine();
                break;
            default:
                throw new IllegalStateException("Unexpected format '" + format + "'");
        }

        return this.next != null;
    }

    @Override
    public T next() {
        T entry = this.next;
        this.next = null;
        return entry;
    }

    private T readNextJsonLine() {
        String jsonLine = readNextLine();
        if (jsonLine == null) {
            return null;
        }

        try {
            return objectMapper.readValue(jsonLine, clazz);
        } catch (JsonProcessingException e) {
            logger.error("JSON parse exception: {}", e.getMessage(), e);
            return null;
        }
    }

    private T readNextYamlLine() {
        String yamlLine = readNextLine();
        if (yamlLine == null) {
            return null;
        }

        try {
            return objectMapper.readValue(yamlLine, clazz);
        } catch (JsonProcessingException e) {
            logger.error("YAML parse exception: {}", e.getMessage(), e);
            return null;
        }
    }

    private T readNextTxtLine() throws IOException {
        if (allowedFields == null) {
            // This is only executed the first time

            // 1. Fetch all data model fields
            allowedFields = new HashMap<>();
            getDeclaredFields(clazz, "", allowedFields);

            // 2. Process header
            fieldPosition = new HashMap<>();
            String line = readNextLine();
            if (line == null || !line.startsWith("#")) {
                throw new IllegalStateException("Missing header line. Header line should start with '#'");
            }
            // Remove first #
            line = line.substring(1);

            String[] split = line.split("\t");
            for (int i = 0; i < split.length; i++) {
                String field = split[i];
                if (!allowedFields.containsKey(field)) {
                    throw new IllegalStateException("Unexpected field '" + field + "'");
                }
                if (allowedFields.get(field).isFile()) {
                    throw new IllegalStateException("Unexpected field '" + field + "'. The list of '" + field + "' should be passed in "
                            + "a different file.");
                }
                fieldPosition.put(i, field);
            }

            // 3. Process additional parameters (fields that are list of something requiring an additional file)
            additionalParams = new ObjectMap();
            processAdditionalTxtFiles(allowedFields, additionalParams);

            // 4. Process permissions

        }

        String line = readNextLine();
        if (line == null) {
            return null;
        }

        String[] split = line.split("\t");

        ObjectMap map = new ObjectMap();
        // Process columns
        for (int i = 0; i < split.length; i++) {
            Type type = allowedFields.get(fieldPosition.get(i));
            Object value = getValue(split[i], type);
            map.putNested(fieldPosition.get(i), value, true);
        }

        String id = map.getString("id");
        // Add additional field values
        if (additionalParams.containsKey(id)) {
            map.putAll(additionalParams.getMap(id));
            additionalParams.remove(id);
        }

        try {
            return objectMapper.readValue(map.toJson(), clazz);
        } catch (JsonProcessingException e) {
            logger.error("Map parse exception: {}", e.getMessage(), e);
            return null;
        }
    }

    private void processAdditionalTxtFiles(Map<String, Type> allowedFields, ObjectMap additionalParams) throws IOException {
        for (Map.Entry<String, Type> entry : allowedFields.entrySet()) {
            if (entry.getValue().isFile()) {
                Path filePath = Paths.get(path.toString().replace("txt", entry.getKey() + ".txt"));
                if (filePath.toFile().exists()) {
                    readAdditionalTxtFile(filePath, entry.getKey(), entry.getValue(), additionalParams);
                }
            }
        }
    }

    private void readAdditionalTxtFile(Path path, String rootField, Type fieldType, ObjectMap additionalParams) throws IOException {
        try (BufferedReader reader = new BufferedReader(new FileReader(path.toFile()))) {
            String header = reader.readLine();
            if (!header.startsWith("#")) {
                throw new IllegalStateException("Missing header line in file '" + path + "'. Header line should start with '#'");
            }
            // Remove first #
            header = header.substring(1);

            // Validate header fields
            Map<String, Type> typeMap = fieldType.getTypeMap();
            Map<Integer, String> auxFieldPosition = new HashMap<>();

            String[] split = header.split("\t");
            // 1st column should be the main entry id, so we start checking column 2
            for (int i = 1; i < split.length; i++) {
                String field = split[i];
                if (!typeMap.containsKey(field)) {
                    throw new IllegalStateException("Unexpected field '" + field + "' in file '" + path + "'");
                }
                auxFieldPosition.put(i, field);
            }

            String line;
            while ((line = reader.readLine()) != null) {
                split = line.split("\t");

                ObjectMap map = new ObjectMap();
                for (int i = 1; i < split.length; i++) {
                    Type type = typeMap.get(auxFieldPosition.get(i));
                    Object value = getValue(split[i], type);
                    map.put(auxFieldPosition.get(i), value);
                }

                String entryId = split[0];
                String fullKey = entryId + "." + rootField;

                Object myList = additionalParams.getNested(fullKey);
                if (myList == null) {
                    myList = new LinkedList<>();
                    additionalParams.putNested(fullKey, myList, true);
                }

                ((List) myList).add(map);
            }
        }
    }

    /**
     * Cast value to its type.
     *
     * @param value String value.
     * @param type  Type instance containing the actual value type.
     * @return the value properly casted to the type.
     */
    private Object getValue(String value, Type type) {
        switch (type.getType()) {
            case TEXT:
                return value;
            case TEXT_ARRAY:
                return Arrays.asList(value.split(","));
            case INTEGER:
                return Integer.parseInt(value);
            case INTEGER_ARRAY:
                String[] split = value.split(",");
                return Arrays.stream(split).map(Integer::parseInt).collect(Collectors.toList());
            case LONG:
                return Long.parseLong(value);
            case LONG_ARRAY:
                split = value.split(",");
                return Arrays.stream(split).map(Long::parseLong).collect(Collectors.toList());
            case BOOLEAN:
                return Boolean.parseBoolean(value);
            case BOOLEAN_ARRAY:
                split = value.split(",");
                return Arrays.stream(split).map(Boolean::getBoolean).collect(Collectors.toList());
            case OBJECT:
                try {
                    return objectMapper.readValue(value, ObjectMap.class);
                } catch (JsonProcessingException e) {
                    throw new RuntimeException("Could not cast JSON value '" + value + "'");
                }
            default:
                throw new IllegalStateException("Unexpected type '" + type.getType() + "'");
        }
    }

    private String readNextLine() {
        try {
            return br.readLine();
        } catch (IOException e) {
            logger.debug("End of BufferedReader? : '{}'", e.getMessage(), e);
        }
        return null;
    }

    @Override
    public void close() {
        if (br != null) {
            try {
                br.close();
            } catch (IOException e) {
                logger.error("Could not close BufferedReader: '{}'", e.getMessage(), e);
            }
        }
    }

    private static class Type {
        private QueryParam.Type type;
        private boolean file;
        private Map<String, Type> typeMap;

        Type() {
        }

        Type(QueryParam.Type type) {
            this.type = type;
        }

        Type(boolean file, Map<String, Type> typeMap) {
            this.file = file;
            this.typeMap = typeMap;
        }

        public QueryParam.Type getType() {
            return type;
        }

        public Type setType(QueryParam.Type type) {
            this.type = type;
            return this;
        }

        public boolean isFile() {
            return file;
        }

        public Type setFile(boolean file) {
            this.file = file;
            return this;
        }

        public Map<String, Type> getTypeMap() {
            return typeMap;
        }

        public Type setTypeMap(Map<String, Type> typeMap) {
            this.typeMap = typeMap;
            return this;
        }
    }

    private void getDeclaredFields(Class<?> clazz, String field, Map<String, Type> map) {
        Field[] declaredFields = clazz.getDeclaredFields();
        for (Field declaredField : declaredFields) {
            // Ignore avro data models
            if (declaredField.getType().getName().equals("org.apache.avro.Schema")) {
                continue;
            }

            String key = getMapKey(field, declaredField.getName());
            if (declaredField.getType().getName().startsWith("org.opencb") && !declaredField.getType().isEnum()
                    && !declaredField.getType().getName().endsWith("ObjectMap")) {
                getDeclaredFields(declaredField.getType(), key, map);
            } else if (declaredField.getType().getName().endsWith("List")) {
                String subclassStr = declaredField.getAnnotatedType().getType().getTypeName();
                subclassStr = subclassStr.substring(subclassStr.indexOf("<") + 1, subclassStr.length() - 1);
                Class subclass;
                try {
                    subclass = Class.forName(subclassStr);
                } catch (ClassNotFoundException e) {
                    throw new RuntimeException("Could not obtain class for " + subclassStr);
                }
                if (subclass.getName().startsWith("org.opencb")) {
                    Map<String, Type> subMap = new HashMap<>();
                    map.put(key, new Type(true, subMap));
                    getDeclaredFields(subclass, "", subMap);
                } else {
                    QueryParam.Type type = getType(declaredField.getType(), subclass);
                    map.put(key, new Type(type));

                }
            } else {
                QueryParam.Type type = getType(declaredField.getType());
                map.put(key, new Type(type));
            }
        }
    }

    private String getMapKey(String prefix, String field) {
        return StringUtils.isEmpty(prefix) ? field : prefix + "." + field;
    }

    private QueryParam.Type getType(Class<?> clazz) {
        return getType(clazz, null);
    }

    private QueryParam.Type getType(Class<?> clazz, Class<?> subclazz) {
        boolean isList = false;
        if (clazz.getName().equals("java.util.List")) {
            clazz = subclazz;
            isList = true;
        }

        switch (clazz.getName()) {
            case "java.lang.String":
                return isList ? QueryParam.Type.TEXT_ARRAY : QueryParam.Type.TEXT;
            case "java.lang.Integer":
            case "int":
                return isList ? QueryParam.Type.INTEGER_ARRAY : QueryParam.Type.INTEGER;
            case "java.lang.Double":
            case "double":
            case "java.lang.Float":
            case "float":
            case "java.lang.Long":
            case "long":
                return isList ? QueryParam.Type.LONG_ARRAY : QueryParam.Type.LONG;
            case "java.lang.Boolean":
            case "boolean":
                return isList ? QueryParam.Type.BOOLEAN_ARRAY : QueryParam.Type.BOOLEAN;
            case "java.util.Map":
            case "org.opencb.commons.datastore.core.ObjectMap":
                return QueryParam.Type.OBJECT;
            default:
                if (clazz.isEnum()) {
                    return QueryParam.Type.TEXT;
                }
                throw new IllegalArgumentException("Unsupported type '" + clazz.getName() + "'");
        }
    }
}
