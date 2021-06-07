package org.opencb.opencga.analysis.template;

import org.apache.commons.lang3.StringUtils;
import org.opencb.commons.datastore.core.QueryParam;
import sun.reflect.generics.reflectiveObjects.ParameterizedTypeImpl;

import java.io.Closeable;
import java.lang.reflect.Field;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

public class TemplateEntryIterator<T> implements Iterator<T>, AutoCloseable {

    private final Path path;
    private final Class<T> clazz;

    public TemplateEntryIterator(Path path, Class<T> clazz) {
        this.path = path;
        this.clazz = clazz;
    }

    @Override
    public boolean hasNext() {
        return false;
    }

    @Override
    public T next() {
        return null;
    }

    private T parseTxtFiles() {
        Map<String, Type> allowedFields = new HashMap<>();
        getDeclaredFields(clazz, "", allowedFields);
        return null;
    }

    @Override
    public void close() {

    }

    private static class Type {
        QueryParam.Type type;
        private boolean file;
        private Map<String, Type> typeMap;

        public Type() {
        }

        public Type(QueryParam.Type type) {
            this.type = type;
        }

        public Type(boolean file, Map<String, Type> typeMap) {
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
            String key = getMapKey(field, declaredField.getName());
            if (declaredField.getType().getName().startsWith("org.opencb")) {
                getDeclaredFields(declaredField.getType(), key, map);
            } else if (declaredField.getType().getName().endsWith("List")) {
                Class subclass = (Class<?>) ((ParameterizedTypeImpl) declaredField.getGenericType()).getActualTypeArguments()[0];
                if (subclass.getName().startsWith("org.opencb")) {
                    Map<String, Type> subMap = new HashMap<>();
                    map.put(key, new Type(true, subMap));
                    getDeclaredFields(subclass, "", subMap);
                } else {
                    QueryParam.Type type = getType(declaredField.getType());
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
        switch (clazz.getName()) {
            case "java.lang.String":
                return QueryParam.Type.TEXT;
            case "java.util.List":
                return QueryParam.Type.TEXT_ARRAY;
            case "int":
                return QueryParam.Type.INTEGER;
            case "boolean":
                return QueryParam.Type.BOOLEAN;
            default:
                throw new IllegalArgumentException("Unsupported type '" + clazz.getName() + "'");
        }
    }
}
