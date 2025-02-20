package org.opencb.opencga.server.generator.openapi.common;

import org.opencb.opencga.server.generator.openapi.models.Definition;
import org.opencb.opencga.server.generator.openapi.models.Property;

import java.lang.reflect.Field;
import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.util.*;

public class SwaggerDefinitionGenerator {

    public static Map<String, Definition> getDefinitions(List<Class<?>> classes) {
        Map<String, Definition> definitions = new HashMap<>();
        Set<Class<?>> processedClasses = new HashSet<>();

        for (Class<?> clazz : classes) {
            if (!definitions.containsKey(clazz.getSimpleName())) {
                definitions.put(clazz.getSimpleName(), generateDefinition(clazz, definitions, processedClasses));
            }
        }

        return definitions;
    }

    private static Definition generateDefinition(Class<?> clazz, Map<String, Definition> definitions, Set<Class<?>> processedClasses) {
        // Evitar procesar clases ya visitadas para prevenir recursión infinita
        if (processedClasses.contains(clazz)) {
            Definition refDefinition = new Definition();
            refDefinition.setRef("#/definitions/" + clazz.getSimpleName());
            return refDefinition; // Retornar una referencia
        }

        processedClasses.add(clazz); // Marcar la clase como procesada
        Definition definition = new Definition();
        Map<String, Property> properties = new LinkedHashMap<>();

        Field[] fields = clazz.getDeclaredFields();
        for (Field field : fields) {
            String fieldName = field.getName();
            Class<?> fieldType = field.getType();

            try {
                // Detectar campos de tipo List
                if (List.class.isAssignableFrom(fieldType)) {
                    ParameterizedType listType = (ParameterizedType) field.getGenericType();
                    Type listArgumentType = listType.getActualTypeArguments()[0];

                    if (listArgumentType instanceof Class<?>) {
                        // Caso: tipo simple dentro de la lista
                        Class<?> listClass = (Class<?>) listArgumentType;

                        if (!isPrimitive(listClass) && !definitions.containsKey(listClass.getSimpleName())) {
                            definitions.put(listClass.getSimpleName(), generateDefinition(listClass, definitions, processedClasses));
                        }

                        Property listProperty = new Property();
                        listProperty.setType("array");
                        listProperty.setItems(new Property("#/definitions/" + listClass.getSimpleName()));
                        properties.put(fieldName, listProperty);

                    } else if (listArgumentType instanceof ParameterizedType) {
                        // Caso: tipo parametrizado dentro de la lista (ej., List<Map<String, Object>>)
                        ParameterizedType parameterizedType = (ParameterizedType) listArgumentType;
                        Type rawType = parameterizedType.getRawType();

                        if (rawType instanceof Class<?>) {
                            Class<?> rawClass = (Class<?>) rawType;

                            Property listProperty = new Property();
                            listProperty.setType("array");
                            listProperty.setItems(new Property("#/definitions/" + rawClass.getSimpleName()));
                            properties.put(fieldName, listProperty);
                        }
                    } else {
                        System.err.println("Unsupported type argument for field: " + fieldName + " in class " + clazz.getSimpleName());
                    }
                }else if (Map.class.isAssignableFrom(fieldType)) {
                    // Manejar campos de tipo Map como objetos
                    Property mapProperty = new Property();
                    mapProperty.setType("object");
                    properties.put(fieldName, mapProperty);

                } else if (!isPrimitive(fieldType)) {
                    // Manejar tipos complejos anidados
                    if (!definitions.containsKey(fieldType.getSimpleName())) {
                        definitions.put(fieldType.getSimpleName(), generateDefinition(fieldType, definitions, processedClasses));
                    }

                    Property complexProperty = new Property();
                    complexProperty.setRef("#/definitions/" + fieldType.getSimpleName());
                    properties.put(fieldName, complexProperty);

                } else {
                    // Manejar tipos primitivos y simples
                    Property primitiveProperty = new Property();
                    primitiveProperty.setType(mapJavaTypeToSwaggerType(fieldType));
                    properties.put(fieldName, primitiveProperty);
                }
            } catch (Exception e) {
                // Manejar errores inesperados durante la reflexión
                System.err.println("Error processing field: " + fieldName + " in class " + clazz.getName());
                e.printStackTrace();
            }
        }

        definition.setType("object");
        definition.setProperties(properties);
        processedClasses.remove(clazz); // Eliminar la clase del conjunto procesado al terminar
        return definition;
    }

    public static boolean isPrimitive(Class<?> clazz) {
        return clazz.isPrimitive() || clazz == String.class ||
                clazz == Integer.class || clazz == Long.class ||
                clazz == Boolean.class || clazz == Double.class ||
                clazz == Float.class || clazz == Byte.class ||
                clazz == Short.class || clazz == Character.class;
    }

    public static String mapJavaTypeToSwaggerType(Class<?> clazz) {
        if (clazz == String.class) return "string";
        if (clazz == Integer.class || clazz == int.class) return "integer";
        if (clazz == Long.class || clazz == long.class) return "integer";
        if (clazz == Boolean.class || clazz == boolean.class) return "boolean";
        if (clazz == Double.class || clazz == double.class) return "number";
        if (clazz == Float.class || clazz == float.class) return "number";
        if (clazz == Byte.class || clazz == byte.class) return "string";
        if (clazz == Short.class || clazz == short.class) return "integer";
        if (clazz == Character.class || clazz == char.class) return "string";
        return "object"; // Default
    }
}

