package org.opencb.opencga.server.generator.openapi.common;

import org.opencb.opencga.server.generator.openapi.models.Definition;
import org.opencb.opencga.server.generator.openapi.models.FieldDefinition;
import org.opencb.opencga.server.generator.openapi.models.ListDefinition;
import org.opencb.opencga.server.generator.openapi.models.MapDefinition;

import java.lang.reflect.Field;
import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.util.*;

public class SwaggerDefinitionGenerator {


    private final static Map<String, Definition> definitions = new HashMap<>();
    private final static Set<String> processedFields = new HashSet<>();
    public static Map<String, Definition> getDefinitions(List<Class<?>> classes) {
        for (Class<?> clazz : classes) {
            if (!definitions.containsKey(clazz.getSimpleName()) && isOpencbBean(clazz)) {
                definitions.put(clazz.getSimpleName(), generateDefinition(clazz));
            }
        }
        return definitions;
    }

    private static Definition generateDefinition(Class<?> clazz) {
        processedFields.add(clazz.getName());
        Definition definition = new Definition();
        Map<String, FieldDefinition> properties = new LinkedHashMap<>();
        Field[] fields = clazz.getDeclaredFields();

        for (Field field : fields) {
            try {
                FieldDefinition fieldDefinition;
                if(isCollection(field.getType())){
                    fieldDefinition = manageCollectionType(field);
                }else{
                    Class<?> fieldType = field.getType();
                    fieldDefinition = manageField(fieldType);
                }
                properties.put(field.getName(), fieldDefinition);
            } catch (Exception e) {
                System.err.println("Error processing field: " + field.getType().getSimpleName() + " in class " + field.getType().getName());
                e.printStackTrace();
            }
        }

        definition.setType("object");
        definition.setProperties(properties);
        return definition;
    }

    private static FieldDefinition manageField(Class<?> fieldType) {
        try {
            if (!isPrimitive(fieldType)) {
                if (!definitions.containsKey(fieldType.getSimpleName()) && !processedFields.contains(fieldType.getName()) && isOpencbBean(fieldType)) {
                    definitions.put(fieldType.getSimpleName(), generateDefinition(fieldType));
                }
                return manageComplexField(fieldType);
            } else {
                return managePrimitiveField(fieldType);
            }
        } catch (Exception e) {
            // Manejar errores inesperados durante la reflexión
            System.err.println("Error processing field: " + fieldType.getSimpleName() + " in class " + fieldType.getName());
            e.printStackTrace();
        }
        return null;
    }

    public static boolean isOpencbBean(Class<?> fieldType) {
        return fieldType.getName().contains("org.opencb");
    }

    private static FieldDefinition managePrimitiveField(Class<?> fieldType) {
        FieldDefinition primitiveProperty = new FieldDefinition();
        if (fieldType.getName().startsWith("[L")) {
            return manageArrayField(fieldType);
        }
        primitiveProperty.setType(mapJavaTypeToSwaggerType(fieldType));
        return primitiveProperty;
    }

    private static FieldDefinition manageComplexField(Class<?> fieldType) {
        FieldDefinition complexProperty = new FieldDefinition();

        // Handle Java object arrays
        if (fieldType.getName().startsWith("[L")) {
            return manageArrayField(fieldType);
        }

        // Default behavior for non-array types
        complexProperty.setType("object");
        if(isOpencbBean(fieldType)){
            complexProperty.set$ref("#/definitions/" + fieldType.getSimpleName());
        }else{
            complexProperty.setRef(fieldType.getSimpleName());
        }
        return complexProperty;
    }

    private static FieldDefinition manageArrayField(Class<?> fieldType) {
        // Extract the class name without "[L" and ";"
        String className = fieldType.getName().substring(2, fieldType.getName().length() - 1);
        try {
            Class<?> arrayElementType = Class.forName(className);

            // Define it as an array in OpenAPI
            ListDefinition arrayProperty = new ListDefinition();
            arrayProperty.setType("array");
            arrayProperty.setItems(manageField(arrayElementType));

            return arrayProperty;
        } catch (ClassNotFoundException e) {
            System.err.println("Error resolving array type: " + className);
            e.printStackTrace();
        }
        return null;
    }


    private static boolean isCollection(Class<?> fieldType) {
        return List.class.isAssignableFrom(fieldType) || Map.class.isAssignableFrom(fieldType);
    }

    private static FieldDefinition manageCollectionType(Field field) {
        Type genericType = field.getGenericType();
        if (genericType instanceof ParameterizedType) {
            ParameterizedType paramType = (ParameterizedType) genericType;
            Type[] typeArguments = paramType.getActualTypeArguments();

            if (List.class.isAssignableFrom(field.getType())) {
                FieldDefinition listDefinition = new ListDefinition();
                listDefinition.setType("array");
                // Get the item type of the list
                Type listItemType = typeArguments[0];
                ((ListDefinition) listDefinition).setItems(manageGenericType(listItemType));
                return listDefinition;

            } else if (Map.class.isAssignableFrom(field.getType())) {
                FieldDefinition mapDefinition = new MapDefinition();
                mapDefinition.setType("array");
                Type keyType = typeArguments[0];
                Type valueType = typeArguments[1];
                ((MapDefinition) mapDefinition).setKey(manageGenericType(keyType));
                ((MapDefinition) mapDefinition).setValue(manageGenericType(valueType));
                return mapDefinition;
            }
        }
        return null;
    }

    /**
     * Handles different types, including raw classes, generic types, and parameterized types.
     * Prevents errors caused by trying to resolve generic placeholders like T, K, or V.
     */
    private static FieldDefinition manageGenericType(Type type) {
        if (type instanceof Class<?>) {
            // If it's a concrete class, process it normally
            return manageField((Class<?>) type);
        } else if (type instanceof ParameterizedType) {
            // If it's another Map/List with generics, handle it recursively
            return manageCollectionTypeForType(type);
        } else if (type instanceof java.lang.reflect.TypeVariable) {
            // If it's a generic placeholder like T, K, or V, assign a default type
            FieldDefinition genericField = new FieldDefinition();
            genericField.setType("UnknownType"); // Generic placeholder in OpenAPI
            return genericField;
        } else if (type instanceof java.lang.reflect.WildcardType) {
            // Handle wildcard types like ? extends Number
            FieldDefinition wildcardField = new FieldDefinition();
            wildcardField.setType("WildcardType");
            return wildcardField;
        } else if (type instanceof java.lang.reflect.GenericArrayType) {
            // Handle generic arrays like T[]
            FieldDefinition arrayField = new ListDefinition();
            arrayField.setType("array");
            Type componentType = ((java.lang.reflect.GenericArrayType) type).getGenericComponentType();
            ((ListDefinition) arrayField).setItems(manageGenericType(componentType));
            return arrayField;
        }
        return null;
    }

    /**
     * Handles parameterized types within collections (List<T>, Map<K, V>, etc.).
     */
    private static FieldDefinition manageCollectionTypeForType(Type type) {
        if (type instanceof ParameterizedType) {
            ParameterizedType paramType = (ParameterizedType) type;
            Type rawType = paramType.getRawType();

            if (rawType instanceof Class<?> && Map.class.isAssignableFrom((Class<?>) rawType)) {
                // It's a Map inside another collection
                Type keyType = paramType.getActualTypeArguments()[0];
                Type valueType = paramType.getActualTypeArguments()[1];

                FieldDefinition mapDefinition = new MapDefinition();
                mapDefinition.setType("array");

                ((MapDefinition) mapDefinition).setKey(manageGenericType(keyType));
                ((MapDefinition) mapDefinition).setValue(manageGenericType(valueType));
                return mapDefinition;

            } else if (rawType instanceof Class<?> && List.class.isAssignableFrom((Class<?>) rawType)) {
                // It's a List inside another List or Map
                Type listItemType = paramType.getActualTypeArguments()[0];

                FieldDefinition listDefinition = new ListDefinition();
                listDefinition.setType("array");
                ((ListDefinition) listDefinition).setItems(manageGenericType(listItemType));
                return listDefinition;
            }
        }
        return null;
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

