package org.opencb.opencga.server.generator.openapi.common;

import com.fasterxml.jackson.databind.BeanDescription;
import com.fasterxml.jackson.databind.JavaType;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.introspect.BeanPropertyDefinition;
import com.fasterxml.jackson.databind.type.TypeFactory;
import org.apache.commons.lang3.StringUtils;
import org.opencb.commons.annotations.DataClass;
import org.opencb.commons.annotations.DataField;
import org.opencb.opencga.core.common.JacksonUtils;
import org.opencb.opencga.server.generator.openapi.models.Definition;
import org.opencb.opencga.server.generator.openapi.models.FieldDefinition;
import org.opencb.opencga.server.generator.openapi.models.ListDefinition;
import org.opencb.opencga.server.generator.openapi.models.MapDefinition;

import java.net.URI;
import java.net.URL;
import java.util.*;
import java.util.stream.Collectors;

public class SwaggerDefinitionGenerator {

    private final static Map<Class<?>, Definition> definitions = new LinkedHashMap<>();
    private final static Set<String> processedFields = new HashSet<>();
    private final static ObjectMapper objectMapper;

    static {
        objectMapper = JacksonUtils.getExternalOpencgaObjectMapper();
    }

    public static Map<String, Definition> getDefinitions(Collection<Class<?>> classes) {
        for (Class<?> clazz : classes) {
            generateDefinition(clazz);
        }
        Map<String, Definition> simpleDefinitions = new TreeMap<>();
        List<Class<?>> duplicatedClasses = new ArrayList<>();
        for (Map.Entry<Class<?>, Definition> entry : definitions.entrySet()) {
            String ref = buildRefName(entry.getKey());
            if (simpleDefinitions.put(ref, entry.getValue()) != null) {
                for (Class<?> aClass : definitions.keySet()) {
                    if (buildRefName(aClass).equals(ref)) {
                        duplicatedClasses.add(aClass);
                    }
                }
            }
        }
        if (simpleDefinitions.size() != definitions.size()) {
            throw new IllegalStateException("Duplicate definitions found. Please check the classes. Duplicated classes: " + duplicatedClasses);
        }
        return simpleDefinitions;

    }


    private static void generateDefinition(Class<?> clazz) {
        // Check if the class is already processed
        if (definitions.containsKey(clazz)) {
            return;
        }
        if (!isOpencbBean(clazz)) {
            // Only known OpenCB beans are supported.
            throw new IllegalArgumentException("Class " + clazz.getName() + " is not an OpenCB bean");
        }
        // Avoid infinite recursion in self-referencing classes
        if (!processedFields.add(clazz.getName())) {
            return;
        }
        Map<String, FieldDefinition> fieldDefinitions = new LinkedHashMap<>();
        BeanDescription beanDescription = objectMapper.getSerializationConfig().introspect(objectMapper.constructType(clazz));
        List<BeanPropertyDefinition> properties = beanDescription.findProperties();

        // Jackson introspector will include all the fields, including the ones that are not serializable
        // Remove the ones that are not serializable or deserializable
        properties.removeIf(property -> {
            if (!property.couldSerialize() || !property.couldDeserialize()) {
//                if (!property.getName().equals("schema")) {
//                    System.out.println("Ignored field " + clazz.getSimpleName() + "." + property.getName());
//                }
                return true;
            } else {
                return false;
            }
        });

        for (BeanPropertyDefinition property : properties) {
            if(property.getName().equals("serialVersionUID") || property.getName().contains("$")){
                throw new IllegalStateException("Field " + property.getName() + " in class " + property.getRawPrimaryType().getName() + " is not supported");
            }
            try {
                FieldDefinition fieldDefinition = manageField(property.getPrimaryType());

                // Enrich field definition with additional information
                if (property.getField() != null) {
                    if (property.getField().hasAnnotation(DataField.class)) {
                        DataField annotation = property.getField().getAnnotation(DataField.class);
                        if (annotation.description() != null && !annotation.description().isEmpty()) {
                            fieldDefinition.setDescription(annotation.description());
                        }
                    }
                }

                fieldDefinitions.put(property.getName(), fieldDefinition);

            } catch (Exception e) {
                throw new IllegalStateException(
                        "Error processing field: " + property.getRawPrimaryType().getSimpleName() + " in class " + property.getRawPrimaryType().getName(), e);
            }
        }

        Definition definition = new Definition();
        definition.setType("object");
        definition.setProperties(fieldDefinitions);
        if (clazz.isAnnotationPresent(DataClass.class)) {
            String description = clazz.getAnnotation(DataClass.class).description();
            if (StringUtils.isNoneEmpty(description)) {
                definition.setDescription(description);
            }
        }
        definitions.put(clazz, definition);
    }


    private static FieldDefinition manageEnumField(Class<?> enumClass) {
        if (!enumClass.isEnum()) {
            throw new IllegalArgumentException("Provided class is not an enum: " + enumClass.getName());
        }

        List<String> values = Arrays.stream(enumClass.getEnumConstants())
                .map(Object::toString)
                .collect(Collectors.toList());
        // Always accept null as a valid value
        values.add(null);

        return new FieldDefinition()
                .setType("string")  // OpenAPI treats enums as strings
                .setEnum(values);
    }

    private static FieldDefinition manageField(JavaType type) {
        Class<?> clazz = type.getRawClass();
        try {
            if (type.isCollectionLikeType() || type.isArrayType() || type.isMapLikeType()) {
                return manageCollectionType(type);
            } else if (isPrimitive(clazz)) {
                return managePrimitiveField(type);
            } else if(type.isEnumImplType()) {
                return manageEnumField(clazz);
            } else {
                // Other complex fields. Returns a $ref to the definition
                return manageComplexField(clazz);
            }
        } catch (Exception e) {
            throw new IllegalStateException("Error processing field: " + type.getTypeName() + " in class " + type.getRawClass().getName(), e);
        }
    }

    public static boolean isOpencbBean(Class<?> fieldType) {
        return fieldType.getName().contains("org.opencb")
                || fieldType.getName().contains("com.zettagenomics")
                || fieldType.getName().contains("org.ga4gh");
    }

    private static FieldDefinition managePrimitiveField(JavaType fieldType) {
        FieldDefinition primitiveProperty = new FieldDefinition();
        primitiveProperty.setType(mapJavaTypeToSwaggerType(fieldType.getRawClass()));
        return primitiveProperty;
    }

    private static FieldDefinition manageComplexField(Class<?> fieldType) {
        FieldDefinition complexProperty = new FieldDefinition();

        if (isOpencbBean(fieldType)) {
            generateDefinition(fieldType);
            complexProperty.setType("object");
            complexProperty.set$ref(build$ref(fieldType));
        } else {
            // Not an OpenCB bean. Check if it is a known non-primitive java type
            if (fieldType == URI.class) {
                complexProperty.setType("string");
                complexProperty.setFormat("uri");
            } else if (fieldType == URL.class) {
                complexProperty.setType("string");
                complexProperty.setFormat("url");
            } else if (fieldType == Class.class) {
                complexProperty.setType("string");
                complexProperty.setFormat("java-class");
            } else if (fieldType == Date.class) {
                complexProperty.setType("string");
                complexProperty.setFormat("full-date");
            } else if (fieldType == Enum.class) {
                // Unknown enum type
                complexProperty.setType("string");
            } else if (fieldType == Object.class) {
                complexProperty.setType("object");
                complexProperty.setRef(fieldType.getSimpleName());
            } else {
                throw new IllegalArgumentException("Class " + fieldType.getName() + " is not a supported type");
            }
        }
        return complexProperty;
    }

    public static String build$ref(Class<?> fieldType) {
        return "#/definitions/" + buildRefName(fieldType);
    }

    public static String buildRefName(Class<?> fieldType) {
        if (isOpencbBean(fieldType)) {
            String prefix = "";
            String suffix = "";
            if (fieldType.getPackage().equals(org.opencb.biodata.models.variant.avro.VariantAvro.class.getPackage())) {
                if (fieldType != org.opencb.biodata.models.variant.avro.VariantAnnotation.class) {
                    suffix = "-avro";
                }
            } else if (fieldType == org.opencb.biodata.models.common.Status.class) {
                suffix = "-biodata";
            } else if (fieldType == org.opencb.biodata.models.metadata.Sample.class) {
                suffix = "-biodata";
            } else if (fieldType == org.opencb.biodata.models.metadata.Individual.class) {
                suffix = "-biodata";
            } else if (fieldType == org.opencb.biodata.models.metadata.Cohort.class) {
                suffix = "-biodata";
            } else if (fieldType == org.opencb.biodata.models.clinical.interpretation.stats.InterpretationStats.class) {
                suffix = "-stats-biodata";
            } else if (fieldType == org.opencb.opencga.core.models.family.IndividualCreateParams.class) {
                suffix = "-family";
            } else if (fieldType.isMemberClass()) {
                prefix = fieldType.getEnclosingClass().getSimpleName() + "-";
            }
            return prefix + fieldType.getSimpleName() + suffix;
        } else {
            throw new IllegalArgumentException("Class " + fieldType.getName() + " is not an OpenCB bean");
        }
    }

    public static boolean isCollection(Class<?> fieldType) {
        return Collection.class.isAssignableFrom(fieldType) || Map.class.isAssignableFrom(fieldType);
    }

    public static FieldDefinition manageCollectionType(JavaType type) {
        Class<?> clazz = type.getRawClass();
        if (type.isArrayType() || type.isCollectionLikeType()) {
            // Get the item type of the list
            FieldDefinition content = manageField(type.getContentType());
            return new ListDefinition(content);
        } else if (type.isMapLikeType()) {
            // Model with Map/Dictionary Properties
            // https://github.com/OAI/OpenAPI-Specification/blob/main/versions/2.0.md#model-with-mapdictionary-properties
            JavaType keyType = type.getKeyType();
            // Ensure the key type is a supported string-like type (string or Enum)
            if (!String.class.isAssignableFrom(keyType.getRawClass()) && !keyType.isEnumType()) {
                throw new IllegalArgumentException("Map keys must be of type String or Enum, but found: " + keyType.getRawClass().getName());
            }
            JavaType valueType = type.getContentType();
            if (valueType.getRawClass() == Object.class) {
                // OpenAPI does not support generic types for Map values. Use string instead.
                valueType = TypeFactory.defaultInstance().constructSimpleType(String.class, null);
            }
            FieldDefinition content = manageField(valueType);
            return new MapDefinition(content);
        } else {
            throw new IllegalArgumentException("Unsupported collection type: " + clazz.getName());
        }
    }

    public static boolean isPrimitive(Class<?> clazz) {
        return clazz.isPrimitive()
                || clazz == String.class
                || Number.class.isAssignableFrom(clazz)
                || clazz == Boolean.class
                || clazz == Byte.class
                || clazz == Character.class
                || clazz == Void.class;
    }

    public static String mapJavaTypeToSwaggerType(Class<?> clazz) {
        if (clazz == String.class) return "string";
        if (clazz == Integer.class || clazz == int.class) return "integer";
        if (clazz == Long.class || clazz == long.class) return "integer";
        if (clazz == Boolean.class || clazz == boolean.class) return "boolean";
        if (clazz == Number.class) return "number";
        if (clazz == Double.class || clazz == double.class) return "number";
        if (clazz == Float.class || clazz == float.class) return "number";
        if (clazz == Byte.class || clazz == byte.class) return "string";
        if (clazz == Short.class || clazz == short.class) return "integer";
        if (clazz == Character.class || clazz == char.class) return "string";
        return "object"; // Default
    }
}

