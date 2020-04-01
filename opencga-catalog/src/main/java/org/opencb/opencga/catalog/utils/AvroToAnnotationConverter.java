/*
 * Copyright 2015-2020 OpenCB
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.opencb.opencga.catalog.utils;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.opencb.opencga.core.common.JacksonUtils;
import org.opencb.opencga.core.models.common.AnnotationSet;
import org.opencb.opencga.core.models.study.Variable;

import java.io.IOException;
import java.util.*;
import java.util.stream.Collectors;

public class AvroToAnnotationConverter {


    public static List<Variable> convertToVariableSet(Schema schema) {
        List<Variable> variables = new ArrayList<>(schema.getFields().size());

        for (Schema.Field field : schema.getFields()) {
            variables.add(getVariable(field, field.schema()));
        }

        return variables;
    }

    public static <T extends GenericRecord> AnnotationSet convertToAnnotationSet(T record, String id) {
        Map<String, Object> annotations = convert(record);
        return new AnnotationSet(id, id, annotations);
    }

    public static <T extends GenericRecord> T convertAnnotationToAvro(AnnotationSet annotationSet, Class<T> clazz) throws IOException {
        String s = JacksonUtils.getDefaultObjectMapper().writer().writeValueAsString(annotationSet.getAnnotations());
        return JacksonUtils.getDefaultObjectMapper().readerFor(clazz).readValue(s);
    }

//    public static GenericRecord convertAnnotationToAvro(AnnotationSet annotationSet, Schema schema) throws IOException {
//        GenericRecordBuilder builder = new GenericRecordBuilder(schema);
//        for (Map.Entry<String, Object> entry : annotationSet.getAnnotations().entrySet()) {
//            Schema.Field field = schema.getField(entry.getKey());
//            if (field == null) {
//                continue;
//            } else {
//                switch (field.schema().getType()) {
//                    case STRING:
//                    case BYTES:
//                    case FIXED:
//                    case INT:
//                    case LONG:
//                    case FLOAT:
//                    case DOUBLE:
//                    case BOOLEAN:
//                        builder.set(field, entry.getValue());
//                        break;
//                    case ENUM:
//                        builder.set(field, entry.getValue());
//                        break;
//                    case ARRAY:
//                    case UNION:
//                    case MAP:
//                    case RECORD:
//                    case NULL:
//                    default:
//                }
//            }
//        }
//        return builder.build();
//    }

    private static Variable getVariable(Schema.Field field, Schema schema) {

        boolean defaultRequired = false;

        switch (schema.getType()) {
            case STRING:
            case BYTES:
            case FIXED:
                return new Variable(field.name(), field.name(), "", Variable.VariableType.STRING,
                        field.defaultValue() == null ? null : field.defaultValue().getTextValue(),
                        defaultRequired, false, null, null, field.pos(), null, field.doc(), null, null);
            case INT:
            case LONG:
                return new Variable(field.name(), field.name(), "", Variable.VariableType.INTEGER,
                        field.defaultValue() == null ? null : field.defaultValue().getLongValue(),
                        defaultRequired, false, null, null, field.pos(), null, field.doc(), null, null);
            case FLOAT:
            case DOUBLE:
                return new Variable(field.name(), field.name(), "", Variable.VariableType.DOUBLE,
                        field.defaultValue() == null ? null : field.defaultValue().getDoubleValue(),
                        defaultRequired, false, null, null, field.pos(), null, field.doc(), null, null);
            case BOOLEAN:
                return new Variable(field.name(), field.name(), "", Variable.VariableType.BOOLEAN,
                        field.defaultValue() == null ? null : field.defaultValue().getBooleanValue(),
                        defaultRequired, false, null, null, field.pos(), null, field.doc(), null, null);
            case ENUM:
                return new Variable(field.name(), field.name(), "", Variable.VariableType.CATEGORICAL,
                        field.defaultValue() == null ? null : field.defaultValue().getTextValue(),
                        defaultRequired, false, schema.getEnumSymbols(), null, field.pos(), null, field.doc(), null, null);
            case ARRAY:
                return getVariable(field, schema.getElementType()).setMultiValue(true);
            case UNION: {
                List<Schema> types = schema.getTypes();
                Map<Schema.Type, Schema> map = types.stream().collect(Collectors.toMap(Schema::getType, s -> s));
                if (map.size() > 2 || !map.containsKey(Schema.Type.NULL)) {
                    throw new IllegalArgumentException("Unable to store unions of " + map.keySet());
                }
                map.remove(Schema.Type.NULL);
                Schema subSchema = map.values().iterator().next();
                return getVariable(field, subSchema).setRequired(false);
            }
            case MAP: {
//                Set<Variable> variableSet = new HashSet<>();
//                variableSet.add(new Variable("key", "key", "", Variable.VariableType.TEXT, null,
//                defaultRequired, false, null, 0, null, "Key", null, null));
//                Variable valueVariable = getVariable(field, schema.getValueType());
//                valueVariable.setId("value")
//                        .setName("value");
//                variableSet.add(valueVariable);
//                return new Variable(field.name(), field.name(), "", Variable.VariableType.OBJECT,
//                        field.defaultValue(), defaultRequired, true, null, field.pos(), null, field.doc(), variableSet, null);
                Variable.VariableType mapType;
                switch (field.schema().getValueType().getType()) {
                    case STRING:
                    case BYTES:
                    case FIXED:
                        mapType = Variable.VariableType.MAP_STRING;
                        break;
                    case INT:
                    case LONG:
                        mapType = Variable.VariableType.MAP_INTEGER;
                        break;
                    case FLOAT:
                    case DOUBLE:
                        mapType = Variable.VariableType.MAP_DOUBLE;
                        break;
                    case BOOLEAN:
                        mapType = Variable.VariableType.MAP_BOOLEAN;
                        break;
                    default:
                        mapType = Variable.VariableType.OBJECT;
                }
                return new Variable(field.name(), field.name(), "", mapType,
                        null, defaultRequired, false, null, null, field.pos(), null, field.doc(), null, null);
            }
            case RECORD: {
                Set<Variable> variableSet = new HashSet<>(convertToVariableSet(schema));
                return new Variable(field.name(), field.name(), "", Variable.VariableType.OBJECT,
                        null, defaultRequired, false, null, null, field.pos(), null, field.doc(), variableSet, null);
            }
            default:
            case NULL:
                throw new IllegalStateException("Unexpected type " + schema.getType());
        }
    }

    @SuppressWarnings("unchecked")
    private static <T extends GenericRecord> Map<String, Object> convert(T record) {
        return (Map<String, Object>) convert(record, record.getSchema());
    }

    private static Object convert(Object object, Schema schema) {
        if (object == null) {
            return null;
        }
        switch (schema.getType()) {
            case ARRAY: {
                Schema elementType = schema.getElementType();
                List<Object> list = new ArrayList<>(((Collection) object).size());
                for (Object o : ((Collection<?>) object)) {
                    list.add(convert(o, elementType));
                }
                return list;
            }
            case MAP: {
//                Schema valueType = schema.getValueType();
//                List<Map<String, Object>> list = new ArrayList<>(((Map) object).size());
//                for (Map.Entry<String, ?> entry : ((Map<String, ?>) object).entrySet()) {
//                    HashMap<String, Object> map = new HashMap<>(2);
//                    map.put("key", entry.getKey());
//                    map.put("value", convert(entry.getValue(), valueType));
//                    list.add(map);
//                }
//                return list;
                return object;
            }
            case RECORD:
                GenericRecord genericRecord = (GenericRecord) object;
                Map<String, Object> annotations = new HashMap<>();
                for (Schema.Field field : genericRecord.getSchema().getFields()) {
                    Object o = genericRecord.get(field.pos());
                    annotations.put(field.name(), convert(o, field.schema()));
                }
                return annotations;
            case UNION:
                for (Schema type : schema.getTypes()) {
                    if (!type.getType().equals(Schema.Type.NULL)) {
                        return convert(object, type);
                    }
                }
                // Is this possible?
                return object;
            case ENUM:
            case FIXED:
            case STRING:
            case BYTES:
            case INT:
            case LONG:
            case FLOAT:
            case DOUBLE:
            case BOOLEAN:
            case NULL:
            default:
                return object;
        }
    }

}
