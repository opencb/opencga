package org.opencb.opencga.core.params;

import java.util.HashMap;
import java.util.Map;

public class ParamDescription {
    private static Map<String, Map<String, String>> constants = new HashMap<>();

    static {
        constants.put("creationDate", getMap("GENERIC_CREATION_DATE_DESCRIPTION", "The creation date of the item"));
        constants.put("modificationDate", getMap("GENERIC_MODIFICATION_DATE_DESCRIPTION", "The last modification date of the item"));
        constants.put("attributes", getMap("GENERIC_ATTRIBUTES_DESCRIPTION", "A map of customizable attributes"));
        constants.put("status", getMap("GENERIC_STATUS_DESCRIPTION", "A map of customizable attributes"));
        constants.put("description", getMap("GENERIC_DESCRIPTION_DESCRIPTION", "Field to store information of the item"));
    }

    private String name;
    private String value;
    private String fieldName;
    private String type;

    public ParamDescription(String name, String value, String fieldName, String type) {
        this.name = name;
        this.value = value;
        this.fieldName = fieldName;
        this.type = type;
    }

    private static Map<String, String> getMap(String k, String v) {
        Map<String, String> res = new HashMap<>();
        res.put(k, v);
        return res;
    }

    @Override
    public String toString() {
        return "ParamDescription{" +
                "name='" + name + '\'' +
                ", value='" + value + '\'' +
                ", fieldName='" + fieldName + '\'' +
                ", type='" + type + '\'' +
                '}';
    }

    public String getType() {
        return type;
    }

    public ParamDescription setType(String type) {
        this.type = type;
        return this;
    }

    public String getName() {
        return name;
    }

    public ParamDescription setName(String name) {
        this.name = name;
        return this;
    }

    public String getValue() {
        return value;
    }

    public ParamDescription setValue(String value) {
        this.value = value;
        return this;
    }

    public String getFieldName() {
        return fieldName;
    }

    public ParamDescription setFieldName(String fieldName) {
        this.fieldName = fieldName;
        return this;
    }

    public String getDataFieldTag() {
        return "    @DataField(description = ParamConstants." + getConstantName(name) + ")";
    }

    private String getConstantName(String name) {
        if (constants.containsKey(fieldName)) {
            return constants.get(fieldName).keySet().stream().findFirst().get();
        }
        return name;
    }

    public String getConstant() {
        String var = name;
        String val = value;
        if (constants.containsKey(fieldName)) {
            var = constants.get(fieldName).keySet().stream().findFirst().get();
            val = constants.get(fieldName).values().stream().findFirst().get();
        }
        return "    public static final String " + var + " = \"" + val + "\";";
    }

}