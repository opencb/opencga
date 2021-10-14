package org.opencb.opencga.server.json.utils;

import java.util.HashSet;
import java.util.Set;

public class CommandLineUtils {

    private static final Set<String> primitiveTypes;

    static {
        primitiveTypes = new HashSet<>();

        primitiveTypes.add("String");
        primitiveTypes.add("string");
        primitiveTypes.add("object");
        primitiveTypes.add("Object");
        primitiveTypes.add("integer");
        primitiveTypes.add("int");
        primitiveTypes.add("boolean");
        primitiveTypes.add("long");
        primitiveTypes.add("enum");
        primitiveTypes.add("Long");
        primitiveTypes.add("java.lang.String");
        primitiveTypes.add("java.lang.Boolean");
        primitiveTypes.add("java.lang.Integer");
        primitiveTypes.add("java.lang.Long");
        primitiveTypes.add("java.lang.Short");
        primitiveTypes.add("java.lang.Double");
        primitiveTypes.add("java.lang.Float");
    }

    public static boolean isPrimitiveType(String type) {
        return primitiveTypes.contains(type);
    }

    public static String getAsVariableName(String path) {
        return (Character.toLowerCase(path.charAt(0)) + path.substring(1)).replace(" ", "").replace("-", "");
    }
}
