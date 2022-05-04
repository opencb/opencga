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

package org.opencb.opencga.server.generator.writers;

import org.apache.commons.lang3.StringUtils;
import org.opencb.opencga.server.generator.config.CategoryConfig;
import org.opencb.opencga.server.generator.config.CommandLineConfiguration;
import org.opencb.opencga.server.generator.models.RestApi;
import org.opencb.opencga.server.generator.models.RestCategory;
import org.opencb.opencga.server.generator.models.RestEndpoint;
import org.opencb.opencga.server.generator.utils.CommandLineUtils;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

public abstract class ParentClientRestApiWriter {

    protected Map<String, String> validTypes;
    protected RestApi restApi;
    protected CommandLineConfiguration config;
    protected Map<String, RestCategory> availableCategories = new HashMap<>();
    protected Map<String, CategoryConfig> availableCategoryConfigs = new HashMap<>();
    protected Map<String, String> invalidNames = new HashMap<>();

    public ParentClientRestApiWriter(RestApi restApi, CommandLineConfiguration config) {
        this.restApi = restApi;
        this.config = config;
        init();
    }

    protected abstract String getClassImports(String key);

    protected abstract String getClassHeader(String key);

    protected abstract String getClassMethods(String key);

    protected abstract String getClassFileName(String key);

    public void write() {
        for (String key : availableCategories.keySet()) {
            StringBuffer sb = new StringBuffer();
            sb.append(getClassImports(key));
            sb.append(getClassHeader(key));
            sb.append(getClassMethods(key));
            sb.append("}");

            File file = new File(getClassFileName(key));
            try {
                writeToFile(file, sb);
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }

    protected static String reverseCommandName(String commandName) {
        if (commandName.contains("-")) {
            String[] phrase = commandName.split("-");
            if (phrase.length == 2) {
                commandName = getKebabCase(phrase[1]) + "-" + getKebabCase(phrase[0]);
            } else if (phrase.length == 3) {
                commandName = getKebabCase(phrase[1]) + "-" + getKebabCase(phrase[2]) + "-" + getKebabCase(phrase[0]);
            }
        }
        return commandName.toLowerCase();
    }

    public String getCategoryCommandName(RestCategory restCategory, CategoryConfig categoryConfig) {
        if (!StringUtils.isEmpty(categoryConfig.getCommandName())) {
            return categoryConfig.getCommandName();
        }
        return restCategory.getPath().substring(restCategory.getPath().lastIndexOf("/") + 1);
    }

    protected void writeToFile(File file, StringBuffer sb) throws IOException {
        String str = sb.toString();
        FileOutputStream outputStream = new FileOutputStream(file.getAbsolutePath());
        byte[] strToBytes = str.getBytes();
        outputStream.write(strToBytes);
        outputStream.close();
    }

    private void init() {
        for (CategoryConfig categoryConfig : config.getApiConfig().getCategoryConfigList()) {
            for (RestCategory restCategory : restApi.getCategories()) {
                if (!categoryConfig.isIgnore() && categoryConfig.getName().equals(getIdCategory(restCategory))) {
                    availableCategories.put(getIdCategory(restCategory), restCategory);
                    availableCategoryConfigs.put(getIdCategory(restCategory), categoryConfig);
                }
            }
        }

        invalidNames = new HashMap<>();
        invalidNames.put("default", "default_values");

        validTypes = new HashMap<>();
        validTypes.put("String", "String");
        validTypes.put("Map", "Map");
        validTypes.put("string", "String");
        validTypes.put("object", "Object");
        validTypes.put("Object", "Object");
        validTypes.put("integer", "Integer");
        validTypes.put("int", "Integer");
        validTypes.put("map", "ObjectMap");
        validTypes.put("boolean", "Boolean");
        validTypes.put("enum", "String");
        validTypes.put("long", "Long");
        validTypes.put("Long", "Long");
        validTypes.put("ObjectMap", "ObjectMap");
        validTypes.put("java.lang.String", "String");
        validTypes.put("java.lang.Boolean", "Boolean");
        validTypes.put("java.lang.Integer", "Integer");
        validTypes.put("java.lang.Long", "Integer");
        validTypes.put("java.lang.Short", "Integer");
        validTypes.put("java.lang.Double", "Integer");
        validTypes.put("java.lang.Float", "Integer");
        validTypes.put("List", "String");
        validTypes.put("java.util.List", "String");
    }

    public String getValidValue(String key) {
        String res = key;

        if (validTypes.containsKey(key)) {
            res = validTypes.get(key);
        }
        return res;
    }

    public String getIdCategory(RestCategory cat) {
        String res = getCleanPath(cat.getPath());
        return getAsCamelCase(res, "_");
    }

    public String getAsVariableName(String path) {
        return CommandLineUtils.getAsVariableName(path);
    }

    public String getAsClassName(String name) {
        return (Character.toUpperCase(name.charAt(0)) + name.substring(1)).replace(" ", "").replace("-", "");
    }

    public String getPathAsClassName(String path) {

        String res = getCleanPath(path);
        res = getAsCamelCase(res, "_");
        return (Character.toUpperCase(res.charAt(0)) + res.substring(1));
    }

    private String getCleanPath(String path) {
        return path.replace("/{apiVersion}/", "").replace("/", "_");
    }

    protected static String getCommandName(RestCategory restCategory, RestEndpoint restEndpoint) {
        return getMethodName(restCategory, restEndpoint).replaceAll("_", "-");
    }

    protected static String getMethodName(RestCategory restCategory, RestEndpoint restEndpoint) {
        String methodName = "";
        String subpath = restEndpoint.getPath().replace(restCategory.getPath() + "/", "");
        String[] items = subpath.split("/");
        if (items.length == 1) {
            methodName = items[0];
        } else if (items.length == 2) {
            if (notAny(items)) {
                methodName = items[1] + "_" + items[0];
            } else if (items[0].contains("}") && (!items[1].contains("}"))) {
                methodName = items[1];
            }
        } else if (items.length == 3) {
            if (notAny(items)) {
                methodName = items[2] + "_" + items[0] + "_" + items[1];
            } else if (items[0].contains("}") && (!items[1].contains("}")) && (!items[2].contains("}"))) {
                methodName = items[2] + "_" + items[1];
            } else if (items[1].contains("}") && (!items[0].contains("}")) && (!items[2].contains("}"))) {
                methodName = items[2] + "_" + items[0];
            }
        } else if (items.length == 4) {
            if (notAny(items)) {
                methodName = items[0] + "_" + items[1] + "_" + items[2] + "_" + items[3];
//                 methodName = items[3] + "_" + items[1] + "_" + items[2];
            } else if (items[0].contains("}") && items[2].contains("}") && (!items[1].contains("}")) && (!items[3].contains("}"))) {
                methodName = items[3] + "_" + items[1];
            }
        } else if (items.length == 5) {
            if (items[0].contains("}") && items[2].contains("}") && (!items[1].contains("}")) && (!items[3].contains("}"))
                    && (!items[4].contains("}"))) {
                //methodName = items[4] + "_" + items[3];
                methodName = items[4] + "_" + items[1] + "_" + items[3];
            }
        }

        return methodName;
    }

    private static boolean notAny(String[] items) {
        for (String item : items) {
            if (item.contains("{")) {
                return false;
            }
        }
        return true;
    }

    protected static String getAsCamelCase(String s) {
        if (s.contains("-")) {
            s = s.replaceAll("-", "_");
        }
        if (s.contains(" ")) {
            s = s.replaceAll(" ", "_");
        }
        return getAsCamelCase(s, "_");
    }

    protected static String getKebabCase(String camelStr) {
        String ret = camelStr.replaceAll("([A-Z]+)([A-Z][a-z])", "$1-$2").replaceAll("([a-z])([A-Z])", "$1-$2");
        return ret.toLowerCase();
    }

    protected static String getAsCamelCase(String s, String separator) {
        String[] words = s.split(separator);
        StringBuilder builder = new StringBuilder();
        for (int i = 0; i < words.length; i++) {
            String word = words[i].trim();
            if (i == 0) {
                word = word.isEmpty() ? word : Character.toLowerCase(word.charAt(0)) + word.substring(1);
            } else {
                word = word.isEmpty() ? word : Character.toUpperCase(word.charAt(0)) + word.substring(1);
            }
            builder.append(word);
        }
        return builder.toString();
    }

    public Map<String, RestCategory> getAvailableCategories() {
        return availableCategories;
    }

    public ParentClientRestApiWriter setAvailableCategories(Map<String, RestCategory> availableCategories) {
        this.availableCategories = availableCategories;
        return this;
    }

    public Map<String, CategoryConfig> getAvailableCategoryConfigs() {
        return availableCategoryConfigs;
    }

    public ParentClientRestApiWriter setAvailableCategoryConfigs(Map<String, CategoryConfig> availableCategoryConfigs) {
        this.availableCategoryConfigs = availableCategoryConfigs;
        return this;
    }
}
