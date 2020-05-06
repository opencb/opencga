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

package org.opencb.opencga.analysis.tools;

import org.apache.commons.lang3.StringUtils;
import org.opencb.opencga.core.tools.annotations.Tool;
import org.opencb.opencga.core.exceptions.ToolException;
import org.reflections.Reflections;
import org.reflections.scanners.SubTypesScanner;
import org.reflections.scanners.TypeAnnotationsScanner;
import org.reflections.util.ClasspathHelper;
import org.reflections.util.ConfigurationBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.Modifier;
import java.net.URL;
import java.util.*;

public class ToolFactory {
    private static final Logger logger = LoggerFactory.getLogger(ToolFactory.class);
    private static Map<String, Class<? extends OpenCgaTool>> toolsCache;
    private static Map<String, Set<Class<? extends OpenCgaTool>>> duplicatedTools;
    private static List<Class<? extends OpenCgaTool>> toolsList;

    private static synchronized Map<String, Class<? extends OpenCgaTool>> loadTools() {
        if (toolsCache == null) {
            Reflections reflections = new Reflections(new ConfigurationBuilder()
                    .setScanners(
                            new SubTypesScanner(),
                            new TypeAnnotationsScanner().filterResultsBy(s -> StringUtils.equals(s, Tool.class.getName()))
                    )
                    .addUrls(getUrls())
                    .filterInputsBy(input -> input != null && input.endsWith(".class"))
            );

            Map<String, Set<Class<? extends OpenCgaTool>>> duplicatedTools = new HashMap<>();
            Set<Class<? extends OpenCgaTool>> subTypes = reflections.getSubTypesOf(OpenCgaTool.class);
            Map<String, Class<? extends OpenCgaTool>> cache = new HashMap<>();
            for (Class<? extends OpenCgaTool> subType : subTypes) {
                Tool annotation = subType.getAnnotation(Tool.class);
                if (annotation != null) {
                    Class<? extends OpenCgaTool> old = cache.put(annotation.id(), subType);
                    if (old != null) {
                        Set<Class<? extends OpenCgaTool>> set = duplicatedTools.computeIfAbsent(annotation.id(), k -> new HashSet<>());
                        set.add(old);
                        set.add(subType);
                    }
                } else if (!Modifier.isAbstract(subType.getModifiers())) {
                    logger.warn("Found non-abstract class " + subType.getName() + " extending " + OpenCgaTool.class.getSimpleName()
                            + " without the java annotation @" + Tool.class.getSimpleName());
                }
            }
            if (!duplicatedTools.isEmpty()) {
                duplicatedTools.forEach((id, set) -> {
                    cache.remove(id);
                    logger.error("Found duplicated tool ID '{}' in classes {}", id, set);
                });
                duplicatedTools.replaceAll((key, set) -> Collections.unmodifiableSet(set));
            }

            List<Class<? extends OpenCgaTool>> toolsList = new ArrayList<>(cache.values());
            toolsList.sort(Comparator
                    .comparing((Class<? extends OpenCgaTool> c) -> c.getAnnotation(Tool.class).type().name())
                    .thenComparing((Class<? extends OpenCgaTool> c) -> c.getAnnotation(Tool.class).resource().name())
                    .thenComparing(c -> c.getAnnotation(Tool.class).id()));

            ToolFactory.toolsList = Collections.unmodifiableList(toolsList);
            ToolFactory.duplicatedTools = Collections.unmodifiableMap(duplicatedTools);
            ToolFactory.toolsCache = cache;
        }
        return toolsCache;
    }

    static Collection<URL> getUrls() {
        // TODO: What if there are third party libraries that implement Tools?
        //  Currently they must contain "opencga" in the jar name.
        //  e.g.  acme-rockets-opencga-5.4.0.jar
        Collection<URL> urls = new LinkedList<>();
        for (URL url : ClasspathHelper.forClassLoader()) {
            String name = url.getPath().substring(url.getPath().lastIndexOf('/') + 1);
            if (name.isEmpty() || (name.contains("opencga") && !name.contains("opencga-storage-hadoop-deps"))) {
                urls.add(url);
            }
        }
        return urls;
    }

    public final Class<? extends OpenCgaTool> getToolClass(String toolId) throws ToolException {
        Objects.requireNonNull(toolId);

        Class<? extends OpenCgaTool> aClass = null;
        try {
            Class<?> inputClass = Class.forName(toolId);
            if (OpenCgaTool.class.isAssignableFrom(inputClass)) {
                aClass = (Class<? extends OpenCgaTool>) inputClass;
            }
        } catch (ClassNotFoundException ignore) {
        }
        if (aClass == null) {
            aClass = loadTools().get(toolId);
        }
        if (aClass == null) {
            throw new ToolException("Tool '" + toolId + "' not found");
        }
        return aClass;
    }

    public Tool getTool(String toolId) throws ToolException {
        return getToolClass(toolId).getAnnotation(Tool.class);
    }

    public final OpenCgaTool createTool(String toolId) throws ToolException {
        return createTool(getToolClass(toolId));
    }

    public final OpenCgaTool createTool(Class<? extends OpenCgaTool> aClass) throws ToolException {
        Tool annotation = aClass.getAnnotation(Tool.class);
        if (annotation == null) {
            throw new ToolException("Class " + aClass + " does not have the required java annotation @" + Tool.class.getSimpleName());
        }
        try {
            return aClass.newInstance();
        } catch (InstantiationException | IllegalAccessException e) {
            throw new ToolException("Can't instantiate class " + aClass + " from tool '" + annotation.id() + "'", e);
        }
    }

    public Collection<Class<? extends OpenCgaTool>> getTools() {
        loadTools();
        return toolsList;
    }

    public Map<String, Set<Class<? extends OpenCgaTool>>> getDuplicatedTools() {
        loadTools();
        return duplicatedTools;
    }
}
