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

import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.opencb.opencga.core.tools.annotations.ToolExecutor;
import org.opencb.opencga.core.exceptions.ToolExecutorException;
import org.opencb.opencga.core.tools.OpenCgaToolExecutor;
import org.reflections.Reflections;
import org.reflections.scanners.SubTypesScanner;
import org.reflections.scanners.TypeAnnotationsScanner;
import org.reflections.util.ConfigurationBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.Modifier;
import java.util.*;

public class ToolExecutorFactory {
    private final Logger logger = LoggerFactory.getLogger(ToolExecutorFactory.class);

    private static Set<Class<? extends OpenCgaToolExecutor>> executorsCache;

    private static synchronized Set<Class<? extends OpenCgaToolExecutor>> loadExecutors() {
        if (executorsCache == null) {
            Reflections reflections = new Reflections(new ConfigurationBuilder()
                    .setScanners(
                            new SubTypesScanner()
                            ,
                            new TypeAnnotationsScanner().filterResultsBy(s -> StringUtils.equals(s, ToolExecutor.class.getName()))
                    )
                    .addUrls(ToolFactory.getUrls())
                    .filterInputsBy(input -> input != null && input.endsWith(".class"))
            );

            executorsCache = reflections.getSubTypesOf(OpenCgaToolExecutor.class);
        }
        return executorsCache;
    }

    public final Class<? extends OpenCgaToolExecutor> getToolExecutorClass(String toolId, String toolExecutorId) {
        return getToolExecutorClass(toolId, toolExecutorId, OpenCgaToolExecutor.class);
    }

    public final <T extends OpenCgaToolExecutor> Class<? extends T> getToolExecutorClass(
            String toolId, String toolExecutorId, Class<T> clazz) {
        return getToolExecutorClass(toolId, toolExecutorId, clazz, null, null);
    }

    public final <T extends OpenCgaToolExecutor> Class<? extends T> getToolExecutorClass(
            String toolId, String toolExecutorId, Class<T> clazz,
            List<ToolExecutor.Source> sourceTypes,
            List<ToolExecutor.Framework> availableFrameworks) {
        Objects.requireNonNull(clazz);

        List<Class<? extends T>> candidateClasses = new ArrayList<>();
        // If the given class is not abstract, check if matches the criteria.
        if (!Modifier.isAbstract(clazz.getModifiers())) {
            if (isValidClass(toolId, toolExecutorId, clazz, clazz, sourceTypes, availableFrameworks)) {
                if (StringUtils.isNotEmpty(toolExecutorId) || Modifier.isFinal(clazz.getModifiers())) {
                    // Shortcut to skip reflection
                    return clazz;
                }
                candidateClasses.add(clazz);
            }
        }

        Set<Class<? extends OpenCgaToolExecutor>> typesAnnotatedWith = loadExecutors();
        for (Class<? extends OpenCgaToolExecutor> aClass : typesAnnotatedWith) {
            if (isValidClass(toolId, toolExecutorId, clazz, aClass, sourceTypes, availableFrameworks)) {
                candidateClasses.add((Class<? extends T>) aClass);
            }
        }
        if (candidateClasses.isEmpty()) {
            return null;
        } else if (candidateClasses.size() == 1) {
            return candidateClasses.get(0);
        } else {
            logger.info("Found multiple " + OpenCgaToolExecutor.class.getName() + " candidates.");
            for (Class<? extends T> matchedClass : candidateClasses) {
                logger.info(" - " + matchedClass);
            }
            logger.info("Sort by framework and source preference.");

            // Prefer the executor that matches better with the source
            // Prefer the executor that matches better with the framework
            List<ToolExecutor.Framework> finalAvailableFrameworks =
                    availableFrameworks == null ? Collections.emptyList() : availableFrameworks;
            List<ToolExecutor.Source> finalSourceTypes =
                    sourceTypes == null ? Collections.emptyList() : sourceTypes;

            Comparator<Class<? extends T>> comparator = Comparator.<Class<? extends T>>comparingInt(c1 -> {
                ToolExecutor annot1 = c1.getAnnotation(ToolExecutor.class);
                return finalAvailableFrameworks.indexOf(annot1.framework());
            }).thenComparingInt(c -> {
                ToolExecutor annot = c.getAnnotation(ToolExecutor.class);
                return finalSourceTypes.indexOf(annot.source());
            }).thenComparing(Class::getName);

            candidateClasses.sort(comparator);

            return candidateClasses.get(0);
        }
    }

    private <T> boolean isValidClass(String toolId, String toolExecutorId, Class<?> parentClass, Class<?> aClass,
                                     List<ToolExecutor.Source> sourceTypes,
                                     List<ToolExecutor.Framework> availableFrameworks) {
        if (!parentClass.isAssignableFrom(aClass)) {
            return false;
        }
        ToolExecutor annotation = aClass.getAnnotation(ToolExecutor.class);
        if (annotation == null) {
            return false;
        }
        // JT commented it to allow tools to call any executor (even executors from other tools)
//        if (!annotation.tool().equals(toolId)) {
//            return false;
//        }
        if (StringUtils.isNotEmpty(toolExecutorId) && !toolExecutorId.equals(annotation.id())) {
            return false;
        }
        if (CollectionUtils.isNotEmpty(sourceTypes) && !sourceTypes.contains(annotation.source())) {
            return false;
        }
        if (CollectionUtils.isNotEmpty(availableFrameworks) && !availableFrameworks.contains(annotation.framework())) {
            return false;
        }
        return true;
    }

    public final <T extends OpenCgaToolExecutor> T getToolExecutor(String toolId, String toolExecutorId, Class<T> clazz,
                                                                      List<ToolExecutor.Source> sourceTypes,
                                                                      List<ToolExecutor.Framework> availableFrameworks)
            throws ToolExecutorException {
        Class<? extends T> executorClass = getToolExecutorClass(toolId, toolExecutorId, clazz, sourceTypes, availableFrameworks);
        if (executorClass == null) {
            throw ToolExecutorException.executorNotFound(clazz, toolId, toolExecutorId, sourceTypes, availableFrameworks);
        }
        try {
            T t = executorClass.newInstance();
            logger.info("Using " + clazz.getName() + " '" + t.getId() + "' : " + executorClass);

            return t;
        } catch (InstantiationException | IllegalAccessException e) {
            throw ToolExecutorException.cantInstantiate(executorClass, e);
        }
    }

}
