package org.opencb.opencga.analysis.tools;

import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.opencb.opencga.core.annotations.ToolExecutor;
import org.opencb.opencga.core.exception.ToolExecutorException;
import org.opencb.opencga.core.tools.OpenCgaToolExecutor;
import org.reflections.Reflections;
import org.reflections.scanners.SubTypesScanner;
import org.reflections.scanners.TypeAnnotationsScanner;
import org.reflections.util.ClasspathHelper;
import org.reflections.util.ConfigurationBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.Modifier;
import java.util.*;

public class ToolExecutorFactory {
    private final Logger logger = LoggerFactory.getLogger(ToolExecutorFactory.class);

    public final Class<? extends OpenCgaToolExecutor> getToolExecutorClass(String toolId, String toolExecutorId) {
        return getToolExecutorClass(toolId, toolExecutorId, OpenCgaToolExecutor.class);
    }

    public final <T extends OpenCgaToolExecutor> Class<? extends T>  getToolExecutorClass(String toolId, String toolExecutorId, Class<T> clazz) {
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
            if (isValidClass(toolId, toolExecutorId, clazz, sourceTypes, availableFrameworks)) {
                if (StringUtils.isNotEmpty(toolExecutorId) || Modifier.isFinal(clazz.getModifiers())) {
                    // Shortcut to skip reflection
                    return clazz;
                }
                candidateClasses.add(clazz);
            }
        }

        Reflections reflections = new Reflections(new ConfigurationBuilder()
                .setScanners(
                        new SubTypesScanner(),
                        new TypeAnnotationsScanner().filterResultsBy(s -> StringUtils.equals(s, ToolExecutor.class.getName())))
                .addUrls(ClasspathHelper.forJavaClassPath())
                .filterInputsBy(input -> input.endsWith(".class"))
        );

        Set<Class<? extends T>> typesAnnotatedWith = reflections.getSubTypesOf(clazz);
        for (Class<? extends T> aClass : typesAnnotatedWith) {
            if (isValidClass(toolId, toolExecutorId, aClass, sourceTypes, availableFrameworks)) {
                candidateClasses.add(aClass);
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

    private <T> boolean isValidClass(String toolId, String toolExecutorId, Class<T> aClass,
                                     List<ToolExecutor.Source> sourceTypes,
                                     List<ToolExecutor.Framework> availableFrameworks) {
        ToolExecutor annotation = aClass.getAnnotation(ToolExecutor.class);
        if (annotation != null) {
            if (annotation.tool().equals(toolId)) {
                if (StringUtils.isEmpty(toolExecutorId) || toolExecutorId.equals(annotation.id())) {
                    if (CollectionUtils.isEmpty(sourceTypes) || sourceTypes.contains(annotation.source())) {
                        if (CollectionUtils.isEmpty(availableFrameworks) || availableFrameworks.contains(annotation.framework())) {
                            return true;
                        }
                    }
                }
            }
        }
        return false;
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
