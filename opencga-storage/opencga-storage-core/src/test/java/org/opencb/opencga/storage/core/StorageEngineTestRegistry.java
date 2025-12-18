package org.opencb.opencga.storage.core;

import org.reflections.Reflections;
import org.reflections.scanners.SubTypesScanner;
import org.reflections.scanners.TypeAnnotationsScanner;
import org.reflections.util.ClasspathHelper;
import org.reflections.util.ConfigurationBuilder;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Discovers Storage Engine tests declared in core.
 */
public final class StorageEngineTestRegistry {

    public static final String CORE_TEST_PACKAGE = "org.opencb.opencga.storage.core";

    private StorageEngineTestRegistry() {
    }

    public static Set<Class<?>> findAnnotatedCoreTests() {
        Reflections reflections = new Reflections(new ConfigurationBuilder()
                .setUrls(ClasspathHelper.forPackage(CORE_TEST_PACKAGE))
                .setScanners(new TypeAnnotationsScanner()));
        return reflections.getTypesAnnotatedWith(StorageEngineTest.class, true)
                .stream()
                .filter(StorageEngineTestRegistry::isAbstractTest)
                .collect(Collectors.toSet());
    }

    public static boolean isAbstractTest(Class<?> clazz) {
        return !clazz.isInterface() && java.lang.reflect.Modifier.isAbstract(clazz.getModifiers());
    }

    public static Map<Class<?>, Set<Class<?>>> findImplementations(String basePackage) {
        Reflections reflections = new Reflections(new ConfigurationBuilder()
                .setUrls(ClasspathHelper.forPackage(basePackage))
                .setScanners(new SubTypesScanner()));
        Map<Class<?>, Set<Class<?>>> map = new HashMap<>();
        for (Class<?> coreTest : findAnnotatedCoreTests()) {
            @SuppressWarnings("unchecked")
            Set<Class<?>> subTypes = reflections.getSubTypesOf((Class<Object>) coreTest);
            if (!subTypes.isEmpty()) {
                map.put(coreTest, new HashSet<>(subTypes));
            }
        }
        return map;
    }

    public static Set<Class<?>> findMissingImplementations(Set<Class<?>> coreTests,
                                                           Map<Class<?>, Set<Class<?>>> implementationMap) {
        Set<Class<?>> missing = new HashSet<>();
        for (Class<?> coreTest : coreTests) {
            Set<Class<?>> impls = implementationMap.get(coreTest);
            if (impls == null || impls.isEmpty()) {
                missing.add(coreTest);
            }
        }
        return missing;
    }
}
