package com.zettagenomics.opencga.enterprise.server;

import com.zettagenomics.opencga.enterprise.server.rest.CvaWSServer;
import com.zettagenomics.opencga.enterprise.server.rest.EnterpriseMetaWSServer;
import org.glassfish.jersey.server.ResourceConfig;
import org.reflections.Reflections;
import org.reflections.scanners.SubTypesScanner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.ws.rs.ApplicationPath;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;

@ApplicationPath("resources")
public class EnterpriseResourceConfig extends ResourceConfig {

    private final static Logger logger;
    public static final Map<String, Class<?>> enterpriseClasses;

    static {
        logger = LoggerFactory.getLogger(EnterpriseResourceConfig.class);

        Set<String> excludedClasses = new HashSet<>();
        excludedClasses.add("MetaWSServer");

        Reflections reflections = new Reflections("org.opencb.opencga.server.rest", new SubTypesScanner(false));
        Set<Class<?>> collect = new HashSet<>(reflections.getSubTypesOf(Object.class));
        enterpriseClasses = new LinkedHashMap<>(collect.size());
        for (Class<?> aClass : collect) {
            if (excludedClasses.contains(aClass.getSimpleName())) {
                logger.debug("Excluded class '{}'", aClass.getName());
            } else {
                enterpriseClasses.put(aClass.getSimpleName(), aClass);
            }
        }

        enterpriseClasses.put("meta", EnterpriseMetaWSServer.class);
        enterpriseClasses.put("cva", CvaWSServer.class);
    }

    public EnterpriseResourceConfig() {
        for (Class<?> value : enterpriseClasses.values()) {
            register(value);
            logger.debug("Registered class '{}'", value.getName());
        }
    }

}
