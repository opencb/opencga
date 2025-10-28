package org.opencb.opencga.server.generator.commons;

import java.util.List;

public interface ApiCommons {

    List<Class<?>> getApiClasses();

    String getVersion();
    // List<String> getOrderCategories();
}
