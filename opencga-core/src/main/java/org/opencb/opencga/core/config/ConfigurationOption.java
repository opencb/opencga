package org.opencb.opencga.core.config;

public interface ConfigurationOption {

    String key();

    <T> T defaultValue();

//    default boolean isFinal() {
//        return false;
//    }
}
