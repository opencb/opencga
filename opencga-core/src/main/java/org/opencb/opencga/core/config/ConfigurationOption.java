package org.opencb.opencga.core.config;

public interface ConfigurationOption {

    String key();

    <T> T defaultValue();

    default boolean isProtected() {
        return false;
    }

//    default boolean isFinal() {
//        return false;
//    }

//    default boolean isModifiableAfterLoading() {
//        return false;
//    }
}
