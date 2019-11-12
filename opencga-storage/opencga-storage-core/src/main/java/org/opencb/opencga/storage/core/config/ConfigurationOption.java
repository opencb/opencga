package org.opencb.opencga.storage.core.config;

public interface ConfigurationOption {

    String key();

    <T> T defaultValue();
}
