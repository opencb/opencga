package org.opencb.opencga.storage.core.alignment;

import org.opencb.biodata.tools.alignment.BamManager;
import org.opencb.opencga.core.config.ConfigurationOption;

public enum AlignmentStorageOptions implements ConfigurationOption {

    BIG_WIG_WINDOWS_SIZE("bigWigWindowsSize", BamManager.DEFAULT_WINDOW_SIZE);

    private final String key;
    private final Object value;

    AlignmentStorageOptions(String key) {
        this.key = key;
        this.value = null;
    }

    AlignmentStorageOptions(String key, Object value) {
        this.key = key;
        this.value = value;
    }

    public String key() {
        return key;
    }

    @SuppressWarnings("unchecked")
    public <T> T defaultValue() {
        return (T) value;
    }
}
