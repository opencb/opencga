package org.opencb.opencga.storage.core.config;

import com.fasterxml.jackson.annotation.JsonProperty;
import org.opencb.commons.datastore.core.ObjectMap;

import java.util.Map;

/**
 * Created on 03/05/19.
 *
 * @author Jacobo Coll &lt;jacobo167@gmail.com&gt;
 */
public class IOManagersConfiguration {

    public IOManagersConfiguration() {
    }

    public IOManagersConfiguration(Map<String, IOManagerConfiguration> managers) {
        this.managers = managers;
    }

    private Map<String, IOManagerConfiguration> managers;

    public Map<String, IOManagerConfiguration> getManagers() {
        return managers;
    }

    public IOManagersConfiguration setManagers(Map<String, IOManagerConfiguration> managers) {
        this.managers = managers;
        return this;
    }

    public static class IOManagerConfiguration {

        public IOManagerConfiguration() {
        }

        public IOManagerConfiguration(String clazz, ObjectMap options) {
            this.clazz = clazz;
            this.options = options;
        }

        @JsonProperty("class")
        private String clazz;

        private ObjectMap options;

        public String getClazz() {
            return clazz;
        }

        public IOManagerConfiguration setClazz(String clazz) {
            this.clazz = clazz;
            return this;
        }

        public ObjectMap getOptions() {
            return options;
        }

        public IOManagerConfiguration setOptions(ObjectMap options) {
            this.options = options;
            return this;
        }
    }


}
