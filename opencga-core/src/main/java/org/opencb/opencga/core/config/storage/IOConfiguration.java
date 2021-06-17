package org.opencb.opencga.core.config.storage;

import com.fasterxml.jackson.annotation.JsonProperty;
import org.opencb.commons.datastore.core.ObjectMap;

import java.util.LinkedHashMap;
import java.util.Map;

/**
 * Created on 03/05/19.
 *
 * @author Jacobo Coll &lt;jacobo167@gmail.com&gt;
 */
public class IOConfiguration {

    public IOConfiguration() {
        connectors = new LinkedHashMap<>();
    }

    public IOConfiguration(Map<String, IOConnectorConfiguration> connectors) {
        this.connectors = connectors;
    }

    private Map<String, IOConnectorConfiguration> connectors;

    public Map<String, IOConnectorConfiguration> getConnectors() {
        return connectors;
    }

    public IOConfiguration setConnectors(Map<String, IOConnectorConfiguration> connectors) {
        this.connectors = connectors;
        return this;
    }

    public static class IOConnectorConfiguration extends ObjectMap {

        public IOConnectorConfiguration() {
        }

        public IOConnectorConfiguration(String clazz, ObjectMap options) {
            super(options);
            this.clazz = clazz;
        }

        @JsonProperty("class")
        private String clazz;

        public String getClazz() {
            return clazz;
        }

        public IOConnectorConfiguration setClazz(String clazz) {
            this.clazz = clazz;
            return this;
        }
    }


}
