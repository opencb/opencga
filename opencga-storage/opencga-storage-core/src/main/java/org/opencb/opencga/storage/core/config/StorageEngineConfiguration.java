/*
 * Copyright 2015 OpenCB
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.opencb.opencga.storage.core.config;

import java.util.Map;

/**
 * Created by imedina on 01/05/15.
 */
public class StorageEngineConfiguration {

    private String id;
//    private String conf;

    /**
     * options parameter defines database-specific parameters
     */
    private Map<String, String> options;

    private StorageEtlConfiguration alignment;
    private StorageEtlConfiguration variant;

    public StorageEngineConfiguration() {

    }

    public StorageEngineConfiguration(String id, StorageEtlConfiguration alignment, StorageEtlConfiguration variant,
                                      Map<String, String> options) {
        this.id = id;
        this.alignment = alignment;
        this.variant = variant;
//        this.conf = conf;
        this.options = options;
    }

    @Override
    public String toString() {
        return "StorageEngineConfiguration{" +
                "id='" + id + '\'' +
//                ", conf='" + conf + '\'' +
                ", options=" + options +
                ", alignment=" + alignment +
                ", variant=" + variant +
                '}';
    }

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

//    public String getConf() {
//        return conf;
//    }
//
//    public void setConf(String conf) {
//        this.conf = conf;
//    }

    public Map<String, String> getOptions() {
        return options;
    }

    public void setOptions(Map<String, String> options) {
        this.options = options;
    }

    public StorageEtlConfiguration getAlignment() {
        return alignment;
    }

    public void setAlignment(StorageEtlConfiguration alignment) {
        this.alignment = alignment;
    }

    public StorageEtlConfiguration getVariant() {
        return variant;
    }

    public void setVariant(StorageEtlConfiguration variant) {
        this.variant = variant;
    }
}
