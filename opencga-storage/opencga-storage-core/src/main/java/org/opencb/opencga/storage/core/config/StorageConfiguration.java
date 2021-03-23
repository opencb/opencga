/*
 * Copyright 2015-2017 OpenCB
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

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.MapperFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import org.apache.commons.lang3.StringUtils;
import org.opencb.commons.datastore.core.ObjectMap;
import org.opencb.opencga.core.config.SearchConfiguration;
import org.opencb.opencga.core.config.ServerConfiguration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

/**
 * Created by imedina on 30/04/15.
 */
@JsonIgnoreProperties({"storageEngine", "studyMetadataManager"})
public class StorageConfiguration {

    private CellBaseConfiguration cellbase;
    private ServerConfiguration server;
    private CacheConfiguration cache;
    private SearchConfiguration search;
    private SearchConfiguration clinical;
    private SearchConfiguration rga;
    private ObjectMap alignment;
    private StorageEnginesConfiguration variant;
    private IOConfiguration io;

    private BenchmarkConfiguration benchmark;


    protected static Logger logger = LoggerFactory.getLogger(StorageConfiguration.class);

    public StorageConfiguration() {
        this.alignment = new ObjectMap();
        this.variant = new StorageEnginesConfiguration();
        this.cellbase = new CellBaseConfiguration();
        this.server = new ServerConfiguration();
        this.cache = new CacheConfiguration();
        this.search = new SearchConfiguration();
        this.clinical = new SearchConfiguration();
        this.rga = new SearchConfiguration();
    }


    public static StorageConfiguration load(InputStream configurationInputStream) throws IOException {
        return load(configurationInputStream, "yaml");
    }

    public static StorageConfiguration load(InputStream configurationInputStream, String format) throws IOException {
        return load(configurationInputStream, format, false);
    }

    public static StorageConfiguration load(InputStream configurationInputStream, String format, boolean failOnUnknown) throws IOException {
        StorageConfiguration storageConfiguration;
        ObjectMapper objectMapper;
        switch (format) {
            case "json":
                objectMapper = new ObjectMapper();
                break;
            case "yml":
            case "yaml":
                objectMapper = new ObjectMapper(new YAMLFactory());
                break;
            default:
                throw new IllegalArgumentException("Unknown format " + format);
        }
        objectMapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, failOnUnknown);
        storageConfiguration = objectMapper.readValue(configurationInputStream, StorageConfiguration.class);

        return storageConfiguration;
    }

    public void serialize(OutputStream configurationOutputStream) throws IOException {
        ObjectMapper jsonMapper = new ObjectMapper(new YAMLFactory());
        jsonMapper.configure(MapperFeature.REQUIRE_SETTERS_FOR_GETTERS, true);
        jsonMapper.writerWithDefaultPrettyPrinter().writeValue(configurationOutputStream, this);
    }

    public ObjectMap getAlignment() {
        return alignment;
    }

    public StorageConfiguration setAlignment(ObjectMap alignment) {
        this.alignment = alignment;
        return this;
    }

    public StorageEnginesConfiguration getVariant() {
        return variant;
    }

    public StorageEngineConfiguration getVariantEngine() {
        return getVariantEngine(variant.getDefaultEngine());
    }

    public StorageEngineConfiguration getVariantEngine(String storageEngine) {
        if (variant.getEngines() == null || variant.getEngines().isEmpty()) {
            throw new IllegalStateException("Variant engines list is empty!");
        }
        if (StringUtils.isEmpty(storageEngine)) {
            return variant.getEngines().get(0);
        }

        for (StorageEngineConfiguration engine : variant.getEngines()) {
            if (engine.getId().equals(storageEngine)) {
                return engine;
            }
        }

        throw new IllegalArgumentException("Unknown variant storage engine '" + storageEngine + "'");
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("StorageConfiguration{");
        sb.append(", cellbase=").append(cellbase);
        sb.append(", server=").append(server);
        sb.append(", cache=").append(cache);
        sb.append(", search=").append(search);
        sb.append(", search=").append(clinical);
        sb.append(", rga=").append(rga);
        sb.append(", benchmark=").append(benchmark);
        sb.append('}');
        return sb.toString();
    }

    public CellBaseConfiguration getCellbase() {
        return cellbase;
    }

    public StorageConfiguration setCellbase(CellBaseConfiguration cellbase) {
        this.cellbase = cellbase;
        return this;
    }

    public ServerConfiguration getServer() {
        return server;
    }

    public StorageConfiguration setServer(ServerConfiguration server) {
        this.server = server;
        return this;
    }

    public CacheConfiguration getCache() {
        return cache;
    }

    public StorageConfiguration setCache(CacheConfiguration cache) {
        this.cache = cache;
        return this;
    }

    public SearchConfiguration getSearch() {
        return search;
    }

    public StorageConfiguration setSearch(SearchConfiguration search) {
        this.search = search;
        return this;
    }

    public SearchConfiguration getClinical() {
        return clinical;
    }

    public StorageConfiguration setClinical(SearchConfiguration clinical) {
        this.clinical = clinical;
        return this;
    }

    public SearchConfiguration getRga() {
        return rga;
    }

    public StorageConfiguration setRga(SearchConfiguration rga) {
        this.rga = rga;
        return this;
    }

    public IOConfiguration getIo() {
        return io;
    }

    public StorageConfiguration setIo(IOConfiguration io) {
        this.io = io;
        return this;
    }

    public BenchmarkConfiguration getBenchmark() {
        return benchmark;
    }

    public StorageConfiguration setBenchmark(BenchmarkConfiguration benchmark) {
        this.benchmark = benchmark;
        return this;
    }

}
