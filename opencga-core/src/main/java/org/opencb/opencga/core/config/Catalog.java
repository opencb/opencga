/*
 * Copyright 2015-2020 OpenCB
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

package org.opencb.opencga.core.config;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Created by pfurio on 01/02/17.
 */
public class Catalog {

    private DatabaseCredentials database;

    private static final Logger logger;

    static {
        logger = LoggerFactory.getLogger(Catalog.class);
    }

    public Catalog() {
    }

    public Catalog(DatabaseCredentials database) {
        this.database = database;
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("Catalog{");
        sb.append("database=").append(database);
        sb.append('}');
        return sb.toString();
    }

    public DatabaseCredentials getDatabase() {
        return database;
    }

    public Catalog setDatabase(DatabaseCredentials database) {
        this.database = database;
        return this;
    }

    @Deprecated
    public DatabaseCredentials getSearchEngine() {
        return null;
    }

    @Deprecated
    public Catalog setSearchEngine(DatabaseCredentials searchEngine) {
        logger.warn("Ignored configuration option 'configuration.yml#catalog.searchEngine' with value '{}'."
                        + " The option was deprecated and removed.", searchEngine);
        return this;
    }

}
