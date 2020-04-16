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

package org.opencb.opencga.app.cli.admin.executors;

/**
 * Created by wasim on 08/06/17.
 */

import org.opencb.commons.datastore.core.ObjectMap;
import org.opencb.opencga.app.cli.admin.AdminCliOptionsParser;
import org.opencb.opencga.app.cli.admin.AdminCliOptionsParser.MetaCommandOptions;
import org.opencb.opencga.catalog.db.api.MetaDBAdaptor;
import org.opencb.opencga.catalog.exceptions.CatalogException;
import org.opencb.opencga.catalog.managers.CatalogManager;

import java.io.IOException;

public class MetaCommandExecutor extends AdminCommandExecutor {
    private MetaCommandOptions metaCommandOptions;

    public MetaCommandExecutor(MetaCommandOptions metaCommandOptions) {
        super(metaCommandOptions.commonOptions);
        this.metaCommandOptions = metaCommandOptions;
    }

    public void execute() throws Exception {
        this.logger.debug("Executing Meta command line");
        String subCommandString = metaCommandOptions.getParsedSubCommand();
        switch (subCommandString) {
            case "update":
                insertUpdatedAAdmin();
                break;
            default:
                logger.error("Subcommand not valid");
                break;
        }
    }

    private void insertUpdatedAAdmin() throws CatalogException, IOException {
        AdminCliOptionsParser.MetaKeyCommandOptions executor = this.metaCommandOptions.metaKeyCommandOptions;

        setCatalogDatabaseCredentials(executor.databaseHost, executor.prefix, executor.databaseUser, executor.databasePassword,
                executor.commonOptions.adminPassword);

        try (CatalogManager catalogManager = new CatalogManager(configuration)) {
            String token = catalogManager.getUserManager().loginAsAdmin(executor.commonOptions.adminPassword).getToken();

            ObjectMap params = new ObjectMap();
            params.putIfNotEmpty(MetaDBAdaptor.SECRET_KEY, executor.updateSecretKey);
            params.putIfNotEmpty(MetaDBAdaptor.ALGORITHM, executor.algorithm);

            catalogManager.updateJWTParameters(params, token);
        }

    }

}
