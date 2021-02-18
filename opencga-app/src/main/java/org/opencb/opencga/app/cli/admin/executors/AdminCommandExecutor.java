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

import org.apache.commons.lang3.StringUtils;
import org.opencb.opencga.app.cli.CommandExecutor;
import org.opencb.opencga.app.cli.admin.AdminCliOptionsParser;
import org.opencb.opencga.catalog.exceptions.CatalogException;
import org.opencb.opencga.core.config.Admin;

import java.util.Collections;

/**
 * Created on 03/05/16
 *
 * @author Jacobo Coll &lt;jacobo167@gmail.com&gt;
 */
public abstract class AdminCommandExecutor extends CommandExecutor {

    protected String adminPassword;


    public AdminCommandExecutor(AdminCliOptionsParser.AdminCommonCommandOptions options) {
        super(options.commonOptions);
        this.adminPassword = options.adminPassword;
    }

    public String getAdminPassword() {
        return adminPassword;
    }

    public CommandExecutor setAdminPassword(String adminPassword) {
        this.adminPassword = adminPassword;
        return this;
    }

    protected void setCatalogDatabaseCredentials(AdminCliOptionsParser.CatalogDatabaseCommandOptions catalogDatabaseOptions,
                                                 AdminCliOptionsParser.AdminCommonCommandOptions adminOptions) throws CatalogException {
        setCatalogDatabaseCredentials(catalogDatabaseOptions.databaseHost,
                catalogDatabaseOptions.prefix, catalogDatabaseOptions.databaseUser,
                catalogDatabaseOptions.databasePassword,
                adminOptions.adminPassword);
    }
    protected void setCatalogDatabaseCredentials(String host, String prefix, String user, String password, String adminPassword)
            throws CatalogException {

        if (StringUtils.isNotEmpty(host)) {
            configuration.getCatalog().getDatabase().setHosts(Collections.singletonList(host));
        }

        if (StringUtils.isNotEmpty(prefix)) {
            configuration.setDatabasePrefix(prefix);
        }

        if (StringUtils.isNotEmpty(user)) {
            configuration.getCatalog().getDatabase().setUser(user);
        }

        if (configuration.getAdmin() == null) {
            configuration.setAdmin(new Admin());
        }
        if (StringUtils.isNotEmpty(password)) {
            configuration.getCatalog().getDatabase().setPassword(password);
        }

        if (StringUtils.isEmpty(adminPassword)) {
            throw new CatalogException("No admin password found. Please, insert your password.");
        }
    }
}
