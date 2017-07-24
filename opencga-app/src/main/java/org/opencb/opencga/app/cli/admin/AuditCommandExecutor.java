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

package org.opencb.opencga.app.cli.admin;

/**
 * Created by imedina on 21/04/16.
 */
public class AuditCommandExecutor extends AdminCommandExecutor {

    private AdminCliOptionsParser.AuditCommandOptions auditCommandOptions;

    public AuditCommandExecutor(AdminCliOptionsParser.AuditCommandOptions auditCommandOptions) {
        super(auditCommandOptions.commonOptions);
        this.auditCommandOptions = auditCommandOptions;
    }

    @Override
    public void execute() throws Exception {
        logger.debug("Executing variant command line");

        String subCommandString = auditCommandOptions.getParsedSubCommand();
        switch (subCommandString) {
            case "query":
                query();
                break;
            case "stats":
                stats();
                break;
            default:
                logger.error("Subcommand not valid");
                break;
        }
    }

    private void query() {

    }

    private void stats() {

    }
}
