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

package org.opencb.opencga.storage.app.cli;

/**
 * Created by imedina on 25/05/15.
 */
public class FetchAlignmentsCommandExecutor extends CommandExecutor {

    private CliOptionsParser.QueryAlignmentsCommandOptions queryAlignmentsCommandOptions;


    public FetchAlignmentsCommandExecutor(CliOptionsParser.QueryAlignmentsCommandOptions queryAlignmentsCommandOptions) {
        super(queryAlignmentsCommandOptions.logLevel, queryAlignmentsCommandOptions.verbose,
                queryAlignmentsCommandOptions.configFile);

        this.queryAlignmentsCommandOptions = queryAlignmentsCommandOptions;
    }


    @Override
    public void execute() {

    }
}
