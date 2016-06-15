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

package org.opencb.opencga.app.cli.analysis;

import org.opencb.opencga.app.cli.CommandExecutor;
import org.opencb.opencga.app.cli.GeneralCliOptions;

import java.io.IOException;

/**
 * Created on 03/05/16
 *
 * @author Jacobo Coll &lt;jacobo167@gmail.com&gt;
 */
public abstract class AnalysisCommandExecutor extends CommandExecutor {

    public AnalysisCommandExecutor(GeneralCliOptions.CommonCommandOptions options) {
        super(options);
    }

}
