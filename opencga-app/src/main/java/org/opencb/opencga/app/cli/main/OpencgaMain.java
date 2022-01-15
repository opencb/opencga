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

package org.opencb.opencga.app.cli.main;

import org.apache.commons.lang3.ArrayUtils;
import org.opencb.opencga.app.cli.GeneralCliOptions;
import org.opencb.opencga.app.cli.main.processors.CliProcessor;
import org.opencb.opencga.app.cli.session.CliSessionManager;
import org.opencb.opencga.catalog.exceptions.CatalogAuthenticationException;

/**
 * Created by imedina on 27/05/16.
 */
public class OpencgaMain {

    public static void main(String[] args) {
        CliSessionManager.getInstance().setDebug(ArrayUtils.contains(args, "--debug"));
        try {
            if (ArrayUtils.contains(args, "--shell")) {
                initShell(args);
            } else {
                CliProcessor processor = new CliProcessor();
                processor.execute(args);
            }
        } catch (Exception e) {
            CommandLineUtils.printError("Failed to initialize OpenCGA CLI", e);
        }
    }

    public static void initShell(String[] args) {
        try {
            OpencgaCliShellExecutor shell = new OpencgaCliShellExecutor(new GeneralCliOptions.CommonCommandOptions());
            CliSessionManager.setShell(shell);
            CliSessionManager.getInstance().init(args, shell);
            CliSessionManager.getInstance().loadSessionStudies(shell);
            CommandLineUtils.printDebug("Shell created ");
            shell.execute();
        } catch (CatalogAuthenticationException e) {
            CommandLineUtils.printError("Failed to initialize shell", e);
        } catch (Exception e) {
            CommandLineUtils.printError("Failed to execute shell", e);
        }
    }


}