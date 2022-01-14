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

import org.opencb.opencga.app.cli.GeneralCliOptions;
import org.opencb.opencga.app.cli.session.CliSessionManager;
import org.opencb.opencga.catalog.exceptions.CatalogAuthenticationException;
import org.opencb.opencga.client.config.ClientConfiguration;
import org.opencb.opencga.client.rest.OpenCGAClient;

/**
 * Created by imedina on 27/05/16.
 */
public class OpencgaMain {

    private static OpencgaCliShellExecutor shell;

    public OpencgaMain() {

    }

    public static void main(String[] args) {

        try {
            if (args.length == 1 && "--shell".equals(args[0]) || (args.length == 2 && "--shell".equals(args[0]) && "--debug".equals(args[1]))) {
                initShell(args);
            } else {
                OpencgaCliProcessor.execute(args);
            }
        } catch (Exception e) {
            CommandLineUtils.printError("Failed to initialize OpenCGA CLI", e);
        }
    }

    public static void initShell(String[] args) {
        try {
            shell = new OpencgaCliShellExecutor(new GeneralCliOptions.CommonCommandOptions());
            CliSessionManager.getInstance().init(args, shell);
            CliSessionManager.getInstance().loadSessionStudies();
            CommandLineUtils.printDebug("Shell created ");
            shell.execute();
        } catch (CatalogAuthenticationException e) {
            CommandLineUtils.printError("Failed to initialize shell", e);
        } catch (Exception e) {
            CommandLineUtils.printError("Failed to execute shell", e);
        }
    }

    public static boolean isShell() {
        return shell != null;
    }

    public static OpencgaCliShellExecutor getShell() {
        return shell;
    }

    public static void setShell(OpencgaCliShellExecutor shell) {
        OpencgaMain.shell = shell;
    }

    public static ClientConfiguration getClientConfiguration() {
        return shell.getClientConfiguration();
    }

    public static OpenCGAClient getClient() {
        return shell.getOpenCGAClient();
    }
}