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
import org.opencb.opencga.catalog.exceptions.CatalogAuthenticationException;

import java.util.Arrays;

/**
 * Created by imedina on 27/05/16.
 */
public class OpencgaMain {

    public static Mode mode = Mode.CLI;
    public static boolean debug = false;
    public static OpencgaCliShellExecutor shell;

    public static void main(String[] args) {
        args = checkDebugMode(args);
        CommandLineUtils.printDebug(Arrays.toString(args));
        try {
            if (ArrayUtils.contains(args, "--shell")) {
                setMode(Mode.SHELL);
            }
            if (Mode.SHELL.equals(getMode())) {
                executeShell(args);
            } else {
                executeCli(args);
            }
        } catch (Exception e) {
            CommandLineUtils.printError("Failed to initialize OpenCGA CLI " + e.getMessage(), e);
            e.printStackTrace();
        }
    }


    private static String[] checkDebugMode(String[] args) {
        setDebug(ArrayUtils.contains(args, "--debug"));
        if (isDebug()) {
            args = ArrayUtils.remove(args, ArrayUtils.indexOf(args, "--debug"));
        }
        return args;
    }


    private static void executeCli(String[] args) throws CatalogAuthenticationException {
        // TODO maybe we should process specific args here?

        CliProcessor processor = new CliProcessor();
        processor.execute(args);
    }

    public static void executeShell(String[] args) {
        CommandLineUtils.printDebug("Initializing Shell...  ");

        try {
            GeneralCliOptions.CommonCommandOptions options = new GeneralCliOptions.CommonCommandOptions();
            if (ArrayUtils.contains(args, "--host")) {
                options.host = args[ArrayUtils.indexOf(args, "--host") + 1];
            }
            shell = new OpencgaCliShellExecutor(options);
            CommandLineUtils.printDebug("Shell created ");
            shell.execute();
        } catch (CatalogAuthenticationException e) {
            CommandLineUtils.printError("Failed to initialize shell", e);
        } catch (Exception e) {
            CommandLineUtils.printError("Failed to execute shell", e);
        }
    }

    public static Mode getMode() {
        return mode;
    }

    public static void setMode(Mode mode) {
        OpencgaMain.mode = mode;
    }

    public static boolean isDebug() {
        return debug;
    }

    public static void setDebug(boolean debug) {
        OpencgaMain.debug = debug;
    }

    public static boolean isShellMode() {
        return getMode().equals(Mode.SHELL);
    }

    public static OpencgaCliShellExecutor getShell() {
        return shell;
    }

    public static void setShell(OpencgaCliShellExecutor shell) {
        OpencgaMain.shell = shell;
    }

    public enum Mode {
        SHELL, CLI
    }
}