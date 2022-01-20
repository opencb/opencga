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
import org.opencb.opencga.app.cli.main.parser.CliParamParser;
import org.opencb.opencga.app.cli.main.processors.CliCommandProcessor;
import org.opencb.opencga.app.cli.main.shell.Shell;
import org.opencb.opencga.app.cli.main.utils.CommandLineUtils;
import org.opencb.opencga.app.cli.session.LogLevel;
import org.opencb.opencga.catalog.exceptions.CatalogAuthenticationException;

import java.util.Arrays;
import java.util.Locale;


/**
 * Created by imedina on 27/05/16.
 */
public class OpencgaMain {

    public static Mode mode = Mode.CLI;
    public static Shell shell;
    public static LogLevel logLevel = LogLevel.OFF;

    public static void main(String[] args) {
        checkLogLevel(args);
        checkMode(args);
        CommandLineUtils.printLog(Arrays.toString(args));
        try {
            if (Mode.SHELL.equals(getMode())) {
                executeShell(args);
            } else {
                executeCli(args);
            }
        } catch (Exception e) {
            CommandLineUtils.error("Failed to initialize OpenCGA CLI " + e.getMessage(), e);
            e.printStackTrace();
        }
    }

    private static void checkMode(String[] args) {
        if (ArrayUtils.contains(args, "--shell")) {
            setMode(Mode.SHELL);
        } else {
            setMode(Mode.CLI);
        }
        CommandLineUtils.printLog("Execution mode " + getMode());
    }


    private static void checkLogLevel(String[] args) {
        if (ArrayUtils.contains(args, "--log-level")) {
            try {
                String level = args[ArrayUtils.indexOf(args, "--log-level") + 1].toUpperCase(Locale.ROOT);
                setLogLevel(LogLevel.valueOf(level));
            } catch (Exception e) {
                setLogLevel(LogLevel.ERROR);
                CommandLineUtils.error("Invalid log level. Valid values are INFO, WARN, DEBUG, ERROR", e);
                System.exit(0);
            }
        }


    }


    private static void executeCli(String[] args) throws CatalogAuthenticationException {
        // TODO maybe we should process specific args here?

        CliCommandProcessor processor = new CliCommandProcessor(new CliParamParser());
        processor.process(args);
    }

    public static void executeShell(String[] args) {
        CommandLineUtils.printLog("Initializing Shell...  ");

        try {
            GeneralCliOptions.CommonCommandOptions options = new GeneralCliOptions.CommonCommandOptions();
            if (ArrayUtils.contains(args, "--host")) {
                options.host = args[ArrayUtils.indexOf(args, "--host") + 1];
            }
            shell = new Shell(options);
            CommandLineUtils.printLog("Shell created ");
            shell.execute();
        } catch (CatalogAuthenticationException e) {
            CommandLineUtils.printLog("Failed to initialize shell", e);
        } catch (Exception e) {
            CommandLineUtils.printLog("Failed to execute shell", e);
        }
    }

    public static Mode getMode() {
        return mode;
    }

    public static void setMode(Mode mode) {
        OpencgaMain.mode = mode;
    }

    public static boolean isShellMode() {
        return getMode().equals(Mode.SHELL);
    }

    public static Shell getShell() {
        return shell;
    }

    public static void setShell(Shell shell) {
        OpencgaMain.shell = shell;
    }

    public static LogLevel getLogLevel() {
        return logLevel;
    }

    public static void setLogLevel(LogLevel logLevel) {
        OpencgaMain.logLevel = logLevel;
    }

    public enum Mode {
        SHELL, CLI
    }


}