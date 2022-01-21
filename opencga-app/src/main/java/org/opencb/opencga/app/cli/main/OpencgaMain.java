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
import org.opencb.opencga.app.cli.main.processors.CommandProcessor;
import org.opencb.opencga.app.cli.main.shell.Shell;
import org.opencb.opencga.app.cli.main.utils.CommandLineUtils;
import org.opencb.opencga.catalog.exceptions.CatalogAuthenticationException;

import java.util.Arrays;
import java.util.Locale;
import java.util.logging.Level;


/**
 * Created by imedina on 27/05/16.
 */
public class OpencgaMain {

    public static Mode mode = Mode.CLI;
    public static Shell shell;
    public static Level logLevel = Level.OFF;

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
                String level = args[ArrayUtils.indexOf(args, "--log-level") + 1].toLowerCase(Locale.ROOT);
                setLogLevel(getNormalizedLogLevel(level));
            } catch (Exception e) {
                setLogLevel(Level.SEVERE);
                CommandLineUtils.error("Invalid log level. Valid values are INFO, WARN, DEBUG, ERROR", e);
                System.exit(0);
            }
        }


    }

    private static Level getNormalizedLogLevel(String level) {
        switch (level) {
            case "debug":
            case "fine":
                return Level.FINE;
            case "info":
                return Level.INFO;
            case "warning":
            case "warn":
                return Level.WARNING;
            case "error":
            case "sever":
                return Level.SEVERE;
            default:
                return Level.OFF;
        }
    }


    private static void executeCli(String[] args) throws CatalogAuthenticationException {
        // TODO maybe we should process specific args here?
        args = parseCliParams(args);
        if (!ArrayUtils.isEmpty(args)) {
            CommandProcessor processor = new CommandProcessor();
            processor.process(args);
        }
    }

    public static void executeShell(String[] args) {
        CommandLineUtils.printLog("Initializing Shell...  ");

        try {
            // If the shell launch command includes a host it is set to be used
            GeneralCliOptions.CommonCommandOptions options = new GeneralCliOptions.CommonCommandOptions();
            if (ArrayUtils.contains(args, "--host")) {
                options.host = args[ArrayUtils.indexOf(args, "--host") + 1];
            }
            // Create a shell executor instance
            shell = new Shell(options);
            CommandLineUtils.printLog("Shell created ");
            // Launch execute command to begin the execution
            shell.execute();
        } catch (CatalogAuthenticationException e) {
            CommandLineUtils.printLog("Failed to initialize shell", e);
        } catch (Exception e) {
            CommandLineUtils.printLog("Failed to execute shell", e);
        }
    }

    private static String[] normalizePasswordArgs(String[] args, String s) {
        for (int i = 0; i < args.length; i++) {
            if (s.equals(args[i])) {
                args[i] = "--password";
                break;
            }
        }
        return args;
    }

    public static String[] parseCliParams(String[] args) throws CatalogAuthenticationException {
        CommandLineUtils.printLog("Executing " + String.join(" ", args));
        if (CommandLineUtils.isNotHelpCommand(args)) {
            if (ArrayUtils.contains(args, "--user-password")) {
                normalizePasswordArgs(args, "--user-password");
            }
        }
        CommandLineUtils.printLog("CLI PARSED PARAMS ::: " + String.join(", ", args));

        return CommandLineUtils.processShortCuts(args);
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

    public static Level getLogLevel() {
        return logLevel;
    }

    public static void setLogLevel(Level logLevel) {
        OpencgaMain.logLevel = logLevel;
    }

    public enum Mode {
        SHELL, CLI
    }

}