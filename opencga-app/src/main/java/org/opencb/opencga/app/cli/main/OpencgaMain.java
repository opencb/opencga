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

import org.opencb.opencga.app.cli.session.CliSessionManager;
import org.opencb.opencga.core.common.ExceptionUtils;
import org.opencb.opencga.core.common.GitRepositoryState;

/**
 * Created by imedina on 27/05/16.
 */
public class OpencgaMain {

    public static final String VERSION = GitRepositoryState.get().getBuildVersion();

    public static void main(String[] args) {
        try {
            CliSessionManager.init(args);
            if (args.length == 1 && "--shell".equals(args[0]) || (args.length == 2 && "--shell".equals(args[0]) && "--debug".equals(args[1]))) {
                CliSessionManager.initShell();
                OpencgaMain.printDebugMessage("Shell created ");
                CliSessionManager.getShell().execute();
            } else {
                OpencgaCliProcessor.process(args);
            }
        } catch (Exception e) {
            printErrorMessage(e.getMessage(), e);
        }
    }

    public static void printErrorMessage(String s) {
        printErrorMessage(s, null);
    }

    public static void printDebugMessage(String s, String[] args) {
        String message = "";
        for (int i = 0; i < args.length; i++) {
            message += args[i] + " ";
        }
        printDebugMessage(s.trim() + " " + message);
    }

    public static void printDebugMessage(String message) {

        if (CliSessionManager.isDebug()) {
            if (CliSessionManager.isShell()) {
                CliSessionManager.getShell().printlnYellow(message);
            } else {
                System.err.println(message);
            }
        }
    }

    public static void printErrorMessage(String s, Throwable e) {
        if (CliSessionManager.isShell()) {
            CliSessionManager.getShell().printlnRed(s);
            if (e != null) {
                if (CliSessionManager.isDebug()) {
                    CliSessionManager.getShell().printlnYellow(ExceptionUtils.prettyExceptionStackTrace(e));
                } else {
                    if (!s.equals(e.getMessage())) {
                        CliSessionManager.getShell().printlnRed(e.getMessage());
                    }
                }
            }
        } else {
            System.err.println(s);
            if (e != null && !s.equals(e.getMessage())) {
                System.err.println(e.getMessage());
            }
        }
    }

    public static void printWarningMessage(String s) {
        if (CliSessionManager.isShell()) {
            CliSessionManager.getShell().printlnYellow(s);
        } else {
            System.err.println(s);
        }
    }

    public static void printInfoMessage(String s) {
        if (CliSessionManager.isShell()) {
            CliSessionManager.getShell().printlnGreen(s);
        } else {
            System.err.println(s);
        }
    }
}