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

import org.opencb.opencga.app.cli.main.impl.CommandLineImpl;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Created by imedina on 27/05/16.
 */
public class OpencgaCommandLine {

    private static final Logger logger = LoggerFactory.getLogger(OpencgaCommandLine.class);
    private static CommandLineImpl commandLine;


    public static void main(String[] args) {
        commandLine = new CommandLineImpl("Opencga");
        commandLine.init(args);
    }


    public static boolean isShellMode() {
        return commandLine.isShellMode();
    }


    public static CommandLineImpl getCommandLine() {
        return commandLine;
    }
}