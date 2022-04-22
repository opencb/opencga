/*
 * Copyright 2015-2020 OpenCB
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

package org.opencb.opencga.test.manager;

import com.beust.jcommander.ParameterException;
import org.apache.commons.lang3.ArrayUtils;
import org.opencb.commons.utils.PrintUtils;
import org.opencb.opencga.test.cli.OptionsParser;
import org.opencb.opencga.test.utils.OpencgaLogger;

import java.util.Date;

public class TestGeneratorManager {

    public static void main(String[] args) {
        Date start = new Date();
        PrintUtils.printShellHeader(PrintUtils.Color.BLUE);
        try {
            OptionsParser.parseArgs(args);
        } catch (ParameterException p) {
            PrintUtils.println("Parameter exception: ", PrintUtils.Color.CYAN, p.getMessage(), PrintUtils.Color.WHITE);
            OptionsParser.printUsage();
            System.exit(-1);
        } catch (Exception t) {
            t.printStackTrace();
            OpencgaLogger.printLog(t.getMessage(), t);
            OptionsParser.printUsage();
            System.exit(-1);
        }
        Date end = new Date();
        if (!(ArrayUtils.contains(args, "--help") || ArrayUtils.contains(args, "--version"))) {
            PrintUtils.printTimeElapsed(start, end);
        }

    }

}
