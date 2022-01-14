/*
 * Copyright 2015-2021-11-23 OpenCB
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

import org.jline.reader.Candidate;
import org.opencb.opencga.app.cli.session.CliSessionManager;
import org.opencb.opencga.client.config.HostConfig;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;


public class OpenCgaCompleterImpl extends OpenCgaCompleter {

    public List<Candidate> checkCandidates(Map<String, List<Candidate>> candidatesMap, String line) {
        List<Candidate> res = new ArrayList();
        if (line.trim().contains(" ") || candidatesMap.containsKey(line)) {
            String[] commandLine = line.split(" ");
            if (commandLine.length == 2 && commandLine[0].equals("use") && commandLine[1].equals("host")) {
                res = getHostCandidates();
            } else if (commandLine.length == 2 && commandLine[0].equals("use") && commandLine[1].equals("study")) {
                res = getStudyCandidates();
            } else if (commandLine.length == 2) {
                for (String candidate : candidatesMap.keySet()) {
                    if (candidate.equals(commandLine[0])) {
                        res = getCandidates(candidatesMap.get(candidate), commandLine[1]);
                    }
                }
            } else if (commandLine.length == 1 && candidatesMap.containsKey(line)) {
                res.addAll(candidatesMap.get(line));
            }
        } else {
            res = getCandidates(commands, line);
        }
        return res;
    }

    private List<Candidate> getHostCandidates() {
        List<HostConfig> hostConfigs = CliSessionManager.getInstance().getClientConfiguration().getRest().getHosts();
        List<Candidate> res = new ArrayList();
        for (HostConfig hostConfig : hostConfigs) {
            res.add(new Candidate(hostConfig.getName()));
        }
        return res;
    }

    private List<Candidate> getStudyCandidates() {
        List<String> studies = CliSessionManager.getInstance().getStudies();
        List<Candidate> res = new ArrayList();
        for (String study : studies) {
            res.add(new Candidate(study));
        }
        return res;
    }

    private List<Candidate> getCandidates(List<Candidate> list, String s) {
        List<Candidate> res = new ArrayList();
        for (Candidate candidate : list) {
            if (candidate.value().startsWith(s)) {
                res.add(candidate);
            }
        }
        return res;
    }
}