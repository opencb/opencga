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

package org.opencb.opencga.master.monitor.models;

import org.opencb.opencga.core.models.file.File;
import org.opencb.opencga.core.models.job.JobInternal;
import org.opencb.opencga.core.models.job.JobStudyParam;
import org.opencb.opencga.core.models.job.JobUpdateParams;
import org.opencb.opencga.core.models.job.ToolInfo;
import org.opencb.opencga.core.tools.result.JobResult;

import java.util.List;
import java.util.Map;

public class PrivateJobUpdateParams extends JobUpdateParams {

    private String commandLine;

    private Map<String, Object> params;
    private ToolInfo tool;
    private JobInternal internal;

    private File outDir;
    private File tmpDir;
    private List<File> input;    // input files to this job
    private List<File> output;   // output files of this job

    private JobResult result;
    private JobStudyParam study;

    private File stdout;
    private File stderr;

    public PrivateJobUpdateParams() {
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("PrivateJobUpdateParams{");
        sb.append("commandLine='").append(commandLine).append('\'');
        sb.append(", params=").append(params);
        sb.append(", tool=").append(tool);
        sb.append(", internal=").append(internal);
        sb.append(", outDir=").append(outDir);
        sb.append(", tmpDir=").append(tmpDir);
        sb.append(", input=").append(input);
        sb.append(", output=").append(output);
        sb.append(", result=").append(result);
        sb.append(", study=").append(study);
        sb.append(", stdout=").append(stdout);
        sb.append(", stderr=").append(stderr);
        sb.append('}');
        return sb.toString();
    }

    public String getCommandLine() {
        return commandLine;
    }

    public PrivateJobUpdateParams setCommandLine(String commandLine) {
        this.commandLine = commandLine;
        return this;
    }

    public ToolInfo getTool() {
        return tool;
    }

    public PrivateJobUpdateParams setTool(ToolInfo tool) {
        this.tool = tool;
        return this;
    }

    public Map<String, Object> getParams() {
        return params;
    }

    public PrivateJobUpdateParams setParams(Map<String, Object> params) {
        this.params = params;
        return this;
    }

    public JobInternal getInternal() {
        return internal;
    }

    public PrivateJobUpdateParams setInternal(JobInternal internal) {
        this.internal = internal;
        return this;
    }

    public File getOutDir() {
        return outDir;
    }

    public PrivateJobUpdateParams setOutDir(File outDir) {
        this.outDir = outDir;
        return this;
    }

    public File getTmpDir() {
        return tmpDir;
    }

    public PrivateJobUpdateParams setTmpDir(File tmpDir) {
        this.tmpDir = tmpDir;
        return this;
    }

    public List<File> getInput() {
        return input;
    }

    public PrivateJobUpdateParams setInput(List<File> input) {
        this.input = input;
        return this;
    }

    public List<File> getOutput() {
        return output;
    }

    public PrivateJobUpdateParams setOutput(List<File> output) {
        this.output = output;
        return this;
    }

    public JobResult getResult() {
        return result;
    }

    public PrivateJobUpdateParams setResult(JobResult result) {
        this.result = result;
        return this;
    }

    public File getStdout() {
        return stdout;
    }

    public PrivateJobUpdateParams setStdout(File stdout) {
        this.stdout = stdout;
        return this;
    }

    public File getStderr() {
        return stderr;
    }

    public PrivateJobUpdateParams setStderr(File stderr) {
        this.stderr = stderr;
        return this;
    }

    public JobStudyParam getStudy() {
        return study;
    }

    public PrivateJobUpdateParams setStudy(JobStudyParam study) {
        this.study = study;
        return this;
    }

    @Override
    public PrivateJobUpdateParams setDescription(String description) {
        super.setDescription(description);
        return this;
    }

    @Override
    public PrivateJobUpdateParams setTags(List<String> tags) {
        super.setTags(tags);
        return this;
    }

    @Override
    public PrivateJobUpdateParams setVisited(Boolean visited) {
        super.setVisited(visited);
        return this;
    }

    @Override
    public PrivateJobUpdateParams setAttributes(Map<String, Object> attributes) {
        super.setAttributes(attributes);
        return this;
    }
}
