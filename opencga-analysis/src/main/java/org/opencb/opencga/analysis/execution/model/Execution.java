/*
 * Copyright 2015 OpenCB
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

package org.opencb.opencga.analysis.execution.model;

import java.util.List;

public class Execution {

    private String id;
    private String name;
    private String executable;

    private List<String> input;
    private String output;

    private List<Option> parameters;

    private Display display;
    private String result;

    // TODO check this parameter
    private String testCmd;

    private List<InputParam> inputParamsFromTxt;
    private List<ConfigAttr> configAttr;

    public Execution() {
    }

    public Execution(String id, String name, String executable, List<String> input,
                     List<InputParam> inputParamsFromTxt, String output, List<Option> parameters,
                     List<ConfigAttr> configAttr, String testCmd, String result) {
        this.id = id;
        this.name = name;
        this.executable = executable;
        this.input = input;
        this.inputParamsFromTxt = inputParamsFromTxt;
        this.output = output;
        this.parameters = parameters;
        this.configAttr = configAttr;
        this.testCmd = testCmd;
        this.result = result;
    }

    @Override
    public String toString() {
        return "Execution{" +
                "id='" + id + '\'' +
                ", name='" + name + '\'' +
                ", executable='" + executable + '\'' +
                ", output='" + output + '\'' +
                ", testCmd='" + testCmd + '\'' +
                ", result='" + result + '\'' +
                ", input=" + input +
                ", inputParamsFromTxt=" + inputParamsFromTxt +
                ", parameters=" + parameters +
                ", configAttr=" + configAttr +
                '}';
    }

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getExecutable() {
        return executable;
    }

    public void setExecutable(String executable) {
        this.executable = executable;
    }

    public String getOutput() {
        return output;
    }

    public void setOutput(String output) {
        this.output = output;
    }

    public List<String> getInput() {
        return input;
    }

    public void setInput(List<String> input) {
        this.input = input;
    }

    public List<Option> getParameters() {
        return parameters;
    }

    public void setParameters(List<Option> parameters) {
        this.parameters = parameters;
    }

    public List<ConfigAttr> getConfigAttr() {
        return configAttr;
    }

    public void setConfigAttr(List<ConfigAttr> configAttr) {
        this.configAttr = configAttr;
    }

    public String getTestCmd() {
        return testCmd;
    }

    public void setTestCmd(String testCmd) {
        this.testCmd = testCmd;
    }

    public List<InputParam> getInputParamsFromTxt() {
        return inputParamsFromTxt;
    }

    public void setInputParamsFromTxt(List<InputParam> inputParamsFromTxt) {
        this.inputParamsFromTxt = inputParamsFromTxt;
    }

    public Display getDisplay() {
        return display;
    }

    public void setDisplay(Display display) {
        this.display = display;
    }

    public String getResult() {
        return result;
    }

    public void setResult(String result) {
        this.result = result;
    }

}
