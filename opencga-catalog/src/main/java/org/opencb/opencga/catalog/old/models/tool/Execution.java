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

package org.opencb.opencga.catalog.old.models.tool;

import java.util.List;

public class Execution {

    private String id, name, executable, outputParam, testCmd, result;
    private List<InputParam> inputParams;
    private List<InputParam> inputParamsFromTxt;
    private List<Option> validParams;
    private List<ConfigAttr> configAttr;


    public Execution() {

    }

    public Execution(String id, String name, String executable,
                     List<InputParam> inputParams, List<InputParam> inputParamsFromTxt, String outputParam,
                     List<Option> validParams, List<ConfigAttr> configAttr, String testCmd, String result) {
        this.id = id;
        this.name = name;
        this.executable = executable;
        this.inputParams = inputParams;
        this.inputParamsFromTxt = inputParamsFromTxt;
        this.outputParam = outputParam;
        this.validParams = validParams;
        this.configAttr = configAttr;
        this.testCmd = testCmd;
        this.result = result;
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("Execution{");
        sb.append("id='").append(id).append('\'');
        sb.append(", name='").append(name).append('\'');
        sb.append(", executable='").append(executable).append('\'');
        sb.append(", outputParam='").append(outputParam).append('\'');
        sb.append(", testCmd='").append(testCmd).append('\'');
        sb.append(", result='").append(result).append('\'');
        sb.append(", inputParams=").append(inputParams);
        sb.append(", inputParamsFromTxt=").append(inputParamsFromTxt);
        sb.append(", validParams=").append(validParams);
        sb.append(", configAttr=").append(configAttr);
        sb.append('}');
        return sb.toString();
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

    public String getOutputParam() {
        return outputParam;
    }

    public void setOutputParam(String outputParam) {
        this.outputParam = outputParam;
    }

    public List<InputParam> getInputParams() {
        return inputParams;
    }

    public void setInputParams(List<InputParam> inputParams) {
        this.inputParams = inputParams;
    }

    public List<Option> getValidParams() {
        return validParams;
    }

    public void setValidParams(List<Option> validParams) {
        this.validParams = validParams;
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

    public String getResult() {
        return result;
    }

    public void setResult(String result) {
        this.result = result;
    }
}
