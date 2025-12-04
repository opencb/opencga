package org.opencb.opencga.analysis.workflow;

import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.opencb.opencga.analysis.tools.OpenCgaDockerToolScopeStudy;
import org.opencb.opencga.core.exceptions.ToolException;
import org.opencb.opencga.core.models.externalTool.ExternalToolVariable;

import java.util.*;

public abstract class ExternalToolDockerScopeStudy extends OpenCgaDockerToolScopeStudy {

    /**
     * Generate the command map to be executed inside the docker container substituting dynamic values such as:
     * $OUTPUT, $JOB_OUTPUT, file://, opencga://, etc.
     *
     * @param params    Map of parameters.
     * @param variables List of external tool variables.
     * @return Sanitised map of parameters.
     * @throws Exception In case of missing mandatory parameters.
     */
    protected Map<String, String> sanitiseParams(Map<String, String> params, List<ExternalToolVariable> variables) throws Exception {
        Set<String> mandatoryParams = new HashSet<>();
        Map<String, ExternalToolVariable> variableMap = new HashMap<>();
        if (CollectionUtils.isNotEmpty(variables)) {
            for (ExternalToolVariable variable : variables) {
                String variableId = removePrefix(variable.getId());
                variableMap.put(variableId, variable);
                if (variable.isRequired()) {
                    mandatoryParams.add(variableId);
                }
            }
        }

        Map<String, String> sanitisedParams = new HashMap<>();
        if (params != null) {
            for (Map.Entry<String, String> entry : params.entrySet()) {
                String variableId = removePrefix(entry.getKey());
                String value = entry.getValue();
                // Remove from the mandatoryParams set
                mandatoryParams.remove(variableId);

                ExternalToolVariable externalToolVariable = variableMap.get(variableId);
                String paramValue = null;

                if (StringUtils.isNotEmpty(value)) {
                    if ((externalToolVariable != null && externalToolVariable.isOutput()) || inputFileUtils.isDynamicOutputFolder(value)) {
                        paramValue = processOutputValue(value, inputFileUtils);
                    } else if (!inputFileUtils.isFlag(value)) {
                        paramValue = processInputValue(value, inputFileUtils);
                    }
                } else if (externalToolVariable != null) {
                    if (StringUtils.isNotEmpty(externalToolVariable.getDefaultValue())) {
                        paramValue = externalToolVariable.getDefaultValue();
                    } else if (externalToolVariable.isOutput()) {
                        paramValue = processOutputValue("", inputFileUtils);
                    } else if (externalToolVariable.isRequired() && externalToolVariable.getType() != ExternalToolVariable.ExternalToolVariableType.FLAG) {
                        throw new ToolException("Missing value for mandatory parameter: '" + variableId + "'.");
                    }
                }
                sanitisedParams.put(entry.getKey(), paramValue);
            }
        }

        for (String mandatoryParam : mandatoryParams) {
            logger.info("Processing missing mandatory param: '{}'", mandatoryParam);
            ExternalToolVariable externalToolVariable = variableMap.get(mandatoryParam);

            String paramValue = null;
            if (StringUtils.isNotEmpty(externalToolVariable.getDefaultValue())) {
                if (externalToolVariable.isOutput()) {
                    paramValue = processOutputValue(externalToolVariable.getDefaultValue(), inputFileUtils);
                } else {
                    paramValue = processInputValue(externalToolVariable.getDefaultValue(), inputFileUtils);
                }
            } else if (externalToolVariable.isOutput()) {
                paramValue = processOutputValue("", inputFileUtils);
            } else if (externalToolVariable.getType() != ExternalToolVariable.ExternalToolVariableType.FLAG) {
                throw new ToolException("Missing mandatory parameter: '" + mandatoryParam + "'.");
            }
            sanitisedParams.put(externalToolVariable.getId(), paramValue);
        }

        return sanitisedParams;
    }

    /**
     * Build the command line to be executed inside the docker container.
     *
     * @param params    List of parameters already sanitised.
     * @return          Command line string.
     */
    protected String buildCommandLine(Map<String, String> params) {
        StringBuilder cliParamsBuilder = new StringBuilder();
        if (params != null) {
            for (Map.Entry<String, String> entry : params.entrySet()) {
                if (!entry.getKey().startsWith("-")) {
                    if (entry.getKey().length() == 1) {
                        cliParamsBuilder.append("-"); // Single dash for single character parameters
                    } else {
                        cliParamsBuilder.append("--");
                    }
                }
                cliParamsBuilder.append(entry.getKey()).append(" ");
                if (StringUtils.isNotEmpty(entry.getValue())) {
                    cliParamsBuilder.append(entry.getValue()).append(" ");
                }
            }
        }
        return cliParamsBuilder.toString();
    }

}
