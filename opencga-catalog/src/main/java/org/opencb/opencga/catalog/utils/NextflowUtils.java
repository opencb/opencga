package org.opencb.opencga.catalog.utils;

import org.apache.commons.lang3.StringUtils;
import org.opencb.opencga.catalog.exceptions.CatalogException;
import org.opencb.opencga.catalog.exceptions.CatalogParameterException;
import org.opencb.opencga.core.common.IOUtils;
import org.opencb.opencga.core.common.TimeUtils;
import org.opencb.opencga.core.models.externalTool.*;
import org.opencb.opencga.core.models.job.MinimumRequirements;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.net.URL;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class NextflowUtils {

    private static final Pattern WORKFLOW_MEMORY_PATTERN = Pattern.compile("\\s*memory\\s*=\\s*\\{\\s*[^0-9]*([0-9]+\\.[A-Za-z]+)");
    private static final Pattern WORKFLOW_CPU_PATTERN = Pattern.compile("\\s*cpus\\s*=\\s*\\{\\s*[^0-9]*([0-9]+)");

    public static final int MAX_CPUS = 15;
    public static final String MAX_MEMORY = "64.GB";  // Format is important for Nextflow. It requires the dot symbol.

    private static final Logger LOGGER = LoggerFactory.getLogger(NextflowUtils.class);

    public static ExternalTool importRepository(WorkflowRepositoryParams repository) throws CatalogException {
        ParamUtils.checkObj(repository, "Nextflow repository parameters");
        if (StringUtils.isEmpty(repository.getName())) {
            throw new CatalogParameterException("Missing 'name' field in workflow import parameters");
        }
        String workflowId = repository.getName().replace("/", ".");
        WorkflowSystem workflowSystem = new WorkflowSystem(WorkflowSystem.SystemId.NEXTFLOW, "");
        ExternalTool externalTool = new ExternalTool("", "", "", ExternalToolType.WORKFLOW, null,
                new Workflow(workflowSystem, new LinkedList<>(), repository.toWorkflowRepository()), null, new LinkedList<>(),
                new LinkedList<>(), new MinimumRequirements(), false, new ExternalToolInternal(), TimeUtils.getTime(), TimeUtils.getTime(),
                new HashMap<>());

        try {
            processNextflowConfig(externalTool, repository);
            processMemoryRequirements(externalTool, repository);
        } catch (CatalogException e) {
            throw new CatalogException("Could not process repository information from workflow '" + workflowId + "'.", e);
        }

        return externalTool;
    }

    private static void processMemoryRequirements(ExternalTool externalTool, WorkflowRepositoryParams repository) throws CatalogException {
        String urlStr;

        if (StringUtils.isEmpty(repository.getTag())) {
            urlStr = "https://raw.githubusercontent.com/" + repository.getName() + "/refs/heads/master/conf/base.config";
        } else {
            urlStr = "https://raw.githubusercontent.com/" + repository.getName() + "/refs/tags/" + repository.getTag()
                    + "/conf/base.config";
        }

        try {
            URL url = new URL(urlStr);
            BufferedReader in = new BufferedReader(new InputStreamReader(url.openStream()));

            int cpus = 0;
            String memory = null;
            String inputLine;
            long maxMemory = IOUtils.fromHumanReadableToByte(MAX_MEMORY);
            while ((inputLine = in.readLine()) != null) {
                Matcher cpuMatcher = WORKFLOW_CPU_PATTERN.matcher(inputLine);
                Matcher memoryMatcher = WORKFLOW_MEMORY_PATTERN.matcher(inputLine);
                if (cpuMatcher.find()) {
                    String value = cpuMatcher.group(1);
                    int intValue = Integer.parseInt(value);
                    if (intValue > cpus) {
                        cpus = Math.min(intValue, MAX_CPUS);
                    }
                } else if (memoryMatcher.find()) {
                    String value = memoryMatcher.group(1);
                    if (memory == null) {
                        memory = value;
                    } else {
                        long memoryBytes = IOUtils.fromHumanReadableToByte(value);
                        long currentMemoryBytes = IOUtils.fromHumanReadableToByte(memory);
                        if (memoryBytes > currentMemoryBytes) {
                            if (memoryBytes > maxMemory) {
                                memory = MAX_MEMORY;
                            } else {
                                memory = value;
                            }
                        }
                    }
                }
            }
            if (cpus > 0 && memory != null) {
                externalTool.getMinimumRequirements().setCpu(String.valueOf(cpus));
                externalTool.getMinimumRequirements().setMemory(memory);
            } else {
                LOGGER.warn("Could not find the minimum requirements for the workflow " + externalTool.getId());
            }
            in.close();
        } catch (Exception e) {
            throw new CatalogException("Could not process nextflow.config file.", e);
        }
    }

    private static void processNextflowConfig(ExternalTool externalTool, WorkflowRepositoryParams repository) throws CatalogException {
        String urlStr;
        if (StringUtils.isEmpty(repository.getTag())) {
            urlStr = "https://raw.githubusercontent.com/" + repository.getName() + "/refs/heads/master/nextflow.config";
        } else {
            urlStr = "https://raw.githubusercontent.com/" + repository.getName() + "/refs/tags/" + repository.getTag()
                    + "/nextflow.config";
        }

        try {
            URL url = new URL(urlStr);
            BufferedReader in = new BufferedReader(new InputStreamReader(url.openStream()));

            // We add the bracket close strings because they are expected to be properly indented. That way, we will be able to know when
            // that section has been properly closed. Otherwise, we may get confused by some other subsections that could be closed before
            // the actual section closure.
            String manifestBracketClose = null;
            String profilesBracketClose = null;
            String gitpodBracketClose = null;
            String inputLine;
            while ((inputLine = in.readLine()) != null) {
                if (manifestBracketClose != null) {
                    if (manifestBracketClose.equals(inputLine)) {
                        manifestBracketClose = null;
                    } else {
                        // Process manifest line
                        fillWithWorkflowManifest(externalTool, inputLine);
                    }
                } else if (profilesBracketClose != null) {
                    if (gitpodBracketClose != null) {
                        if (gitpodBracketClose.equals(inputLine)) {
                            gitpodBracketClose = null;
                        } else {
                            // Process gitpod line
                            fillWithGitpodManifest(externalTool, inputLine);
                        }
                    } else if (inputLine.trim().startsWith("gitpod {")) {
                        int position = inputLine.indexOf("gitpod {");
                        gitpodBracketClose = StringUtils.repeat(" ", position) + "}";
                    } else if (profilesBracketClose.equals(inputLine)) {
                        profilesBracketClose = null;
                    }
                } else if (inputLine.trim().startsWith("manifest {")) {
                    int position = inputLine.indexOf("profiles {");
                    manifestBracketClose = StringUtils.repeat(" ", position) + "}";
                } else if (inputLine.trim().startsWith("profiles {")) {
                    int position = inputLine.indexOf("profiles {");
                    profilesBracketClose = StringUtils.repeat(" ", position) + "}";

                }
            }
            in.close();
        } catch (Exception e) {
            throw new CatalogException("Could not process nextflow.config file.", e);
        }
    }

    private static void fillWithWorkflowManifest(ExternalTool externalTool, String rawline) {
        String[] split = rawline.split("= ");
        if (split.length != 2) {
            return;
        }
        String key = split[0].trim();
//        String key = split[0].replaceAll(" ", "");
        String value = split[1].replace("\"", "").replace("'", "").trim();
        switch (key) {
            case "name":
                externalTool.setId(value.replace("/", "."));
                externalTool.setName(value.replace("/", " "));
                externalTool.getWorkflow().getRepository().setName(value);
                break;
            case "author":
                externalTool.getWorkflow().getRepository().setAuthor(value);
                break;
            case "description":
                externalTool.setDescription(value);
                externalTool.getWorkflow().getRepository().setDescription(value);
                break;
            case "version":
                String version = value.replaceAll("^[^0-9]+|[^0-9.]+$", "");
                externalTool.getWorkflow().getRepository().setTag(version);
                break;
            case "nextflowVersion":
                // Nextflow version must start with a number
                version = value.replaceAll("^[^0-9]+|[^0-9.]+$", "");
                externalTool.getWorkflow().getManager().setVersion(version);
                break;
            default:
                break;
        }
    }

    private static void fillWithGitpodManifest(ExternalTool externalTool, String rawline) {
        String[] split = rawline.split("=");
        if (split.length != 2) {
            return;
        }
        String key = split[0].replaceAll(" ", "");
        String value = split[1].replace("\"", "").replace("'", "").trim();
        switch (key) {
            case "executor.cpus":
                externalTool.getMinimumRequirements().setCpu(value);
                break;
            case "executor.memory":
                externalTool.getMinimumRequirements().setMemory(value);
                break;
            default:
                break;
        }
    }

}
