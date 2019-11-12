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

package org.opencb.opencga.app.cli.main.io;

import org.apache.commons.lang3.StringUtils;
import org.opencb.commons.datastore.core.DataResponse;
import org.opencb.commons.datastore.core.DataResult;
import org.opencb.opencga.core.common.TimeUtils;
import org.opencb.opencga.core.models.*;
import org.opencb.opencga.core.models.acls.permissions.AbstractAclEntry;

import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static org.opencb.opencga.core.common.IOUtils.humanReadableByteCount;

/**
 * Created by pfurio on 28/11/16.
 */
public class TextOutputWriter extends AbstractOutputWriter {

    public TextOutputWriter() {
    }

    public TextOutputWriter(WriterConfiguration writerConfiguration) {
        super(writerConfiguration);
    }

    @Override
    public void print(DataResponse queryResponse) {
        if (checkErrors(queryResponse)) {
            return;
        }

        if (queryResponse.getResponses().size() == 0 || ((DataResult) queryResponse.getResponses().get(0)).getNumResults() == 0) {
            if (queryResponse.first().getNumMatches() > 0) {
                // count
                ps.println(queryResponse.first().getNumMatches());
            } else {
                ps.println("No results found for the query.");
            }
            return;
        }

        ps.print(printMetadata(queryResponse));

        List<DataResult> queryResultList = queryResponse.getResponses();
        String[] split = queryResultList.get(0).getResultType().split("\\.");
        String clazz = split[split.length - 1];

        switch (clazz) {
            case "User":
                printUser(queryResponse.getResponses());
                break;
            case "Project":
                printProject(queryResponse.getResponses());
                break;
            case "Study":
                printStudy(queryResponse.getResponses());
                break;
            case "File":
                printFiles(queryResponse.getResponses());
                break;
            case "Sample":
                printSamples(queryResponse.getResponses());
                break;
            case "Cohort":
                printCohorts(queryResponse.getResponses());
                break;
            case "Individual":
                printIndividual(queryResponse.getResponses());
                break;
//            case "Family":
//                printFamily(queryResponse.getResponses());
//                break;
            case "Job":
                printJob(queryResponse.getResponses());
                break;
            case "VariableSet":
                printVariableSet(queryResponse.getResponses());
                break;
            case "AnnotationSet":
                printAnnotationSet(queryResponse.getResponses());
                break;
            case "FileTree":
                printTreeFile(queryResponse);
                break;
            default:
                System.err.println(ANSI_YELLOW + "Warning: " + clazz + " results not yet supported in text format. Using YAML format"
                        + ANSI_RESET);
                YamlOutputWriter yamlOutputWriter = new YamlOutputWriter(writerConfiguration);
                yamlOutputWriter.print(queryResponse);
                break;
        }

    }

    private String printMetadata(DataResponse queryResponse) {
        StringBuilder sb = new StringBuilder();
        if (writerConfiguration.isMetadata()) {
            int numResults = 0;
//            int totalResults = 0;
            int time = 0;

            List<DataResult> queryResultList = queryResponse.getResponses();
            for (DataResult queryResult : queryResultList) {
                numResults += queryResult.getNumResults();
//                totalResults += queryResult.getNumMatches();
                time += queryResult.getTime();
            }

            sb.append("## Date: ").append(TimeUtils.getTime()).append("\n")
                    .append("## Number of results: ").append(numResults)
                    .append(". Time: ").append(time).append(" ms\n");

            // TODO: Add query info
            sb.append("## Query: { ")
                    .append(queryResponse.getParams()
                            .entrySet().stream().map(entry -> entry.getKey() + ": " + entry.getValue()).collect(Collectors.joining(", ")))
                    .append(" }\n");
        }
        return sb.toString();
    }

    private void printUser(List<DataResult<User>> queryResultList) {
        StringBuilder sb = new StringBuilder();
        for (DataResult<User> queryResult : queryResultList) {
            // Write header
            if (writerConfiguration.isHeader()) {
                sb.append("#(U)ID\tNAME\tE-MAIL\tORGANIZATION\tACCOUNT_TYPE\tSIZE\tQUOTA\n");
                sb.append("#(P)\tID\tNAME\tORGANIZATION\tDESCRIPTION\tSIZE\n");
                sb.append("#(S)\t\tID\tNAME\tTYPE\tDESCRIPTION\t#GROUPS\tSIZE\n");
            }

            for (User user : queryResult.getResults()) {
                sb.append(String.format("%s%s\t%s\t%s\t%s\t%s\t%d\t%d\n", "",
                        StringUtils.defaultIfEmpty(user.getId(), "-"), StringUtils.defaultIfEmpty(user.getName(), "-"),
                        StringUtils.defaultIfEmpty(user.getEmail(), "-"), StringUtils.defaultIfEmpty(user.getOrganization(), "-"),
                        StringUtils.defaultIfEmpty(user.getAccount() != null ? user.getAccount().getType().name() : "-", "-"),
                        user.getSize(), user.getQuota()));

                if (user.getProjects().size() > 0) {
                    for (Project project : user.getProjects()) {
                        sb.append(String.format("%s%s\t%s\t%s\t%s\t%d\n", " * ",
                                StringUtils.defaultIfEmpty(project.getId(), "-"), StringUtils.defaultIfEmpty(project.getName(), "-"),
                                StringUtils.defaultIfEmpty(project.getOrganization(), "-"),
                                StringUtils.defaultIfEmpty(project.getDescription(), "-"), project.getSize()));

                        if (project.getStudies().size() > 0) {
                            for (Study study : project.getStudies()) {
                                sb.append(String.format("    - %s\t%s\t%s\t%s\t%s\t%d\n",
                                        StringUtils.defaultIfEmpty(study.getId(), "-"), StringUtils.defaultIfEmpty(study.getName(), "-"),
                                        study.getType(), StringUtils.defaultIfEmpty(study.getDescription(), "-"),
                                        study.getGroups() == null ? ""
                                                : study.getGroups().stream().map(Group::getId).collect(Collectors.joining(",")),
                                        study.getSize()));

                                if (study.getGroups() != null && study.getGroups().size() > 0) {
                                    sb.append("       Groups:\n");
                                    for (Group group : study.getGroups()) {
                                        printGroup(group, sb, "        + ");
                                    }
                                }
                            }
                        }
                    }
                }
            }
        }

        ps.println(sb.toString());
    }

    private void printProject(List<DataResult<Project>> queryResultList) {
        StringBuilder sb = new StringBuilder();
        for (DataResult<Project> queryResult : queryResultList) {
            // Write header
            sb.append("#ID\tNAME\tORGANIZATION\tORGANISM\tASSEMBLY\tDESCRIPTION\tSIZE\t#STUDIES\tSTATUS\n");

            for (Project project : queryResult.getResults()) {
                String organism = "NA";
                String assembly = "NA";
                if (project.getOrganism() != null) {
                    organism = StringUtils.isNotEmpty(project.getOrganism().getScientificName())
                            ? project.getOrganism().getScientificName()
                            : (StringUtils.isNotEmpty(project.getOrganism().getCommonName())
                            ? project.getOrganism().getCommonName() : "NA");
                    if (StringUtils.isNotEmpty(project.getOrganism().getAssembly())) {
                        assembly = project.getOrganism().getAssembly();
                    }
                }

                sb.append(String.format("%s\t%s\t%s\t%s\t%s\t%s\t%d\t%d\t%s\n", StringUtils.defaultIfEmpty(project.getId(), "-"),
                        StringUtils.defaultIfEmpty(project.getName(), ","), StringUtils.defaultIfEmpty(project.getOrganization(), "-"),
                        organism, assembly, StringUtils.defaultIfEmpty(project.getDescription(), "-"), project.getSize(),
                        project.getStudies() != null ? project.getStudies().size() : -1,
                        project.getStatus() != null ? StringUtils.defaultIfEmpty(project.getStatus().getName(), "-") : "-"));
            }
        }
        ps.println(sb.toString());
    }

    private void printStudy(List<DataResult<Study>> queryResultList) {
        StringBuilder sb = new StringBuilder();
        for (DataResult<Study> queryResult : queryResultList) {
            // Write header
            sb.append("#ID\tNAME\tTYPE\tDESCRIPTION\t#GROUPS\tSIZE\t#FILES\t#SAMPLES\t#COHORTS\t#INDIVIDUALS\t#JOBS\t")
                    .append("#VARIABLE_SETS\tSTATUS\n");

            for (Study study : queryResult.getResults()) {
                sb.append(String.format("%s\t%s\t%s\t%s\t%d\t%d\t%s\t%d\t%d\t%d\t%d\t%d\t%s\n",
                        StringUtils.defaultIfEmpty(study.getId(), "-"), StringUtils.defaultIfEmpty(study.getName(), "-"),
                        study.getType(), StringUtils.defaultIfEmpty(study.getDescription(), "-"),
                        study.getGroups() != null ? study.getGroups().size() : -1, study.getSize(),
                        study.getFiles() != null ? study.getFiles().size() : -1,
                        study.getSamples() != null ? study.getSamples().size() : -1,
                        study.getCohorts() != null ? study.getCohorts().size() : -1,
                        study.getIndividuals() != null ? study.getIndividuals().size() : -1,
                        study.getJobs() != null ? study.getJobs().size() : -1,
                        study.getVariableSets() != null ? study.getVariableSets().size() : -1,
                        study.getStatus() != null ? StringUtils.defaultIfEmpty(study.getStatus().getName(), "-") : "-"));
            }
        }

        ps.println(sb.toString());
    }

    private void printGroup(Group group, StringBuilder sb, String prefix) {
        sb.append(String.format("%s%s\t%s\n", prefix, group.getId(), StringUtils.join(group.getUserIds(), ", ")));
    }

    private void printACL(AbstractAclEntry aclEntry, StringBuilder sb, String prefix) {
        sb.append(String.format("%s%s\t%s\n", prefix, aclEntry.getMember(), aclEntry.getPermissions().toString()));
    }

    private void printFiles(List<DataResult<File>> queryResultList) {
        StringBuilder sb = new StringBuilder();
        for (DataResult<File> queryResult : queryResultList) {
            // Write header
            sb.append("#ID\tNAME\tTYPE\tFORMAT\tBIOFORMAT\tDESCRIPTION\tCATALOG_PATH\tFILE_SYSTEM_URI\tSTATUS\tSIZE\tINDEX_STATUS"
                    + "\tRELATED_FILES\tSAMPLES\n");

            printFiles(queryResult.getResults(), sb, "");
        }

        ps.println(sb.toString());
    }

    private void printFiles(List<File> files, StringBuilder sb, String format) {
        // # name	type	format	bioformat	description	path	id	status	size	index status	related files   samples
        for (File file : files) {
            String indexStatus = "NA";
            if (file.getIndex() != null && file.getIndex().getStatus() != null && file.getIndex().getStatus().getName() != null) {
                indexStatus = file.getIndex().getStatus().getName();
            }
            sb.append(String.format("%s%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%d\t%s\t%s\t%s\n", format,
                    StringUtils.defaultIfEmpty(file.getId(), "-"), StringUtils.defaultIfEmpty(file.getName(), "-"),
                    file.getType(), file.getFormat(), file.getBioformat(), StringUtils.defaultIfEmpty(file.getDescription(), "-"),
                    StringUtils.defaultIfEmpty(file.getPath(), "-"), file.getUri(),
                    file.getStatus() != null ? StringUtils.defaultIfEmpty(file.getStatus().getName(), "-") : "-", file.getSize(),
                    indexStatus,
                    file.getRelatedFiles() != null ?
                            StringUtils.join(file.getRelatedFiles()
                                    .stream()
                                    .map(File.RelatedFile::getFile)
                                    .map(File::getId)
                                    .collect(Collectors.toList()), ", ")
                            : "-",
                    file.getSamples() != null ?
                            StringUtils.join(file.getSamples()
                                    .stream()
                                    .map(Sample::getId)
                                    .collect(Collectors.toList()), ", ")
                            : "-")
            );
        }
    }

    private void printSamples(List<DataResult<Sample>> queryResultList) {
        StringBuilder sb = new StringBuilder();
        for (DataResult<Sample> queryResult : queryResultList) {
            // Write header
            if (writerConfiguration.isHeader()) {
                sb.append("#ID\tNAME\tSOURCE\tDESCRIPTION\tSTATUS\tINDIVIDUAL_ID\tINDIVIDUAL_NAME\n");
            }

            printSamples(queryResult.getResults(), sb, "");
        }

        ps.println(sb.toString());
    }

    private void printSamples(List<Sample> samples, StringBuilder sb, String format) {
        // # name	id	source	description	status	individualName	individualID
        for (Sample sample : samples) {
            String individualId = StringUtils.defaultIfEmpty(sample.getIndividualId(), "-");
            sb.append(String.format("%s%s\t%s\t%s\t%s\t%s\t%s\n", format, StringUtils.defaultIfEmpty(sample.getId(), "-"),
                    StringUtils.defaultIfEmpty(sample.getId(), "-"), StringUtils.defaultIfEmpty(sample.getSource(), "-"),
                    StringUtils.defaultIfEmpty(sample.getDescription(), "-"),
                    sample.getStatus() != null ? StringUtils.defaultIfEmpty(sample.getStatus().getName(), "-") : "-", individualId));
        }
    }

    private void printCohorts(List<DataResult<Cohort>> queryResultList) {
        StringBuilder sb = new StringBuilder();
        for (DataResult<Cohort> queryResult : queryResultList) {
            // Write header
            if (writerConfiguration.isHeader()) {
                sb.append("#ID\tNAME\tTYPE\tDESCRIPTION\tSTATUS\tTOTAL_SAMPLES\tSAMPLES\tFAMILY\n");
            }

            for (Cohort cohort : queryResult.getResults()) {
                sb.append(String.format("%s\t%s\t%s\t%s\t%s\t%d\t%s\t%s\n", StringUtils.defaultIfEmpty(cohort.getId(), "-"),
                        StringUtils.defaultIfEmpty(cohort.getId(), "-"), cohort.getType(),
                        StringUtils.defaultIfEmpty(cohort.getDescription(), "-"),
                        cohort.getStatus() != null ? StringUtils.defaultIfEmpty(cohort.getStatus().getName(), "-") : "-",
                        cohort.getSamples().size(), cohort.getSamples().size() > 0 ? StringUtils.join(cohort.getSamples(), ", ") : "NA",
                        cohort.getFamily() != null ? StringUtils.defaultIfEmpty(cohort.getFamily().getId(), "-") : "-"));
            }
        }

        ps.println(sb.toString());
    }

    private void printIndividual(List<DataResult<Individual>> queryResultList) {
        StringBuilder sb = new StringBuilder();
        for (DataResult<Individual> queryResult : queryResultList) {
            // Write header
            if (writerConfiguration.isHeader()) {
                sb.append("#ID\tNAME\tAFFECTATION_STATUS\tSEX\tKARYOTYPIC_SEX\tETHNICITY\tPOPULATION\tSUBPOPULATION\tLIFE_STATUS")
                        .append("\tSTATUS\tFATHER_ID\tMOTHER_ID\tCREATION_DATE\n");
            }

            for (Individual individual : queryResult.getResults()) {
                String population = "NA";
                String subpopulation = "NA";
                if (individual.getPopulation() != null) {
                    if (StringUtils.isNotEmpty(individual.getPopulation().getName())) {
                        population = individual.getPopulation().getName();
                    }
                    if (StringUtils.isNotEmpty(individual.getPopulation().getSubpopulation())) {
                        subpopulation = individual.getPopulation().getSubpopulation();
                    }
                }
                sb.append(String.format("%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\n",
                        StringUtils.defaultIfEmpty(individual.getId(), "-"), StringUtils.defaultIfEmpty(individual.getName(), "-"),
                        individual.getAffectationStatus(), individual.getSex(), individual.getKaryotypicSex(),
                        StringUtils.defaultIfEmpty(individual.getEthnicity(), "-"), population, subpopulation,
                        individual.getLifeStatus(),
                        individual.getStatus() != null ? StringUtils.defaultIfEmpty(individual.getStatus().getName(), "-") : "-",
                        individual.getFather() != null ? StringUtils.defaultIfEmpty(individual.getFather().getId(), "-") : "-",
                        individual.getMother() != null ? StringUtils.defaultIfEmpty(individual.getMother().getId(), "-") : "-",
                        StringUtils.defaultIfEmpty(individual.getCreationDate(), "-")));
            }
        }

        ps.println(sb.toString());
    }

//    private void printFamily(List<DataResult<Family>> queryResultList) {
//        StringBuilder sb = new StringBuilder();
//        for (DataResult<Family> queryResult : queryResultList) {
//            // Write header
//            if (writerConfiguration.isHeader()) {
//                sb.append("#NAME\tID\tMOTHER\tFATHER\tMEMBER\tSTATUS\tCREATION_DATE\n");
//            }
//
//            for (Family family : queryResult.getResults()) {
//                String mother = (family.getMother() != null && StringUtils.isNotEmpty(family.getMother().getName()))
//                        ? family.getMother().getName() + "(" + family.getMother().getId() + ")"
//                        : "NA";
//                String father = (family.getFather() != null && StringUtils.isNotEmpty(family.getFather().getName()))
//                        ? family.getFather().getName() + "(" + family.getFather().getId() + ")"
//                        : "NA";
//                String children = family.getChildren() != null
//                        ? StringUtils.join(
//                                family.getChildren().stream()
//                                    .filter(Objects::nonNull)
//                                    .filter(individual -> StringUtils.isNotEmpty(individual.getName()))
//                                    .map(individual -> individual.getName() + "(" + individual.getId() + ")")
//                                    .collect(Collectors.toList()), ", ")
//                        : "NA";
//                sb.append(String.format("%s\t%d\t%s\t%s\t%s\t%s\t%s\n",
//                        family.getName(), family.getId(), mother, father, children,
//                        family.getStatus().getName(), family.getCreationDate()));
//            }
//        }
//
//        ps.println(sb.toString());
//    }

    private void printJob(List<DataResult<Job>> queryResultList) {
        StringBuilder sb = new StringBuilder();
        for (DataResult<Job> queryResult : queryResultList) {
            // Write header
            if (writerConfiguration.isHeader()) {
                sb.append("#ID\tNAME\tCREATION_DATE\tSTATUS\tINPUT")
                        .append("\tOUTPUT\tOUTPUT_DIRECTORY\n");
            }

            for (Job job : queryResult.getResults()) {
                sb.append(String.format("%s\t%s\t%s\t%s\t%s\t%s\t%s\n",
                        StringUtils.defaultIfEmpty(job.getId(), "-"), StringUtils.defaultIfEmpty(job.getName(), "-"),
                        StringUtils.defaultIfEmpty(job.getCreationDate(), "-"),
                        job.getStatus() != null ? StringUtils.defaultIfEmpty(job.getStatus().getName(), "-") : "-",
                        StringUtils.join(job.getInput(), ", "),
                        StringUtils.join(job.getOutput(), ", "),
                        job.getOutDir() != null ? StringUtils.defaultIfEmpty(job.getOutDir().getId(), "-") : "-"));
            }
        }

        ps.println(sb.toString());
    }

    private void printVariableSet(List<DataResult<VariableSet>> queryResultList) {
        StringBuilder sb = new StringBuilder();
        for (DataResult<VariableSet> queryResult : queryResultList) {
            // Write header
            if (writerConfiguration.isHeader()) {
                sb.append("#ID\tNAME\tDESCRIPTION\tVARIABLES\n");
            }

            for (VariableSet variableSet : queryResult.getResults()) {
                sb.append(String.format("%s\t%s\t%s\t%s\n", StringUtils.defaultIfEmpty(variableSet.getId(), "-"),
                        StringUtils.defaultIfEmpty(variableSet.getName(), "-"),
                        StringUtils.defaultIfEmpty(variableSet.getDescription(), "-"),
                        variableSet.getVariables().stream().map(Variable::getId).collect(Collectors.joining(", "))));
            }
        }

        ps.println(sb.toString());
    }

    private void printAnnotationSet(List<DataResult<AnnotationSet>> queryResultList) {
        StringBuilder sb = new StringBuilder();
        for (DataResult<AnnotationSet> queryResult : queryResultList) {
            for (AnnotationSet annotationSet : queryResult.getResults()) {
                // Write header
                if (writerConfiguration.isHeader()) {
                    sb.append("#KEY\tVALUE\n");
                }

                for (Map.Entry<String, Object> annotation : annotationSet.getAnnotations().entrySet()) {
                    sb.append(String.format("%s\t%s\n", annotation.getKey(), annotation.getValue()));
                }
            }
        }

        ps.println(sb.toString());
    }

    private void printTreeFile(DataResponse<FileTree> queryResponse) {
        StringBuilder sb = new StringBuilder();
        for (DataResult<FileTree> fileTreeQueryResult : queryResponse.getResponses()) {
            printRecursiveTree(fileTreeQueryResult.getResults(), sb, "");
        }
        ps.println(sb.toString());
    }

    private void printRecursiveTree(List<FileTree> fileTreeList, StringBuilder sb, String indent) {
        if (fileTreeList == null || fileTreeList.size() == 0) {
            return;
        }

        for (Iterator<FileTree> iterator = fileTreeList.iterator(); iterator.hasNext(); ) {
            FileTree fileTree = iterator.next();
            File file = fileTree.getFile();

            sb.append(String.format("%s %s  [%s, %s, %s]\n",
                    indent.isEmpty() ? "" : indent + (iterator.hasNext() ? "├──" : "└──"),
                    file.getType() == File.Type.FILE ? file.getName() : file.getName() + "/",
                    file.getName(),
                    file.getStatus() != null ? file.getStatus().getName() : "",
                    humanReadableByteCount(file.getSize(), false)));

            if (file.getType() == File.Type.DIRECTORY) {
                printRecursiveTree(fileTree.getChildren(), sb, indent + (iterator.hasNext()? "│   " : "    "));
            }
        }
    }

}
