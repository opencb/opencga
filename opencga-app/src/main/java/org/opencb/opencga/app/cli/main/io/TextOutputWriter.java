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
import org.opencb.commons.datastore.core.DataResult;
import org.opencb.commons.datastore.core.Event;
import org.opencb.opencga.core.common.TimeUtils;
import org.opencb.opencga.core.models.AbstractAclEntry;
import org.opencb.opencga.core.models.cohort.Cohort;
import org.opencb.opencga.core.models.common.Annotable;
import org.opencb.opencga.core.models.common.AnnotationSet;
import org.opencb.opencga.core.models.common.Enums;
import org.opencb.opencga.core.models.file.File;
import org.opencb.opencga.core.models.file.FileTree;
import org.opencb.opencga.core.models.individual.Individual;
import org.opencb.opencga.core.models.job.Job;
import org.opencb.opencga.core.models.project.Project;
import org.opencb.opencga.core.models.sample.Sample;
import org.opencb.opencga.core.models.study.Group;
import org.opencb.opencga.core.models.study.Study;
import org.opencb.opencga.core.models.study.Variable;
import org.opencb.opencga.core.models.study.VariableSet;
import org.opencb.opencga.core.models.user.User;
import org.opencb.opencga.core.response.RestResponse;
import org.opencb.opencga.core.tools.result.ToolStep;

import java.text.SimpleDateFormat;
import java.time.Instant;
import java.util.*;
import java.util.stream.Collectors;

import static org.opencb.opencga.core.common.IOUtils.humanReadableByteCount;
import static org.opencb.opencga.core.models.common.Enums.ExecutionStatus.DONE;
import static org.opencb.opencga.core.models.common.Enums.ExecutionStatus.RUNNING;

/**
 * Created by pfurio on 28/11/16.
 */
public class TextOutputWriter extends AbstractOutputWriter {

    public static final String SIMPLE_DATE_PATTERN = "yyyy-MM-dd HH:mm:ss";
    public static final SimpleDateFormat SIMPLE_DATE_FORMAT = new SimpleDateFormat(SIMPLE_DATE_PATTERN);

    private Table.PrinterType tableType;

    public TextOutputWriter() {
    }

    public TextOutputWriter(WriterConfiguration writerConfiguration) {
        this(writerConfiguration, Table.PrinterType.TSV);
    }

    public TextOutputWriter(WriterConfiguration writerConfiguration, Table.PrinterType tableType) {
        super(writerConfiguration);
        this.tableType = tableType;
    }

    @Override
    public void print(RestResponse queryResponse) {
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

    private String printMetadata(RestResponse queryResponse) {
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
                sb.append("#(P)\tID\tNAME\tDESCRIPTION\n");
                sb.append("#(S)\t\tID\tNAME\tDESCRIPTION\t#GROUPS\tSIZE\n");
            }

            for (User user : queryResult.getResults()) {
                sb.append(String.format("%s%s\t%s\t%s\t%s\t%s\t%d\n", "",
                        StringUtils.defaultIfEmpty(user.getId(), "-"), StringUtils.defaultIfEmpty(user.getName(), "-"),
                        StringUtils.defaultIfEmpty(user.getEmail(), "-"), StringUtils.defaultIfEmpty(user.getOrganization(), "-"),
                        StringUtils.defaultIfEmpty(user.getAccount() != null ? user.getAccount().getType().name() : "-", "-"),
                        user.getQuota().getMaxDisk()));

                if (user.getProjects().size() > 0) {
                    for (Project project : user.getProjects()) {
                        sb.append(String.format("%s%s\t%s\t%s\n", " * ",
                                StringUtils.defaultIfEmpty(project.getId(), "-"), StringUtils.defaultIfEmpty(project.getName(), "-"),
                                StringUtils.defaultIfEmpty(project.getDescription(), "-")));

                        if (project.getStudies().size() > 0) {
                            for (Study study : project.getStudies()) {
                                sb.append(String.format("    - %s\t%s\t%s\t%s\t%d\n",
                                        StringUtils.defaultIfEmpty(study.getId(), "-"), StringUtils.defaultIfEmpty(study.getName(), "-"),
                                        StringUtils.defaultIfEmpty(study.getDescription(), "-"),
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
        new Table<Project>(tableType)
                .addColumn("ID", Project::getId)
                .addColumn("NAME", Project::getName)
                .addColumn("ORGANISM", p -> StringUtils.defaultIfEmpty(p.getOrganism().getScientificName(), p.getOrganism().getCommonName()), "NA")
                .addColumn("ASSEMBLY", p -> p.getOrganism().getAssembly(), "NA")
                .addColumn("DESCRIPTION", Project::getDescription)
                .addColumnNumber("#STUDIES", p -> p.getStudies().size())
                .addColumn("STATUS", p -> p.getInternal().getStatus().getName())
                .printTable(unwind(queryResultList));
    }

    private void printStudy(List<DataResult<Study>> queryResultList) {
        Table<Study> table = new Table<Study>(tableType)
                .addColumn("ID", Study::getId)
                .addColumn("NAME", Study::getName)
                .addColumn("DESCRIPTION", Study::getDescription)
                .addColumnNumber("#GROUPS", s -> s.getGroups().size())
                .addColumnNumber("SIZE", Study::getSize)
                .addColumnNumber("#FILES", s -> s.getFiles().size())
                .addColumnNumber("#SAMPLES", s -> s.getSamples().size())
                .addColumnNumber("#COHORTS", s -> s.getCohorts().size())
                .addColumnNumber("#INDIVIDUALS", s -> s.getIndividuals().size())
                .addColumnNumber("#JOBS", s -> s.getJobs().size())
                .addColumnNumber("#VARIABLE_SETS", s -> s.getVariableSets().size())
                .addColumn("STATUS", s -> s.getInternal().getStatus().getName());

        table.printTable(queryResultList.stream().flatMap(r -> r.getResults().stream()).collect(Collectors.toList()));
    }

    private void printGroup(Group group, StringBuilder sb, String prefix) {
        sb.append(String.format("%s%s\t%s\n", prefix, group.getId(), StringUtils.join(group.getUserIds(), ", ")));
    }

    private void printACL(AbstractAclEntry aclEntry, StringBuilder sb, String prefix) {
        sb.append(String.format("%s%s\t%s\n", prefix, aclEntry.getMember(), aclEntry.getPermissions().toString()));
    }

    private void printFiles(List<DataResult<File>> queryResultList) {
        Table<File> table = new Table<File>(tableType)
                .addColumn("ID", File::getId, 50)
                .addColumn("NAME", File::getName, 50)
                .addColumnEnum("TYPE", File::getType)
                .addColumnEnum("FORMAT", File::getFormat)
                .addColumnEnum("BIOFORMAT", File::getBioformat)
                .addColumn("DESCRIPTION", File::getDescription)
                .addColumn("CATALOG_PATH", File::getPath)
                .addColumn("FILE_SYSTEM_URI", file -> file.getUri().toString())
                .addColumn("STATUS", f -> f.getInternal().getStatus().getName())
                .addColumnNumber("SIZE", File::getSize)
                .addColumn("INDEX_STATUS", f -> f.getInternal().getIndex().getStatus().getName(), "NA")
                .addColumn("RELATED_FILES", f -> f.getRelatedFiles().stream().map(rf -> rf.getFile().getName()).collect(Collectors.joining(",")))
                .addColumn("SAMPLES", f -> StringUtils.join(f.getSampleIds(), ","));

        table.printTable(unwind(queryResultList));
    }

    private void printSamples(List<DataResult<Sample>> queryResultList) {
        Table<Sample> table = new Table<Sample>(tableType)
                .addColumn("ID", Sample::getId)
                .addColumn("DESCRIPTION", Sample::getDescription)
                .addColumn("STATUS", s -> s.getInternal().getStatus().getName())
                .addColumn("INDIVIDUAL_ID", Sample::getIndividualId);

        table.printTable(unwind(queryResultList));
    }

    private void printCohorts(List<DataResult<Cohort>> queryResultList) {
        Table<Cohort> table = new Table<Cohort>(tableType)
                .addColumn("ID", Cohort::getId)
                .addColumnEnum("TYPE", Cohort::getType)
                .addColumn("DESCRIPTION", Cohort::getDescription)
                .addColumn("STATUS", c -> c.getInternal().getStatus().getName())
                .addColumnNumber("TOTAL_SAMPLES", c -> c.getSamples().size())
                .addColumn("SAMPLES", c -> c.getSamples().stream().map(Sample::getId).collect(Collectors.joining(",")));

        table.printTable(unwind(queryResultList));
    }

    private void printIndividual(List<DataResult<Individual>> queryResultList) {
        Table<Individual> table = new Table<Individual>(tableType)
                .addColumn("ID", Individual::getId)
                .addColumn("NAME", Individual::getId)
                .addColumnEnum("SEX", Individual::getSex)
                .addColumnEnum("KARYOTYPIC_SEX", Individual::getKaryotypicSex)
                .addColumn("ETHNICITY", Individual::getEthnicity)
                .addColumn("POPULATION", i -> i.getPopulation().getName(), "NA")
                .addColumn("SUBPOPULATION", i -> i.getPopulation().getSubpopulation(), "NA")
                .addColumnEnum("LIFE_STATUS", Individual::getLifeStatus)
                .addColumn("STATUS", i -> i.getInternal().getStatus().getName())
                .addColumn("FATHER_ID", i -> i.getFather().getId())
                .addColumn("MOTHER_ID", i -> i.getMother().getId())
                .addColumn("CREATION_DATE", Individual::getCreationDate);

        table.printTable(unwind(queryResultList));
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
        List<JobColumns> jobColumns = Arrays.asList(
                JobColumns.ID,
                JobColumns.TOOL_ID,
                JobColumns.SUBMISSION,
                JobColumns.STATUS,
                JobColumns.EVENTS,
                JobColumns.START,
                JobColumns.RUNNING_TIME,
                JobColumns.INPUT,
                JobColumns.OUTPUT
        );
        new Table<Job>(tableType)
                .addColumns(jobColumns.stream().map(JobColumns::getColumnSchema).collect(Collectors.toList()))
                .printTable(unwind(queryResultList));
    }

    public interface TableSchema<T> {
        Table.TableColumnSchema<T> getColumnSchema();
    }

    public enum JobColumns implements TableSchema<Job> {
        ID(new Table.TableColumnSchema<>("ID", Job::getId, 60)),
        TOOL_ID(new Table.TableColumnSchema<>("Tool id", job -> job.getTool().getId())),
        STATUS(new Table.TableColumnSchema<>("Status", job -> job.getInternal().getStatus().getName())),
        STEP(new Table.TableColumnSchema<>("Step", job -> {
            if (job.getInternal().getStatus().getName().equals(RUNNING)) {
                String currentStep = job.getExecution().getStatus().getStep();
                int currentStepPosition = 0;
                for (int i = 0; i < job.getExecution().getSteps().size(); i++) {
                    if (job.getExecution().getSteps().get(i).getId().equals(currentStep)) {
                        currentStepPosition = i + 1;
                        break;
                    }
                }
                return job.getExecution().getStatus().getStep() + " " + currentStepPosition + "/" + job.getExecution().getSteps().size();
            } else {
                return null;
            }
        })),
        EVENTS(new Table.TableColumnSchema<>("Events", j -> {
            Map<Event.Type, Long> map = j.getExecution().getEvents().stream()
                    .collect(Collectors.groupingBy(Event::getType, Collectors.counting()));
            if (map.isEmpty()) {
                return null;
            } else {
                return map.entrySet().stream().map(e -> e.getKey() + ":" + e.getValue()).collect(Collectors.joining(", "));
            }
        })),
        STUDY(new Table.TableColumnSchema<>("Study", job -> {
            String id = job.getStudy().getId();
            if (id.contains(":")) {
                return id.split(":")[1];
            } else {
                return id;
            }
        }, 25)),
        SUBMISSION(new Table.TableColumnSchema<>("Submission date", job -> job.getCreationDate() != null
                ? SIMPLE_DATE_FORMAT.format(TimeUtils.toDate(job.getCreationDate())) : "")),
        PRIORITY(new Table.TableColumnSchema<>("Priority", job -> job.getPriority() != null
                ? job.getPriority().name() : "")),
        RUNNING_TIME(new Table.TableColumnSchema<>("Running time", JobColumns::getDurationString)),
        START(new Table.TableColumnSchema<>("Start", job -> getStart(job) != null
                ? SIMPLE_DATE_FORMAT.format(getStart(job)) : "")),
        END(new Table.TableColumnSchema<>("End", job -> getEnd(job) != null
                ? SIMPLE_DATE_FORMAT.format(getEnd(job)) : "")),
        INPUT(new Table.TableColumnSchema<>("Input",  j -> j.getInput().stream().map(File::getName).collect(Collectors.joining(",")), 45)),
        OUTPUT(new Table.TableColumnSchema<>("Output", j -> j.getOutput().stream().map(File::getName).collect(Collectors.joining(",")), 45)),
        OUTPUT_DIRECTORY(new Table.TableColumnSchema<>("Output directory", j -> j.getOutDir().getPath(), 45));

        private final Table.TableColumnSchema<Job> columnSchema;

        JobColumns(Table.TableColumnSchema<Job> columnSchema) {
            this.columnSchema = columnSchema;
        }

        @Override
        public Table.TableColumnSchema<Job> getColumnSchema() {
            return columnSchema;
        }

        private static Date getStart(Job job) {
            return job.getExecution() == null ? null : job.getExecution().getStart();
        }

        private static Date getEnd(Job job) {
            if (job.getExecution() == null) {
                return null;
            } else {
                if (job.getExecution().getEnd() != null) {
                    return job.getExecution().getEnd();
                } else {
                    if (job.getInternal() != null && job.getInternal().getStatus() != null) {
                        if (Enums.ExecutionStatus.ERROR.equals(job.getInternal().getStatus().getName())
                                && StringUtils.isNotEmpty(job.getInternal().getStatus().getDate())) {
                            return TimeUtils.toDate(job.getInternal().getStatus().getDate());
                        }
                    }
                    return null;
                }
            }
        }

        private static String getDurationString(Job job) {
            long durationInMillis = getDurationInMillis(getStart(job), getEnd(job));
            if (durationInMillis > 0) {
                return TimeUtils.durationToStringSimple(durationInMillis);
            } else {
                return "";
            }
        }

        private static long getDurationInMillis(Date start, Date end) {
            long durationInMillis = -1;
            if (start != null) {
                if (end == null) {
                    durationInMillis = Instant.now().toEpochMilli() - start.getTime();
                } else {
                    durationInMillis = end.getTime() - start.getTime();
                }
            }
            return durationInMillis;
        }
    }

    private void printVariableSet(List<DataResult<VariableSet>> queryResultList) {
        new Table<VariableSet>(tableType)
                .addColumn("ID", VariableSet::getId)
                .addColumn("NAME", VariableSet::getName)
                .addColumn("DESCRIPTION", VariableSet::getDescription)
                .addColumn("VARIABLES", v -> v.getVariables().stream().map(Variable::getId).collect(Collectors.joining(",")))
                .printTable(unwind(queryResultList));
    }

    private void printAnnotationSet(List<DataResult<AnnotationSet>> queryResultList) {
        new Table<Map.Entry<String, Object>>(tableType)
                .addColumn("KEY", Map.Entry::getKey)
                .addColumn("VALUE", e -> e.getValue().toString())
                .printTable(queryResultList.stream().flatMap(r -> r.getResults().stream().flatMap(a -> a.getAnnotations().entrySet().stream())).collect(Collectors.toList()));
    }

    private void printTreeFile(RestResponse<FileTree> queryResponse) {
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
                    file.getInternal() != null && file.getInternal().getStatus() != null ? file.getInternal().getStatus().getName() : "",
                    humanReadableByteCount(file.getSize(), false)));

            if (file.getType() == File.Type.DIRECTORY) {
                printRecursiveTree(fileTree.getChildren(), sb, indent + (iterator.hasNext()? "│   " : "    "));
            }
        }
    }

    private <T> List<T> unwind(List<DataResult<T>> queryResultList) {
        return queryResultList.stream().flatMap(r -> r.getResults().stream()).collect(Collectors.toList());
    }

    private String getId(Annotable annotable) {
        return getId(annotable, "-");
    }

    private String getId(Annotable annotable, String defaultStr) {
        return annotable != null ? StringUtils.defaultIfEmpty(annotable.getId(), defaultStr) : defaultStr;
    }
}
