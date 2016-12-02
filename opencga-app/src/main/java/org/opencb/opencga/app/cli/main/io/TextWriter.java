package org.opencb.opencga.app.cli.main.io;

import org.apache.commons.lang3.StringUtils;
import org.opencb.commons.datastore.core.QueryResponse;
import org.opencb.commons.datastore.core.QueryResult;
import org.opencb.opencga.catalog.models.*;
import org.opencb.opencga.catalog.models.acls.permissions.AbstractAclEntry;
import org.opencb.opencga.catalog.models.acls.permissions.StudyAclEntry;
import org.opencb.opencga.core.common.TimeUtils;

import java.io.PrintStream;
import java.util.Iterator;
import java.util.List;
import java.util.stream.Collectors;

/**
 * Created by pfurio on 28/11/16.
 */
public class TextWriter extends AbstractWriter {

    public TextWriter() {
    }

    public TextWriter(WriterConfiguration writerConfiguration) {
        super(writerConfiguration);
    }

    @Override
    public void print(QueryResponse queryResponse) {
        if (checkErrors(queryResponse)) {
            return;
        }

        if (queryResponse.getResponse().size() == 0 || ((QueryResult) queryResponse.getResponse().get(0)).getNumResults() == 0) {
            ps.print("No results found for the query.");
            return;
        }

        ps.print(printMetadata(queryResponse));

        List<QueryResult> queryResultList = queryResponse.getResponse();
        String[] split = queryResultList.get(0).getResultType().split("\\.");
        String clazz = split[split.length - 1];

        switch (clazz) {
            case "User":
                printUser(queryResponse.getResponse());
                break;
            case "Project":
                printProject(queryResponse.getResponse());
                break;
            case "Study":
                printStudy(queryResponse.getResponse());
                break;
            case "File":
                printFiles(queryResponse.getResponse());
                break;
            case "Sample":
                printSamples(queryResponse.getResponse());
                break;
            case "Cohort":
                printCohorts(queryResponse.getResponse());
                break;
            case "Individual":
                printIndividual(queryResponse.getResponse());
                break;
            case "VariableSet":
                printVariableSet(queryResponse.getResponse());
                break;
            case "FileTree":
                printTreeFile(queryResponse);
                break;
            default:
                System.err.println(ANSI_RED + "Error: " + clazz + " not yet supported in text format" + ANSI_RESET);
                break;
        }

    }

    private String printMetadata(QueryResponse queryResponse) {
        StringBuilder sb = new StringBuilder();
        if (writerConfiguration.isMetadata()) {
            int numResults = 0;
//            int totalResults = 0;
            int time = 0;

            List<QueryResult> queryResultList = queryResponse.getResponse();
            for (QueryResult queryResult : queryResultList) {
                numResults += queryResult.getNumResults();
//                totalResults += queryResult.getNumTotalResults();
                time += queryResult.getDbTime();
            }

            sb.append("## Date: ").append(TimeUtils.getTime()).append("\n")
                    .append("## Number of results: ").append(numResults)
                        .append(". Time: ").append(time).append(" ms\n");

            // TODO: Add query info
            sb.append("## Query: { ")
                    .append(queryResponse.getQueryOptions()
                            .entrySet().stream().map(entry -> entry.getKey() + ": " + entry.getValue()).collect(Collectors.joining(", ")))
                    .append(" }\n");
        }
        return sb.toString();
    }

    private void printUser(List<QueryResult<User>> queryResultList) {
        StringBuilder sb = new StringBuilder();
        for (QueryResult<User> queryResult : queryResultList) {
            // Write header
            if (writerConfiguration.isHeader()) {
                sb.append("#(U) ID\tNAME\tE-MAIL\tORGANIZATION\tACCOUNT_TYPE\tDISK_USAGE\tDISK_QUOTA\n");
                sb.append("#(P) \tALIAS\tNAME\tORGANIZATION\tDESCRIPTION\tID\tDISK_USAGE\n");
                sb.append("#(S) \t\tALIAS\tNAME\tTYPE\tDESCRIPTION\tID\t#GROUPS\tDISK_USAGE\n");
            }

            for (User user : queryResult.getResult()) {
                sb.append(String.format("%s%s\t%s\t%s\t%s\t%s\t%d\t%d\n", "", user.getId(), user.getName(), user.getEmail(),
                        user.getOrganization(), user.getAccount().getType(), user.getDiskUsage(), user.getDiskQuota()));

                if (user.getProjects().size() > 0) {
                    for (Project project : user.getProjects()) {
                        sb.append(String.format("%s%s\t%s\t%s\t%s\t%d\t%d\n", " * ", project.getAlias(), project.getName(),
                                project.getOrganization(), project.getDescription(), project.getId(), project.getDiskUsage()));

                        if (project.getStudies().size() > 0) {
                            for (Study study : project.getStudies()) {
                                sb.append(String.format("    - %s\t%s\t%s\t%s\t%d\t%s\t%d\n", study.getAlias(), study.getName(),
                                        study.getType(), study.getDescription(), study.getId(),
                                        StringUtils.join(study.getGroups().stream().map(Group::getName).collect(Collectors.toList()), ", "),
                                        study.getDiskUsage()));

                                if (study.getGroups().size() > 0) {
                                    sb.append("       Groups:\n");
                                    for (Group group : study.getGroups()) {
                                        printGroup(group, sb, "        + ");
                                    }
                                }

                                if (study.getAcl().size() > 0) {
                                    sb.append("       Acl:\n");
                                    for (StudyAclEntry studyAclEntry : study.getAcl()) {
                                        printACL(studyAclEntry, sb, "        + ");
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

    private void printProject(List<QueryResult<Project>> queryResultList) {
        StringBuilder sb = new StringBuilder();
        for (QueryResult<Project> queryResult : queryResultList) {
            // Write header
            sb.append("# ALIAS\tNAME\tID\tORGANIZATION\tORGANISM\tASSEMBLY\tDESCRIPTION\tDISK_USAGE\t#STUDIES\tSTATUS\n");

            for (Project project : queryResult.getResult()) {
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

                sb.append(String.format("%s\t%s\t%d\t%s\t%s\t%s\t%s\t%d\t%d\t%s\n", project.getAlias(), project.getName(),
                        project.getId(), project.getOrganization(), organism, assembly, project.getDescription(), project.getDiskUsage(),
                        project.getStudies().size(), project.getStatus().getName()));
            }
        }
        ps.println(sb.toString());
    }

    private void printStudy(List<QueryResult<Study>> queryResultList) {
        StringBuilder sb = new StringBuilder();
        for (QueryResult<Study> queryResult : queryResultList) {
            // Write header
            sb.append("# ALIAS\tNAME\tTYPE\tDESCRIPTION\tID\t#GROUPS\tDISK_USAGE\t#FILES\t#SAMPLES\t#COHORTS\t#INDIVIDUALS\t#JOBS\t")
                    .append("#VARIABLE_SETS\tSTATUS\n");

            for (Study study : queryResult.getResult()) {
                sb.append(String.format("%s\t%s\t%s\t%s\t%d\t%d\t%d\t%s\t%d\t%d\t%d\t%d\t%d\t%s\n",
                        study.getAlias(), study.getName(), study.getType(), study.getDescription(), study.getId(), study.getGroups().size(),
                        study.getDiskUsage(), study.getFiles().size(), study.getSamples().size(), study.getCohorts().size(),
                        study.getIndividuals().size(), study.getJobs().size(), study.getVariableSets().size(),
                        study.getStatus().getName()));
            }
        }

        ps.println(sb.toString());
    }

    private void printGroup(Group group, StringBuilder sb, String prefix) {
        sb.append(String.format("%s%s\t%s\n", prefix, group.getName(), StringUtils.join(group.getUserIds(), ", ")));
    }

    private void printACL(AbstractAclEntry aclEntry, StringBuilder sb, String prefix) {
        sb.append(String.format("%s%s\t%s\n", prefix, aclEntry.getMember(), aclEntry.getPermissions().toString()));
    }

    private void printFiles(List<QueryResult<File>> queryResultList) {
        StringBuilder sb = new StringBuilder();
        for (QueryResult<File> queryResult : queryResultList) {
            // Write header
            sb.append("# name\ttype\tformat\tbioformat\tdescription\tpath\tid\tstatus\tdiskUsage\tindexStatus\trelatedFiles\t"
                    + "samples\n");

            printFiles(queryResult.getResult(), sb, "");
        }

        ps.println(sb.toString());
    }

    private void printFiles(List<File> files, StringBuilder sb, String format) {
        // # name	type	format	bioformat	description	path	id	status	diskUsage	index status	related files   samples
        for (File file : files) {
            sb.append(String.format("%s%s\t%s\t%s\t%s\t%s\t%s\t%s\t%d\t%s\t%d\t%s\t%s\t%s\n", format, file.getName(), file.getType(),
                    file.getFormat(), file.getBioformat(), file.getDescription(), file.getPath(), file.getUri(), file.getId(),
                    file.getStatus().getName(), file.getDiskUsage(), file.getIndex() != null ? file.getIndex().getStatus() : "NA",
                    StringUtils.join(file.getRelatedFiles().stream().map(File.RelatedFile::getFileId).collect(Collectors.toList()), ", "),
                    StringUtils.join(file.getSampleIds().stream().collect(Collectors.toList()), ", ")));
        }
    }

    private void printSamples(List<QueryResult<Sample>> queryResultList) {
        StringBuilder sb = new StringBuilder();
        for (QueryResult<Sample> queryResult : queryResultList) {
            // Write header
            if (writerConfiguration.isHeader()) {
                sb.append("# NAME\tID\tSOURCE\tDESCRIPTION\tSTATUS\tINDIVIDUAL_NAME\tINDIVIDUAL_ID\n");
            }

            printSamples(queryResult.getResult(), sb, "");
        }

        ps.println(sb.toString());
    }

    private void printSamples(List<Sample> samples, StringBuilder sb, String format) {
        // # name	id	source	description	status	individualName	individualID
        for (Sample sample : samples) {
            String individualName = "NA";
            String individualId = "NA";
            if (sample.getIndividual() != null) {
                if (sample.getIndividual().getId() >= 0) {
                    individualId = Long.toString(sample.getIndividual().getId());
                }
                if (StringUtils.isNotEmpty(sample.getIndividual().getName())) {
                    individualName = sample.getIndividual().getName();
                }
            }
            sb.append(String.format("%s%s\t%d\t%s\t%s\t%s\t%s\t%s\n", format, sample.getName(), sample.getId(), sample.getSource(),
                    sample.getDescription(), sample.getStatus().getName(), individualName, individualId));
        }
    }

    private void printCohorts(List<QueryResult<Cohort>> queryResultList) {
        StringBuilder sb = new StringBuilder();
        for (QueryResult<Cohort> queryResult : queryResultList) {
            // Write header
            if (writerConfiguration.isHeader()) {
                sb.append("# NAME\tID\tTYPE\tDESCRIPTION\tSTATUS\tTOTAL_SAMPLES\tSAMPLES\tFAMILY\n");
            }

            for (Cohort cohort : queryResult.getResult()) {
                sb.append(String.format("%s\t%d\t%s\t%s\t%s\t%d\t%s\t%s\n", cohort.getName(), cohort.getId(), cohort.getType(),
                        cohort.getDescription(), cohort.getStatus().getName(), cohort.getSamples().size(),
                        cohort.getSamples().size() > 0 ? StringUtils.join(cohort.getSamples(), ", ") : "NA",
                        cohort.getFamily() != null && StringUtils.isNotEmpty(cohort.getFamily().getId()) ? cohort.getFamily().getId() : "NA"));
            }
        }

        ps.println(sb.toString());
    }

    private void printIndividual(List<QueryResult<Individual>> queryResultList) {
        StringBuilder sb = new StringBuilder();
        for (QueryResult<Individual> queryResult : queryResultList) {
            // Write header
            if (writerConfiguration.isHeader()) {
                sb.append("# NAME\tID\tFAMILY\tAFFECTATION_STATUS\tSEX\tKARYOTYPIC_SEX\tETHNICITY\tPOPULATION\tSUBPOPULATION\tLIFE_STATUS")
                        .append("\tSTATUS\tFATHER_ID\tMOTHER_ID\tCREATION_DATE\n");
            }

            for (Individual individual : queryResult.getResult()) {
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
                sb.append(String.format("%s\t%d\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\n",
                        individual.getName(), individual.getId(), individual.getFamily(), individual.getAffectationStatus(),
                        individual.getSex(), individual.getKaryotypicSex(), individual.getEthnicity(), population, subpopulation,
                        individual.getLifeStatus(), individual.getStatus().getName(),
                        individual.getFatherId() > 0 ? Long.toString(individual.getFatherId()) : "NA",
                        individual.getMotherId() > 0 ? Long.toString(individual.getMotherId()) : "NA", individual.getCreationDate()));
            }
        }

        ps.println(sb.toString());
    }

    private void printVariableSet(List<QueryResult<VariableSet>> queryResultList) {
        StringBuilder sb = new StringBuilder();
        for (QueryResult<VariableSet> queryResult : queryResultList) {
            // Write header
            if (writerConfiguration.isHeader()) {
                sb.append("# NAME\tID\tDESCRIPTION\tVARIABLES\n");
            }

            for (VariableSet variableSet : queryResult.getResult()) {
                sb.append(String.format("%s\t%s\t%s\t%s\n", variableSet.getName(), variableSet.getId(), variableSet.getDescription(),
                        variableSet.getVariables().stream().map(variable -> variable.getName()).collect(Collectors.joining(", "))));
            }
        }

        ps.println(sb.toString());
    }

    private void printTreeFile(QueryResponse<FileTree> queryResponse) {
        StringBuilder sb = new StringBuilder();
        for (QueryResult<FileTree> fileTreeQueryResult : queryResponse.getResponse()) {
            printRecursiveTree(fileTreeQueryResult.getResult(), sb, "");
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

            sb.append(String.format("%s %s - (%d)   [%s, %s]\n",
                    indent.isEmpty() ? "" : indent + (iterator.hasNext() ? "├──" : "└──"),
                    file.getType() == File.Type.FILE ? file.getName() : file.getName() + "/",
                    file.getId(),
                    file.getStatus().getName(),
                    humanReadableByteCount(file.getDiskUsage(), false)));

            if (file.getType() == File.Type.DIRECTORY) {
                printRecursiveTree(fileTree.getChildren(), sb, indent + (iterator.hasNext()? "│   " : "    "));
            }
        }
    }

    /**
     * Get Bytes numbers in a human readable string
     * See http://stackoverflow.com/a/3758880
     *
     * @param bytes     Quantity of bytes
     * @param si        Use International System (power of 10) or Binary Units (power of 2)
     * @return
     */
    public static String humanReadableByteCount(long bytes, boolean si) {
        int unit = si ? 1000 : 1024;
        if (bytes < unit) return bytes + " B";
        int exp = (int) (Math.log(bytes) / Math.log(unit));
        String pre = (si ? "kMGTPE" : "KMGTPE").charAt(exp-1) + (si ? "" : "i");
        return String.format("%.1f %sB", bytes / Math.pow(unit, exp), pre);
    }
}
