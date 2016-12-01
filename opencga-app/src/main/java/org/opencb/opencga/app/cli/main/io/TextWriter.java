package org.opencb.opencga.app.cli.main.io;

import org.apache.commons.lang3.StringUtils;
import org.opencb.commons.datastore.core.QueryResponse;
import org.opencb.commons.datastore.core.QueryResult;
import org.opencb.opencga.catalog.models.*;
import org.opencb.opencga.catalog.models.acls.permissions.AbstractAclEntry;
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
    public void print(QueryResponse queryResponse, WriterConfiguration writerConfiguration, PrintStream ps) {
        if (writerConfiguration == null) {
            writerConfiguration = new WriterConfiguration();
        }

        if (checkErrors(queryResponse)) {
            return;
        }

        if (queryResponse.getResponse().size() == 0 || ((QueryResult) queryResponse.getResponse().get(0)).getNumResults() == 0) {
            ps.println("No results found for the query.");
            return;
        }

        List<QueryResult> queryResultList = queryResponse.getResponse();
        String[] split = queryResultList.get(0).getResultType().split("\\.");
        String clazz = split[split.length - 1];

        switch (clazz) {
            case "User":
                printUser(queryResponse.getResponse(), writerConfiguration, ps);
                break;
            case "Project":
                printProject(queryResponse.getResponse(), writerConfiguration, ps);
                break;
            case "Study":
                printStudy(queryResponse.getResponse(), writerConfiguration, ps);
                break;
            case "File":
                printFiles(queryResponse.getResponse(), writerConfiguration, ps);
                break;
            case "Sample":
                printSamples(queryResponse.getResponse(), writerConfiguration, ps);
                break;
            case "Cohort":
                break;
            case "Individual":
                break;
            case "VariableSet":
                break;
            case "FileTree":
                printTreeFile(queryResponse, writerConfiguration, ps);
                break;
            default:
                System.err.println(ANSI_RED + "Error: " + clazz + " not yet supported in text format" + ANSI_RESET);
                break;
        }

    }

    private void printHeader(QueryResult queryResult, StringBuilder sb) {
        sb.append("# ").append(TimeUtils.getTime()).append(" - ")
                .append(queryResult.getNumResults()).append("/").append(queryResult.getNumTotalResults()).append("results. ")
                .append(queryResult.getDbTime()).append(" ms.\n");
    }

    private void printUser(List<QueryResult<User>> queryResultList, WriterConfiguration writerConfiguration, PrintStream ps) {
        StringBuilder sb = new StringBuilder();
        for (QueryResult<User> queryResult : queryResultList) {
            // Write num results and time (metadata)
            if (writerConfiguration.isMetadata()) {
                printHeader(queryResult, sb);
            }
            // Write header
            if (writerConfiguration.isHeader()) {
                sb.append("#(U) id\tname\te-mail\torganization\taccountType\tdiskUsage\tdiskQuota\n");
                sb.append("#(P) \talias\tname\torganization\tdescription\tid\tdiskUsage\n");
                sb.append("#(S) \t\talias\tname\ttype\tdescription\tid\tgroups\tdiskUsage\n");
            }

            for (User user : queryResult.getResult()) {
                printUser(user, sb, "");
            }
        }

        ps.println(sb.toString());

    }


    private void printUser(User user, StringBuilder sb, String format) {
        // #(U) id	name	e-mail	organization	account type	diskUsage	diskQuota
        sb.append(String.format("%s%s\t%s\t%s\t%s\t%s\t%d\t%d\n", format, user.getId(), user.getName(), user.getEmail(),
                user.getOrganization(), user.getAccount().getType(), user.getDiskUsage(), user.getDiskQuota()));

        if (user.getProjects().size() > 0) {
            format = format + "\t";
            for (Project project : user.getProjects()) {
                printProject(project, sb, format);
            }
        }
    }

    @Deprecated
    private void printUser(User user, PrintStream ps, String format) {
        ps.println(format + "Name:\t\t" + user.getName());
        ps.println(format + "Id:\t\t" + user.getId());
        ps.println(format + "Email:\t\t" + user.getEmail());
        ps.println(format + "Organization:\t" + user.getOrganization());
        ps.println(format + "Status:\t\t" + user.getStatus().getName());
        ps.println(format + "Disk usage:\t" + user.getDiskUsage());
        ps.println(format + "Account:");
        printAccount(user.getAccount(), ps, format + "  ");

        if (user.getProjects().size() > 0) {
            ps.println(format + "Projects:");
            format = format + "  ";
            for (Project project : user.getProjects()) {
                ps.println(format + "- Project\n" + format + "  ------");
                printProject(project, ps, format + "    ");
            }
        }
    }

    private void printAccount(Account account, PrintStream ps, String format) {
        ps.println(format + "Type:\t" + account.getType() + " - " + account.getAuthOrigin());
        ps.println(format + "Date:\t" + account.getCreationDate() + " - " + account.getExpirationDate());
    }

    private void printProject(List<QueryResult<Project>> queryResultList, WriterConfiguration writerConfiguration, PrintStream ps) {
//        for (QueryResult<Project> queryResult : queryResultList) {
//            ps.println("QueryResult id: " + queryResult.getId() + "\n");
////            ps.println("==============================================");
//
//            for (Project project : queryResult.getResult()) {
//                ps.println("- Project\n  =======");
//                printProject(project, ps, "    ");
//            }
//        }

        StringBuilder sb = new StringBuilder();
        for (QueryResult<Project> queryResult : queryResultList) {
            // Write num results and time (metadata)
            if (writerConfiguration.isMetadata()) {
                printHeader(queryResult, sb);
            }

            // Write header
            sb.append("#(P) alias\tname\torganization\tdescription\tid\tdiskUsage\n");
            sb.append("#(S) \talias\tname\ttype\tdescription\tid\tgroups\tdiskUsage\n");

            for (Project project : queryResult.getResult()) {
                printProject(project, sb, "");
            }
        }

        ps.println(sb.toString());

    }

    @Deprecated
    private void printProject(Project project, PrintStream ps, String format) {
        ps.println(format + "Id:\t\t" + project.getId());
        ps.println(format + "Alias:\t\t" + project.getAlias());
        ps.println(format + "Name:\t\t" + project.getName());
        ps.println(format + "Description:\t" + project.getDescription());
        ps.println(format + "Creation date:\t" + project.getCreationDate());
        ps.println(format + "Organization:\t" + project.getOrganization());
        ps.println(format + "Status:\t\t" + project.getStatus().getName());
        if (project.getStudies().size() > 0) {
            ps.println(format + "Studies:");
            format = format + "  ";
            for (Study study : project.getStudies()) {
                ps.println(format + "- Study\n" + format + "  ------");
                printStudy(study, ps, format + "    ");
            }
        }
    }

    private void printProject(Project project, StringBuilder sb, String format) {
        // #(P) \talias\tname\torganization\tdescription\tid\tdiskUsage\n
        sb.append(String.format("%s%s\t%s\t%s\t%s\t%d\t%d\n", format, project.getAlias(), project.getName(), project.getOrganization(),
                project.getDescription(), project.getId(), project.getDiskUsage()));

        if (project.getStudies().size() > 0) {
            format = format + "\t";
            for (Study study : project.getStudies()) {
                printStudy(study, sb, format);
            }
        }
    }

    private void printStudy(List<QueryResult<Study>> queryResultList, WriterConfiguration writerConfiguration, PrintStream ps) {
//        for (QueryResult<Study> queryResult : queryResultList) {
//            ps.println("QueryResult id: " + queryResult.getId());
//            ps.println("==============================================");
//
//            for (Study study : queryResult.getResult()) {
//                ps.println("- Study\n  =======");
//                printStudy(study, ps, "    ");
//            }
//        }

        StringBuilder sb = new StringBuilder();
        for (QueryResult<Study> queryResult : queryResultList) {
            // Write num results and time (metadata)
            if (writerConfiguration.isMetadata()) {
                printHeader(queryResult, sb);
            }

            // Write header
            sb.append("# alias\tname\ttype\tdescription\tid\tgroups\tdiskUsage\n");

            for (Study study : queryResult.getResult()) {
                printStudy(study, sb, "");
            }
        }

        ps.println(sb.toString());
    }

    @Deprecated
    private void printStudy(Study study, PrintStream ps, String format) {
        ps.println(format + "Id:\t\t" + study.getId());
        ps.println(format + "Alias:\t" + study.getAlias());
        ps.println(format + "Name:\t\t" + study.getName());
        ps.println(format + "Description:\t" + study.getDescription());
        ps.println(format + "Creation date:" + study.getCreationDate());
        ps.println(format + "Status:\t" + study.getStatus().getName());

        if (study.getGroups().size() > 0) {
            ps.println(format + "Groups:");
            printGroups(study.getGroups(), ps, format + "  ");
        }

        if (study.getAcl().size() > 0) {
            ps.println(format + "ACL:");
            printACLs(study.getAcl(), ps, format + "  ");
        }

        if (study.getFiles().size() > 0) {
            ps.println(format + "Total files: " + study.getFiles().size());
        }

        if (study.getSamples().size() > 0) {
            ps.println(format + "Total samples: " + study.getSamples().size());
        }

        if (study.getIndividuals().size() > 0) {
            ps.println(format + "Total individuals: " + study.getIndividuals().size());
        }

        if (study.getCohorts().size() > 0) {
            ps.println(format + "Total cohorts: " + study.getCohorts().size());
        }
    }

    private void printStudy(Study study, StringBuilder sb, String format) {
        // #(S) 		alias	name	type	description	id	groups	diskUsage
        sb.append(String.format("%s%s\t%s\t%s\t%s\t%d\t%s\t%d\n", format, study.getAlias(), study.getName(), study.getType(),
                study.getDescription(), study.getId(),
                StringUtils.join(study.getGroups().stream().map(Group::getName).collect(Collectors.toList()), ", "), study.getDiskUsage()));
    }

    private void printGroups(List<Group> groupList, PrintStream ps, String format) {
        for (Group group : groupList) {
            ps.println(format + group.getName() + ":\t" + StringUtils.join(group.getUserIds(), ", "));
        }
    }

    private void printACLs(List<? extends AbstractAclEntry> aclEntries, PrintStream ps, String format) {
        for (AbstractAclEntry aclEntry : aclEntries) {
            ps.println(format + aclEntry.getMember() + ":\t" + aclEntry.getPermissions().toString());
        }
    }

    private void printFiles(List<QueryResult<File>> queryResultList, WriterConfiguration writerConfiguration, PrintStream ps) {
        StringBuilder sb = new StringBuilder();
        for (QueryResult<File> queryResult : queryResultList) {
            // Write num results and time (metadata)
            if (writerConfiguration.isMetadata()) {
                printHeader(queryResult, sb);
            }

            // Write header
            sb.append("# name\ttype\tformat\tbioformat\tdescription\tpath\tid\tstatus\tdiskUsage\tindexStatus\trelatedFiles\t"
                    + "samples\n");

            printFiles(queryResult.getResult(), sb, "");
        }

        ps.println(sb.toString());

//        ps.println(sb.toString());
//        for (QueryResult<File> queryResult : queryResultList) {
//            ps.println("QueryResult id: " + queryResult.getId());
//            ps.println("==============================================");
//
//            ps.println("Showing " + queryResult.getNumResults() + " results out of the " + queryResult.getNumTotalResults()
//                    + " matching the query");
//            printFiles(queryResult.getResult(), ps, "  ");
//        }
    }

    @Deprecated
    private void printFiles(List<File> files, PrintStream ps, String format) {
        for (File file : files) {
            ps.println(String.format("%s (%d) - %s   [%s]", format, file.getId(), file.getName(), file.getPath()));
        }
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

    private void printSamples(List<QueryResult<Sample>> queryResultList, WriterConfiguration writerConfiguration, PrintStream ps) {
        for (QueryResult<Sample> queryResult : queryResultList) {
            ps.println("QueryResult id: " + queryResult.getId());
            ps.println("==============================================");

            ps.println("Showing " + queryResult.getNumResults() + " results out of the " + queryResult.getNumTotalResults()
                    + " matching the query");
            printSamples(queryResult.getResult(), ps, "  ");
        }
    }

    private void printSamples(List<Sample> samples, PrintStream ps, String format) {
        String internalFormat = format + "    ";
        for (Sample sample : samples) {
            ps.println(format + "- Sample (" + sample.getId() + ")");

            ps.println(internalFormat + "Id:\t\t" + sample.getId());
            ps.println(internalFormat + "Name:\t\t" + sample.getName());
            ps.println(internalFormat + "Source:\t\t" + sample.getSource());
            ps.println(internalFormat + "Description:\t" + sample.getDescription());
            if (sample.getIndividual() != null && sample.getIndividual().getId() > 0) {
                Individual individual = sample.getIndividual();
                ps.println(String.format("%sIndividual:\t(%d) - %s [%s]", internalFormat, individual.getId(), individual.getName(),
                        individual.getFamily()));
            }
            ps.println(internalFormat + "Status:\t\t" + sample.getStatus().getName());
        }
    }


    private void printTreeFile(QueryResponse<FileTree> queryResponse, WriterConfiguration writerConfiguration, PrintStream ps) {
        StringBuilder sb = new StringBuilder();
        for (QueryResult<FileTree> fileTreeQueryResult : queryResponse.getResponse()) {
            if (writerConfiguration.isMetadata()) {
                printHeader(fileTreeQueryResult, sb);
            }
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

            sb.append(String.format("%s (%d) - %s   [%s, %s]\n",
                    indent.isEmpty() ? "" : indent + (iterator.hasNext() ? "├──" : "└──"),
                    file.getId(),
                    file.getName(),
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
