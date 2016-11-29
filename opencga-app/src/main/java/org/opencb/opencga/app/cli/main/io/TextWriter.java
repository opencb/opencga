package org.opencb.opencga.app.cli.main.io;

import org.opencb.commons.datastore.core.QueryResponse;
import org.opencb.commons.datastore.core.QueryResult;
import org.opencb.opencga.catalog.models.File;
import org.opencb.opencga.catalog.models.FileTree;
import org.opencb.opencga.catalog.models.Project;
import org.opencb.opencga.catalog.models.Study;

import java.io.PrintStream;
import java.util.Iterator;
import java.util.List;

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

        if (queryResponse.getResponse().size() == 0) {
            if (writerConfiguration.isPretty()) {
                ps.println("No results found for the query.");
            }
            return;
        }

        List<QueryResult> queryResultList = queryResponse.getResponse();
        String[] split = queryResultList.get(0).getResultType().split("\\.");
        String clazz = split[split.length - 1];

        switch (clazz) {
            case "User":
                break;
            case "Project":
                printProject(queryResponse.getResponse(), ps);
                break;
            case "Study":
//                printStudy(queryResponse, ps);
                break;
            case "File":
                break;
            case "Sample":
                break;
            case "Cohort":
                break;
            case "Individual":
                break;
            case "VariableSet":
                break;
            case "FileTree":
                printTreeFile(queryResponse, ps);
                break;
            default:
                break;
        }



    }

    private void printProject(List<QueryResult<Project>> queryResultList, PrintStream ps) {
        for (QueryResult<Project> queryResult : queryResultList) {
            ps.println("\nId: " + queryResult.getId());
            ps.println("==============================================");

            for (Project project : queryResult.getResult()) {
                ps.println("Project\n=======");
                printProject(project, ps, "");
            }
        }
    }

    private void printProject(Project project, PrintStream ps, String format) {
        ps.println(format + "Id:\t\t" + project.getId());
        ps.println(format + "Alias:\t\t" + project.getAlias());
        ps.println(format + "Name:\t\t" + project.getName());
        ps.println(format + "Description:\t" + project.getDescription());
        ps.println(format + "Creation date:\t" + project.getCreationDate());
        ps.println(format + "Organization:\t" + project.getOrganization());
        ps.println(format + "Status:\t\t" + project.getStatus().getName());
        if (project.getStudies().size() > 0) {
            ps.println(format + "Studies: ");
            format = format + "  ";
            for (Study study : project.getStudies()) {
                ps.println(format + "Study\n" + format + "------");
                printStudy(study, ps, format);
            }
        }
    }

    private void printStudy(Study study, PrintStream ps, String format) {
        ps.println(format + "Id:\t\t" + study.getId());
        ps.println(format + "Alias:\t" + study.getAlias());
        ps.println(format + "Name:\t\t" + study.getName());
        ps.println(format + "Description:\t" + study.getDescription());
        ps.println(format + "Creation date:" + study.getCreationDate());
        ps.println(format + "Status:\t" + study.getStatus().getName());
    }


    private void printTreeFile(QueryResponse<FileTree> queryResponse, PrintStream ps) {
        for (QueryResult<FileTree> fileTreeQueryResult : queryResponse.getResponse()) {
            printRecursiveTree(fileTreeQueryResult.getResult(), ps, "");
        }
    }

    private void printRecursiveTree(List<FileTree> fileTreeList, PrintStream ps, String indent) {
        if (fileTreeList == null || fileTreeList.size() == 0) {
            return;
        }

        for (Iterator<FileTree> iterator = fileTreeList.iterator(); iterator.hasNext(); ) {
            FileTree fileTree = iterator.next();
            File file = fileTree.getFile();

            ps.println(String.format("%s (%d) - %s   [%s, %s]",
                    indent.isEmpty() ? "" : indent + (iterator.hasNext() ? "├──" : "└──"),
                    file.getId(),
                    file.getName(),
                    file.getStatus().getName(),
                    humanReadableByteCount(file.getDiskUsage(), false)));
//                    file.getUri() != null ? " --> " + file.getUri() : ""));

            if (file.getType() == File.Type.DIRECTORY) {
                printRecursiveTree(fileTree.getChildren(), ps, indent + (iterator.hasNext()? "│   " : "    "));
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
