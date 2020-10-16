package org.opencb.opencga.app.cli.main.executors.catalog;

import com.google.common.base.Stopwatch;
import org.apache.commons.lang3.StringUtils;
import org.opencb.commons.datastore.core.Query;
import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.opencga.app.cli.main.io.Table;
import org.opencb.opencga.app.cli.main.io.TextOutputWriter;
import org.opencb.opencga.catalog.db.api.JobDBAdaptor;
import org.opencb.opencga.client.exceptions.ClientException;
import org.opencb.opencga.client.rest.OpenCGAClient;
import org.opencb.opencga.core.common.GitRepositoryState;
import org.opencb.opencga.core.models.common.Enums;
import org.opencb.opencga.core.models.job.Job;
import org.opencb.opencga.core.models.job.JobInternal;
import org.opencb.opencga.core.models.job.JobTop;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.time.Instant;
import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import static org.opencb.opencga.app.cli.main.io.TextOutputWriter.JobColumns.*;

public class JobsTopManager {

    public static final int MAX_ERRORS = 4;
    private final Logger logger = LoggerFactory.getLogger(this.getClass());

    private final OpenCGAClient openCGAClient;
    private final Query baseQuery;
    private final int iterations;
    private final int jobsLimit;
    private final long delay;
    private final boolean plain;

    // FIXME: Use an intermediate buffer to prepare the table, and print in one system call to avoid flashes
    private final ByteArrayOutputStream buffer;
    private final Table<Job> jobTable;
    private PrintStream bufferStream;

    public JobsTopManager(OpenCGAClient openCGAClient, Query query, Integer iterations, Integer jobsLimit, long delay, boolean plain) {
        this(openCGAClient, query, iterations, jobsLimit, delay, plain, parseColumns(null));
    }

    public JobsTopManager(OpenCGAClient openCGAClient, Query query, Integer iterations, Integer jobsLimit, long delay, boolean plain, String columns) {
        this(openCGAClient, query, iterations, jobsLimit, delay, plain, parseColumns(columns));
    }

    public JobsTopManager(OpenCGAClient openCGAClient, Query query, Integer iterations, Integer jobsLimit, long delay, boolean plain, List<TextOutputWriter.JobColumns> columns) {
        this.openCGAClient = openCGAClient;
        this.baseQuery = new Query(query)
            .append(QueryOptions.SORT, JobDBAdaptor.QueryParams.CREATION_DATE.key())
            .append(QueryOptions.ORDER, QueryOptions.DESCENDING);
        this.buffer = new ByteArrayOutputStream();
        this.iterations = iterations == null || iterations <= 0 ? -1 : iterations;
        if (jobsLimit == null || jobsLimit <= 0) {
            String lines = System.getenv("LINES");
            if (StringUtils.isNumeric(lines)) {
                int HEADER_SIZE = 9;
                this.jobsLimit = Integer.parseInt(lines) - HEADER_SIZE;
            } else {
                this.jobsLimit = 20;
            }
        } else {
            this.jobsLimit = jobsLimit;
        }
        this.delay = delay < 0 ? 2 : delay;
        this.plain = plain;

        buffer.reset();
        bufferStream = new PrintStream(buffer);

        Table.TablePrinter tablePrinter = new Table.JAnsiTablePrinter(bufferStream);
        jobTable = new Table<>(tablePrinter);
        for (TextOutputWriter.JobColumns column : columns) {
            jobTable.addColumn(column.getColumnSchema());
        }
        jobTable.setMultiLine(false);

    }

    private static List<TextOutputWriter.JobColumns> parseColumns(String columnsStr) {
        if (StringUtils.isBlank(columnsStr) || columnsStr.equalsIgnoreCase("default")) {
            return Arrays.asList(ID, TOOL_ID, STATUS, STUDY, SUBMISSION, PRIORITY, RUNNING_TIME, START, END);
        } else {
            List<TextOutputWriter.JobColumns> columns = new LinkedList<>();
            for (String c : columnsStr.split(",")) {
                columns.add(TextOutputWriter.JobColumns.valueOf(c.toUpperCase()));
            }
            return columns;
        }
    }

    public void run() throws ClientException, InterruptedException {
        Stopwatch timer = Stopwatch.createStarted();
        int iteration = 0;
        int errors = 0;
        openCGAClient.setThrowExceptionOnError(true);
        while (iterations != iteration) {
            try {
                iteration++;
                if (timer.elapsed(TimeUnit.MINUTES) > 5) {
                    openCGAClient.refresh();
                    timer.reset().start();
                }

                print(openCGAClient.getJobClient().top(baseQuery).firstResult());

                Thread.sleep(TimeUnit.SECONDS.toMillis(this.delay));
                // Reset errors counter
                errors = 0;
            } catch (InterruptedException e) {
                // Do not ignore InterruptedException!!
                throw e;
            } catch (Exception e) {
                errors++;
                if (errors > MAX_ERRORS) {
                    logger.error("Got " + errors + " consecutive errors trying to print Jobs Top");
                    throw e;
                }
            }
        }
    }

    public void print(JobTop top) {
        buffer.reset();
        List<Job> jobList = processJobs(top.getJobs());
        jobTable.updateTable(jobList);

        jobTable.restoreCursorPosition();
        jobTable.println("OpenCGA jobs TOP");
        jobTable.println("  Version " + GitRepositoryState.get().getBuildVersion());
        jobTable.println("  " + TextOutputWriter.SIMPLE_DATE_FORMAT.format(Date.from(Instant.now())));
        jobTable.println();
        jobTable.print(Enums.ExecutionStatus.RUNNING + ": " + top.getStats().getRunning() + ", ");
        jobTable.print(Enums.ExecutionStatus.QUEUED + ": " + top.getStats().getQueued() + ", ");
        jobTable.print(Enums.ExecutionStatus.PENDING + ": " + top.getStats().getPending() + ", ");
        jobTable.print(Enums.ExecutionStatus.DONE + ": " + top.getStats().getDone() + ", ");
        jobTable.print(Enums.ExecutionStatus.ERROR + ": " + top.getStats().getError() + ", ");
        jobTable.print(Enums.ExecutionStatus.ABORTED + ": " + top.getStats().getAborted());
        jobTable.println();
        jobTable.println();
        jobTable.printTable();

        bufferStream.flush();
        System.out.print(buffer);
    }

    private List<Job> processJobs(List<Job> jobs) {
        List<Job> jobList = new LinkedList<>();
        jobs.sort(Comparator.comparing(j -> j.getInternal().getStatus().getName().equals(Enums.ExecutionStatus.RUNNING) ? 0 : 1));
        jobs = trimJobs(jobs);

        int jobDependsMax = 5;
        for (Job job : jobs) {
            jobList.add(job);
            if (job.getDependsOn() != null && !job.getDependsOn().isEmpty()) {
                List<Job> dependsOn = job.getDependsOn();
                dependsOn.removeIf(Objects::isNull);
                if (dependsOn.size() > jobDependsMax) {
                    int size = dependsOn.size();
                    TreeMap<String, Integer> byType = dependsOn
                            .stream()
                            .collect(Collectors.groupingBy(
                                    j -> j.getInternal().getStatus().getName(),
                                    TreeMap::new,
                                    Collectors.summingInt(j -> 1)));
                    int maxStatus = byType.keySet().stream().mapToInt(String::length).max().orElse(0);
                    dependsOn = new ArrayList<>(byType.size());
                    for (Map.Entry<String, Integer> entry : byType.entrySet()) {
                        dependsOn.add(new Job()
                                .setId(StringUtils.rightPad(entry.getKey(), maxStatus) + " : " + entry.getValue() + "/" + size)
                                .setInternal(new JobInternal(new Enums.ExecutionStatus(entry.getKey()))));
                    }
                }
                if (!plain) {
                    for (int i = 0; i < dependsOn.size(); i++) {
                        Job auxJob = dependsOn.get(i);
                        if (i + 1 < dependsOn.size()) {
                            auxJob.setId("├── " + auxJob.getId());
                        } else {
                            auxJob.setId("└── " + auxJob.getId());
                        }
                        jobList.add(auxJob);
                    }
                }
            }
        }

        jobList = trimJobs(jobList);

        return jobList;
    }

    private List<Job> trimJobs(List<Job> jobs) {
        if (jobs.size() <= jobsLimit) {
            return jobs;
        }
        List<Job> jobList = jobs.subList(0, jobsLimit);
        for (int i = jobList.size() - 1; i >= 0; i--) {
            if (jobList.get(i).getId().startsWith("└") || (!jobList.get(i).getId().startsWith("├") && (jobList.get(i).getDependsOn() == null || jobList.get(i).getDependsOn().isEmpty()))) {
                break;
            }
            jobList.remove(i);
        }
        return jobList;

    }

}
