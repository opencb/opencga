package org.opencb.opencga.app.cli.main.executors.catalog;

import com.google.common.base.Stopwatch;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.opencb.commons.datastore.core.Query;
import org.opencb.opencga.app.cli.main.io.Table;
import org.opencb.opencga.app.cli.main.io.Table.TableColumnSchema;
import org.opencb.opencga.client.exceptions.ClientException;
import org.opencb.opencga.client.rest.OpenCGAClient;
import org.opencb.opencga.core.common.GitRepositoryState;
import org.opencb.opencga.core.common.TimeUtils;
import org.opencb.opencga.core.models.common.Enums;
import org.opencb.opencga.core.models.job.Job;
import org.opencb.opencga.core.models.job.JobInternal;
import org.opencb.opencga.core.models.job.JobTop;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.text.SimpleDateFormat;
import java.time.Instant;
import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import static org.opencb.opencga.app.cli.main.executors.catalog.JobsTopManager.Columns.*;

public class JobsTopManager {

    private static final String DATE_PATTERN = "yyyy-MM-dd HH:mm:ss";
    private static final SimpleDateFormat SIMPLE_DATE_FORMAT = new SimpleDateFormat(DATE_PATTERN);
    public static final int MAX_ERRORS = 4;
    private final Logger logger = LoggerFactory.getLogger(this.getClass());

    private final OpenCGAClient openCGAClient;
    private final Query baseQuery;
    private final int iterations;
    private final int jobsLimit;
    private final long delay;

    // FIXME: Use an intermediate buffer to prepare the table, and print in one system call to avoid flashes
    private final ByteArrayOutputStream buffer;
    private final Table<Job> jobTable;
    private PrintStream bufferStream;

    public enum Columns {
        ID,
        TOOL_ID,
        STATUS,
        STUDY,
        SUBMISSION,
        PRIORITY,
        RUNNING_TIME,
        START,
        END
    }

    public JobsTopManager(OpenCGAClient openCGAClient, Query query, Integer iterations, Integer jobsLimit, long delay) {
        this(openCGAClient, query, Arrays.asList(ID, TOOL_ID, STATUS, STUDY, SUBMISSION, PRIORITY, RUNNING_TIME, START, END), iterations,
                jobsLimit, delay);
    }

    public JobsTopManager(OpenCGAClient openCGAClient, Query query, List<Columns> columns, Integer iterations, Integer jobsLimit, long delay) {
        this.openCGAClient = openCGAClient;
        this.baseQuery = new Query(query);
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

        List<TableColumnSchema<Job>> tableColumnList = new ArrayList<>(columns.size());
        for (Columns column : columns) {
            switch (column) {
                case ID:
                    tableColumnList.add(new TableColumnSchema<>("ID", Job::getId, 50));
                    break;
                case TOOL_ID:
                    tableColumnList.add(new TableColumnSchema<>("Tool id", job -> job.getTool().getId()));
                    break;
                case STATUS:
                    tableColumnList.add(new TableColumnSchema<>("Status", job -> job.getInternal().getStatus().getName()));
                    break;
                case STUDY:
                    tableColumnList.add(new TableColumnSchema<>("Study", job -> {
                        String id = job.getStudy().getId();
                        if (id.contains(":")) {
                            return id.split(":")[1];
                        } else {
                            return id;
                        }
                    }, 25));
                    break;
                case SUBMISSION:
                    tableColumnList.add(new TableColumnSchema<>("Submission date", job -> job.getCreationDate() != null
                            ? SIMPLE_DATE_FORMAT.format(TimeUtils.toDate(job.getCreationDate())) : ""));
                    break;
                case PRIORITY:
                    tableColumnList.add(new TableColumnSchema<>("Priority", job -> job.getPriority() != null
                            ? job.getPriority().name() : ""));
                    break;
                case RUNNING_TIME:
                    tableColumnList.add(new TableColumnSchema<>("Running time", JobsTopManager::getDurationString));
                    break;
                case START:
                    tableColumnList.add(new TableColumnSchema<>("Start", job -> getStart(job) != null
                            ? SIMPLE_DATE_FORMAT.format(getStart(job)) : ""));
                    break;
                case END:
                    tableColumnList.add(new TableColumnSchema<>("End", job -> getEnd(job) != null
                            ? SIMPLE_DATE_FORMAT.format(getEnd(job)) : ""));
                    break;
                default:
                    throw new IllegalArgumentException("Unknown column " + column);
            }
        }

        buffer.reset();
        bufferStream = new PrintStream(buffer);

        Table.TablePrinter tablePrinter = new Table.JAnsiTablePrinter(bufferStream);
        jobTable = new Table<>(tablePrinter);
        jobTable.addColumns(tableColumnList);
        jobTable.setMultiLine(false);

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
        jobTable.println("  " + SIMPLE_DATE_FORMAT.format(Date.from(Instant.now())));
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
        jobs.sort(Comparator.comparing(Job::getCreationDate).thenComparing(j -> CollectionUtils.size(j.getDependsOn())));
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

        jobList = trimJobs(jobList);

        return jobList;
    }

    private List<Job> trimJobs(List<Job> jobs) {
        int i = 0;
        while (jobs.size() > jobsLimit) {
            if (i > jobs.size()) {
                jobs = jobs.subList(jobs.size() - jobsLimit, jobs.size());
                while (jobs.size() > 0 && (jobs.get(0).getId().startsWith("├") || jobs.get(0).getId().startsWith("└"))) {
                    jobs.remove(0);
                }
                break;
            }
            Job job = jobs.get(i);
            if (job.getInternal() != null
                    && job.getInternal().getStatus() != null
                    && Enums.ExecutionStatus.RUNNING.equals(job.getInternal().getStatus().getName())) {
                i++;
            } else {
                jobs.remove(i);
                while (jobs.size() > i && (jobs.get(i).getId().startsWith("├") || jobs.get(i).getId().startsWith("└"))) {
                    jobs.remove(i);
                }
            }
        }
        return jobs;
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
