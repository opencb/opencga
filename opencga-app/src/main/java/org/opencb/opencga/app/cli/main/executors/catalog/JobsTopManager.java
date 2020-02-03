package org.opencb.opencga.app.cli.main.executors.catalog;

import com.google.common.base.Stopwatch;
import org.opencb.commons.datastore.core.Query;
import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.opencga.app.cli.main.io.Table;
import org.opencb.opencga.app.cli.main.io.Table.TableColumnSchema;
import org.opencb.opencga.catalog.db.api.JobDBAdaptor;
import org.opencb.opencga.client.exceptions.ClientException;
import org.opencb.opencga.client.rest.OpenCGAClient;
import org.opencb.opencga.core.common.GitRepositoryState;
import org.opencb.opencga.core.common.TimeUtils;
import org.opencb.opencga.core.models.job.JobsTop;
import org.opencb.opencga.core.models.common.Enums;
import org.opencb.opencga.core.models.job.Job;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.PrintStream;
import java.text.SimpleDateFormat;
import java.time.Instant;
import java.util.*;
import java.util.concurrent.TimeUnit;

import static java.util.Optional.ofNullable;

public class JobsTopManager {

    private static final String DATE_PATTERN = "yyyy-MM-dd HH:mm:ss";
    private static final SimpleDateFormat SIMPLE_DATE_FORMAT = new SimpleDateFormat(DATE_PATTERN);

    private final OpenCGAClient openCGAClient;
    private final Query baseQuery;
    private final int iterations;
    private final int jobsLimit;
    private final long delay;
    // FIXME: Use an intermediate buffer to prepare the table, and print in one system call to avoid flashes
    private final ByteArrayOutputStream buffer;
    private final QueryOptions queryOptions = new QueryOptions()
            .append(QueryOptions.INCLUDE, "id,name,status,execution,creationDate")
            .append(QueryOptions.COUNT, true)
            .append(QueryOptions.ORDER, QueryOptions.ASCENDING);
    private final QueryOptions countOptions = new QueryOptions()
            .append(QueryOptions.COUNT, true)
            .append(QueryOptions.LIMIT, 0);
    private final Table<Job> jobTable;

    public JobsTopManager(OpenCGAClient openCGAClient, String study, Integer iterations, int jobsLimit, long delay) {
        this.openCGAClient = openCGAClient;
        this.baseQuery = new Query();
        baseQuery.putIfNotEmpty(JobDBAdaptor.QueryParams.STUDY.key(), study);
        this.iterations = iterations == null || iterations <= 0 ? -1 : iterations;
        this.jobsLimit = jobsLimit <= 0 ? 20 : jobsLimit;
        this.delay = delay < 0 ? 2 : delay;
        this.buffer = new ByteArrayOutputStream();

        // TODO: Make this configurable
        List<TableColumnSchema<Job>> columns = new ArrayList<>();
        columns.add(new TableColumnSchema<>("ID", Job::getId, 50));
        columns.add(new TableColumnSchema<>("Status", job -> job.getStatus().getName()));
        columns.add(new TableColumnSchema<>("Creation", Job::getCreationDate));
        columns.add(new TableColumnSchema<>("Time", JobsTopManager::getDurationString));
        columns.add(new TableColumnSchema<>("Start", job -> SIMPLE_DATE_FORMAT.format(getStart(job))));
        columns.add(new TableColumnSchema<>("End", job -> SIMPLE_DATE_FORMAT.format(getEnd(job))));


        // TODO: Decide if use Ascii or JAnsi
        Table.TablePrinter tablePrinter = new Table.AsciiTablePrinter();
        // TODO: Improve style
//        Table.TablePrinter tablePrinter = new Table.JAnsiTablePrinter();
        jobTable = new Table<>(tablePrinter);
        jobTable.addColumns(columns);

    }

    public void run() throws ClientException, InterruptedException {
        Stopwatch timer = Stopwatch.createStarted();
        int iteration = 0;
        while (iterations != iteration) {
            iteration++;
            if (timer.elapsed(TimeUnit.MINUTES) > 5) {
                openCGAClient.refresh();
                timer.reset().start();
            }

            print(openCGAClient.getJobClient().top(baseQuery).firstResult());
            Thread.sleep(TimeUnit.SECONDS.toMillis(this.delay));
        }
    }

    public void print(JobsTop top) {
        // Reuse buffer to avoid allocate new memory
        buffer.reset();

        // FIXME: Use intermediate buffer
//        PrintStream out = new PrintStream(buffer);
        PrintStream out = System.out;

        jobTable.printFullLine();
        out.println();
        out.println("OpenCGA jobs TOP");
        out.println("  Version " + GitRepositoryState.get().getBuildVersion());
        out.println("  " + SIMPLE_DATE_FORMAT.format(Date.from(Instant.now())));
        out.println();
        TreeMap<String, Long> counts = new TreeMap<>(top.getJobStatusCount());
        out.print(Enums.ExecutionStatus.RUNNING + ": " + ofNullable(counts.remove(Enums.ExecutionStatus.RUNNING + ", ")).orElse(0L));
        out.print(Enums.ExecutionStatus.QUEUED + ": " + ofNullable(counts.remove(Enums.ExecutionStatus.QUEUED + ", ")).orElse(0L));
        out.print(Enums.ExecutionStatus.PENDING + ": " + ofNullable(counts.remove(Enums.ExecutionStatus.PENDING + ", ")).orElse(0L));
        out.print(Enums.ExecutionStatus.DONE + ": " + ofNullable(counts.remove(Enums.ExecutionStatus.DONE + ", ")).orElse(0L));
        out.print(Enums.ExecutionStatus.ERROR + ": " + ofNullable(counts.remove(Enums.ExecutionStatus.ERROR + ", ")).orElse(0L));
        counts.forEach((status, count) -> {
            out.print(", " + status + ": " + count);
        });
        out.println();

        jobTable.updateTable(top.getJobs());
        jobTable.print();

        // FIXME: Use intermediate buffer
//        out.flush();
//        System.out.print(buffer);
    }

    private static Date getStart(Job job) {
        return job.getExecution() == null ? null : job.getExecution().getStart();
    }

    private static Date getEnd(Job job) {
        return job.getExecution() == null ? null : job.getExecution().getEnd();
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
