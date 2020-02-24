package org.opencb.opencga.app.cli.main.executors.catalog;

import com.google.common.base.Stopwatch;
import org.apache.commons.lang3.StringUtils;
import org.opencb.commons.datastore.core.ObjectMap;
import org.opencb.commons.datastore.core.Query;
import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.opencga.catalog.db.api.JobDBAdaptor;
import org.opencb.opencga.client.exceptions.ClientException;
import org.opencb.opencga.client.rest.OpenCGAClient;
import org.opencb.opencga.core.common.GitRepositoryState;
import org.opencb.opencga.core.common.TimeUtils;
import org.opencb.opencga.core.models.common.Enums;
import org.opencb.opencga.core.models.job.Job;
import org.opencb.opencga.core.response.OpenCGAResult;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.PrintStream;
import java.text.SimpleDateFormat;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.List;
import java.util.concurrent.TimeUnit;

public class JobsTop {

    private static final String DATE_PATTERN = "yyyy-MM-dd HH:mm:ss";
    private static final SimpleDateFormat SIMPLE_DATE_FORMAT = new SimpleDateFormat(DATE_PATTERN);
    private static final int STATUS_PAD = 10;
    private static final int DATE_PAD = DATE_PATTERN.length();
    private static final int DURATION_PAD = 8;
    private static final String SEP = " | ";

    private final OpenCGAClient openCGAClient;
    private final Query baseQuery;
    private final int iterations;
    private final int jobsLimit;
    private final long delay;
    private final ByteArrayOutputStream buffer;
    private final QueryOptions queryOptions = new QueryOptions()
            .append(QueryOptions.INCLUDE, "id,name,internal.status,execution,creationDate")
            .append(QueryOptions.COUNT, true)
            .append(QueryOptions.ORDER, QueryOptions.ASCENDING);
    private final QueryOptions countOptions = new QueryOptions()
            .append(QueryOptions.COUNT, true)
            .append(QueryOptions.LIMIT, 0);

    public JobsTop(OpenCGAClient openCGAClient, String study, Integer iterations, int jobsLimit, long delay) {
        this.openCGAClient = openCGAClient;
        this.baseQuery = new Query();
        baseQuery.putIfNotEmpty(JobDBAdaptor.QueryParams.STUDY.key(), study);
        this.iterations = iterations == null || iterations <= 0 ? -1 : iterations;
        this.jobsLimit = jobsLimit <= 0 ? 20 : jobsLimit;
        this.delay = delay < 0 ? 2 : delay;
        this.buffer = new ByteArrayOutputStream();
    }

    public void run() throws ClientException, IOException, InterruptedException {
        Stopwatch timer = Stopwatch.createStarted();
        int iteration = 0;
        while (iterations != iteration) {
            iteration++;
            if (timer.elapsed(TimeUnit.MINUTES) > 5) {
                openCGAClient.refresh();
                timer.reset().start();
            }
            loop();
            Thread.sleep(TimeUnit.SECONDS.toMillis(this.delay));
        }
    }

    public void loop() throws ClientException {
        int jobsLimit = this.jobsLimit;
        OpenCGAResult<Job> running = openCGAClient.getJobClient().search(
                new ObjectMap(baseQuery)
                        .appendAll(queryOptions)
                        .append(JobDBAdaptor.QueryParams.INTERNAL_STATUS_NAME.key(), Enums.ExecutionStatus.RUNNING)
                        .append(QueryOptions.LIMIT, jobsLimit)
                        .append(QueryOptions.SORT, "execution.start")
        ).getResponses().get(0);
        jobsLimit -= running.getResults().size();

        OpenCGAResult<Job> queued = openCGAClient.getJobClient().search(
                new ObjectMap(baseQuery)
                        .appendAll(queryOptions)
                        .append(JobDBAdaptor.QueryParams.INTERNAL_STATUS_NAME.key(), Enums.ExecutionStatus.QUEUED)
                        .append(QueryOptions.LIMIT, jobsLimit)
                        .append(QueryOptions.SORT, "creationDate")
        ).getResponses().get(0);
        jobsLimit -= queued.getResults().size();

        OpenCGAResult<Job> pending = openCGAClient.getJobClient().search(
                new ObjectMap(baseQuery)
                        .appendAll(queryOptions)
                        .append(JobDBAdaptor.QueryParams.INTERNAL_STATUS_NAME.key(), Enums.ExecutionStatus.PENDING)
                        .append(QueryOptions.LIMIT, jobsLimit)
                        .append(QueryOptions.SORT, "creationDate")
        ).getResponses().get(0);
        jobsLimit -= pending.getResults().size();

        boolean truncatedJobs = jobsLimit <= 0;

        long doneCount = openCGAClient.getJobClient().search(new ObjectMap(baseQuery).appendAll(countOptions)
                .append(JobDBAdaptor.QueryParams.INTERNAL_STATUS_NAME.key(), Enums.ExecutionStatus.DONE)).getResponses().get(0).getNumMatches();
        long errorCount = openCGAClient.getJobClient().search(new ObjectMap(baseQuery).appendAll(countOptions)
                .append(JobDBAdaptor.QueryParams.INTERNAL_STATUS_NAME.key(), Enums.ExecutionStatus.ERROR)).getResponses().get(0).getNumMatches();

        List<Job> finishedJobs = openCGAClient.getJobClient().search(
                new ObjectMap(baseQuery)
                        .appendAll(queryOptions)
                        .append(JobDBAdaptor.QueryParams.INTERNAL_STATUS_NAME.key(),
                                Enums.ExecutionStatus.DONE + "," + Enums.ExecutionStatus.ERROR + "," + Enums.ExecutionStatus.ABORTED)
                        .append(QueryOptions.LIMIT, Math.max(1, jobsLimit))
                        .append(QueryOptions.SORT, "execution.end")
                        .append(QueryOptions.ORDER, QueryOptions.DESCENDING) // Get last n elements
        ).allResults();
        Collections.reverse(finishedJobs); // Reverse elements

        List<Job> allJobs = new ArrayList<>(running.getResults().size() + pending.getResults().size() + queued.getResults().size());
        allJobs.addAll(finishedJobs);
        allJobs.addAll(running.getResults());
        allJobs.addAll(queued.getResults());
        allJobs.addAll(pending.getResults());

        print(running, queued, pending, doneCount, errorCount, allJobs, truncatedJobs);
    }

    public void print(OpenCGAResult<Job> running, OpenCGAResult<Job> queued, OpenCGAResult<Job> pending,
                      long doneCount, long errorCount,
                      List<Job> allJobs, boolean truncatedJobs) {
        // Reuse buffer to avoid allocate new memory
        buffer.reset();
        PrintStream out = new PrintStream(buffer);

        int idPad = allJobs.stream().mapToInt(j -> j.getId().length()).max().orElse(10);
        String line = StringUtils.repeat("-", idPad + SEP.length()
                + STATUS_PAD + SEP.length()
                + DURATION_PAD + SEP.length()
                + DATE_PAD + SEP.length()
                + DATE_PAD + SEP.length()
                + DATE_PAD);

        out.println();
        out.println();
        out.println(line);
        out.println("OpenCGA jobs TOP");
        out.println("  Version " + GitRepositoryState.get().getBuildVersion());
        out.println("  " + SIMPLE_DATE_FORMAT.format(Date.from(Instant.now())));
        out.println();
        out.println("Running: " + running.getNumMatches()
                + ", Queued: " + queued.getNumMatches()
                + ", Pending: " + pending.getNumMatches()
                + ", Done: " + doneCount
                + ", Error: " + errorCount);
        out.println(line);
        out.println(StringUtils.rightPad("ID", idPad) + SEP              // COLUMN - 1
                + StringUtils.rightPad("Status", STATUS_PAD) + SEP       // COLUMN - 2
                + StringUtils.rightPad("Creation", DATE_PAD) + SEP       // COLUMN - 3
                + StringUtils.rightPad("Duration", DURATION_PAD) + SEP   // COLUMN - 4
                + StringUtils.rightPad("Start", DATE_PAD) + SEP          // COLUMN - 5
                + StringUtils.rightPad("End", DATE_PAD));                // COLUMN - 6

        out.println(StringUtils.rightPad("", idPad, "-") + SEP           // COLUMN - 1
                + StringUtils.repeat("-", STATUS_PAD) + SEP              // COLUMN - 2
                + StringUtils.repeat("-", DATE_PAD) + SEP                // COLUMN - 3
                + StringUtils.repeat("-", DURATION_PAD) + SEP            // COLUMN - 4
                + StringUtils.repeat("-", DATE_PAD) + SEP                // COLUMN - 5
                + StringUtils.repeat("-", DATE_PAD));                    // COLUMN - 6

        for (Job job : allJobs) {
            Date start = job.getExecution() == null ? null : job.getExecution().getStart();
            Date end = job.getExecution() == null ? null : job.getExecution().getEnd();
            long durationInMillis = getDurationInMillis(start, end);

            // COLUMN 1 - Job ID
            out.print(StringUtils.rightPad(job.getId(), idPad));
            out.print(SEP);

            // COLUMN 2 - Job Status
            out.print(StringUtils.rightPad(job.getInternal().getStatus().getName(), STATUS_PAD));
            out.print(SEP);

            // COLUMN 3 - Creation Date
            out.print(SIMPLE_DATE_FORMAT.format(TimeUtils.toDate(job.getCreationDate())));
            out.print(SEP);

            // COLUMN 4 - Duration
            if (durationInMillis > 0) {
                out.print(StringUtils.rightPad(TimeUtils.durationToStringSimple(durationInMillis), DURATION_PAD));
            } else {
                out.print(StringUtils.repeat(" ", DURATION_PAD));
            }
            out.print(SEP);

            // COLUMN 5 - Start Date
            if (start == null) {
                out.print(StringUtils.repeat(" ", DATE_PAD));
            } else {
                out.print(SIMPLE_DATE_FORMAT.format(start));
            }
            out.print(SEP);

            // COLUMN 6 - End Date
            if (end == null) {
                out.print(StringUtils.repeat(" ", DATE_PAD));
            } else {
                out.print(SIMPLE_DATE_FORMAT.format(end));
            }
            out.println();
        }

        if (truncatedJobs) {
            out.print("...... skip ");
            if (running.getNumMatches() != running.getNumResults()) {
                out.print((running.getNumMatches() - running.getNumResults()) + " running jobs, ");
            }
            if (queued.getNumMatches() != queued.getNumResults()) {
                out.print((queued.getNumMatches() - queued.getNumResults()) + " queued jobs, ");
            }
            if (pending.getNumMatches() != pending.getNumResults()) {
                out.print((pending.getNumMatches() - pending.getNumResults()) + " pending jobs");
            }
            out.println();
        }

        out.flush();
        System.out.print(buffer);
    }

    public long getDurationInMillis(Date start, Date end) {
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
