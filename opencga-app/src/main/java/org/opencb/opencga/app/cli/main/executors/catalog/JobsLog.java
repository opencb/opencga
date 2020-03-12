package org.opencb.opencga.app.cli.main.executors.catalog;

import com.google.common.base.Stopwatch;
import org.opencb.commons.datastore.core.ObjectMap;
import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.opencga.app.cli.main.options.JobCommandOptions;
import org.opencb.opencga.catalog.db.api.JobDBAdaptor;
import org.opencb.opencga.client.exceptions.ClientException;
import org.opencb.opencga.client.rest.OpenCGAClient;
import org.opencb.opencga.core.api.ParamConstants;
import org.opencb.opencga.core.models.file.FileContent;
import org.opencb.opencga.core.models.job.Job;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.PrintStream;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.opencb.opencga.core.models.common.Enums.ExecutionStatus.RUNNING;

public class JobsLog {
    private static final int BATCH_SIZE = 500;

    private final Logger logger = LoggerFactory.getLogger(this.getClass().toString());
    private final OpenCGAClient openCGAClient;
    private final JobCommandOptions.LogCommandOptions c;
    private final Map<String, FileContent> jobs = new HashMap<>();
    private final Map<String, AtomicInteger> printedLines = new HashMap<>();
    private final ObjectMap params;
    private final int maxLines;
    private final PrintStream out;
    private final boolean logAllRunningJobs;
    private final boolean logMultipleJobs;
    private String lastFile = null;

    public JobsLog(OpenCGAClient openCGAClient, JobCommandOptions.LogCommandOptions c, PrintStream out) {
        this.openCGAClient = openCGAClient;
        this.c = c;
        this.out = out;
        this.openCGAClient.setThrowExceptionOnError(true);

        params = new ObjectMap(ParamConstants.STUDY_PARAM, c.study).append("type", c.type);

        if (c.follow || c.tailLines == null || c.tailLines < 0) {
            maxLines = Integer.MAX_VALUE;
        } else {
            maxLines = c.tailLines;
        }

        logAllRunningJobs = c.job.equalsIgnoreCase(RUNNING);
        logMultipleJobs = logAllRunningJobs || c.job.contains(",");
    }

    public void run() throws ClientException, InterruptedException {
        if (logMultipleJobs) {
            openCGAClient.getJobClient()
                    .search(new ObjectMap(ParamConstants.STUDY_PARAM, c.study)
                            .append(JobDBAdaptor.QueryParams.INTERNAL_STATUS_NAME.key(), RUNNING)
                            .append(QueryOptions.INCLUDE, "id"))
                    .allResults()
                    .forEach(job -> jobs.put(job.getId(), null));
        } else {
            for (String job : c.job.split(",")) {
                jobs.put(job, null);
            }
        }

        Stopwatch timer = Stopwatch.createStarted();
        while (!jobs.isEmpty()) {
            if (timer.elapsed(TimeUnit.MINUTES) > 5) {
                openCGAClient.refresh();
                timer.reset().start();
            }

            Iterator<String> iterator = jobs.keySet().iterator();
            while (iterator.hasNext()) {
                String job = iterator.next();
                boolean eof = jobLogLoop(job);
                if (eof) {
                    iterator.remove();
                    printedLines.remove(job);
                }
            }
            if (logAllRunningJobs) {
                final int waitForNewJobsSecs = 60; // Seconds to wait to find new jobs, if list is empty
                int i = 0;

                // Update list of running jobs
                do {
                    openCGAClient.getJobClient()
                            .search(new ObjectMap(ParamConstants.STUDY_PARAM, c.study)
                                    .append(JobDBAdaptor.QueryParams.INTERNAL_STATUS_NAME.key(), RUNNING)
                                    .append(QueryOptions.INCLUDE, "id"))
                            .allResults()
                            .forEach(job -> jobs.putIfAbsent(job.getId(), null));
                    i++;
                    if (jobs.isEmpty()) {
                        // Sleep if there are jobs left.
                        logger.debug("Sleep");
                        Thread.sleep(TimeUnit.SECONDS.toMillis(1));
                    }
                } while (jobs.isEmpty() && i < waitForNewJobsSecs);
            }
            if (!jobs.isEmpty()) {
                // Sleep if there are jobs left.
                logger.debug("Sleep");
                Thread.sleep(TimeUnit.SECONDS.toMillis(c.delay));
            }
        }
    }


    private boolean jobLogLoop(String jobId) throws ClientException {
        boolean eof = true;
        AtomicInteger printedLines = this.printedLines.computeIfAbsent(jobId, k -> new AtomicInteger());
        FileContent content = jobs.get(jobId);
        while (c.follow || printedLines.get() < maxLines) {
            ObjectMap params = new ObjectMap(this.params);
            if (content == null) {
                // First file content
                if (c.tailLines == null || c.tailLines < 0) {
                    // Undefined tail. Print all.
                    params.append("tail", false).append("lines", BATCH_SIZE);
                } else {
                    params.append("tail", true).append("lines", c.tailLines);
                }
            } else {
                params.put("lines", Math.min(maxLines - printedLines.get(), BATCH_SIZE));
                params.put("tail", false); // Only use tail for the first batch
                params.put("offset", content.getOffset());
            }
            content = openCGAClient.getJobClient().log(jobId, params).firstResult();
            jobs.put(jobId, content);
            printedLines.addAndGet(printContent(content));

            // Read fewer lines than expected
            if (content.getLines() < params.getInt("lines")) {
                if (c.follow) {
                    // Check job status
                    Job job = openCGAClient.getJobClient().info(jobId, new ObjectMap(ParamConstants.STUDY_PARAM, c.study)).firstResult();
                    if (job.getInternal().getStatus().getName().equals(RUNNING)) {
                        // The job is still running. eof=false and break
                        eof = false;
                        break;
                    } else {
                        // If the job is not running, skip sleep and break loop
                        eof = true;
                        break;
                    }
                } else {
                    // End of file
                    if (content.isEof()) {
                        eof = true;
                        break;
                    }
                }
            }
        }
        return eof;
    }

    private int printContent(FileContent content) {
        if (!content.getContent().isEmpty()) {
            if (logMultipleJobs) {
                String fileId = content.getFileId().substring(content.getFileId().lastIndexOf("/") + 1);
                if (!fileId.equals(lastFile)) {
                    out.println();
                    out.println("==> " + fileId + " <==");
                }
                lastFile = fileId;
            }
            out.print(content.getContent());
            if (!content.getContent().endsWith("\n")) {
                out.println();
            }
            out.flush();
        }
        return content.getLines();
    }

}
