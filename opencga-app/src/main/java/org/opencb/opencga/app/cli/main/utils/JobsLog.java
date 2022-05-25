/*
 * Copyright 2015-2020 OpenCB
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

package org.opencb.opencga.app.cli.main.utils;

import com.google.common.base.Stopwatch;
import org.opencb.commons.datastore.core.ObjectMap;
import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.opencga.app.cli.main.options.JobsCommandOptions;
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
    private static final int BATCH_SIZE = ParamConstants.MAXIMUM_LINES_CONTENT;

    private final Logger logger = LoggerFactory.getLogger(this.getClass().toString());
    private final OpenCGAClient openCGAClient;
    private final JobsCommandOptions.LogCommandOptions logCommandOptions;
    private final Map<String, FileContent> jobs = new HashMap<>();
    private final Map<String, AtomicInteger> printedLines = new HashMap<>();
    private final ObjectMap params;
    private final int maxLines;
    private final PrintStream out;
    private final boolean logAllRunningJobs;
    private final boolean logMultipleJobs;
    private String lastFile = null;
    public static final int MAX_ERRORS = 3;

    public JobsLog(OpenCGAClient openCGAClient, JobsCommandOptions.LogCommandOptions logCommandOptions, PrintStream out) {
        this.openCGAClient = openCGAClient;
        this.logCommandOptions = logCommandOptions;
        this.out = out;
        this.openCGAClient.setThrowExceptionOnError(true);

        params = new ObjectMap(ParamConstants.STUDY_PARAM, logCommandOptions.study)
                .append("type", logCommandOptions.type);

        if (logCommandOptions.follow || logCommandOptions.tailLines == null || logCommandOptions.tailLines < 0) {
            maxLines = Integer.MAX_VALUE;
        } else {
            maxLines = logCommandOptions.tailLines;
        }

        logAllRunningJobs = logCommandOptions.job.equalsIgnoreCase(RUNNING);
        logMultipleJobs = logAllRunningJobs || logCommandOptions.job.contains(",");
    }

    public void run() throws ClientException, InterruptedException {
        if (logMultipleJobs) {
            openCGAClient.getJobClient()
                    .search(new ObjectMap(ParamConstants.STUDY_PARAM, logCommandOptions.study)
                            .append(JobDBAdaptor.QueryParams.INTERNAL_STATUS_ID.key(), RUNNING)
                            .append(QueryOptions.INCLUDE, "id"))
                    .allResults()
                    .forEach(job -> jobs.put(job.getId(), null));
        } else {
            for (String job : logCommandOptions.job.split(",")) {
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
                    secureOp(() -> openCGAClient.getJobClient()
                            .search(new ObjectMap(ParamConstants.STUDY_PARAM, logCommandOptions.study)
                                    .append(JobDBAdaptor.QueryParams.INTERNAL_STATUS_ID.key(), RUNNING)
                                    .append(QueryOptions.INCLUDE, "id"))
                            .allResults()
                            .forEach(job -> jobs.putIfAbsent(job.getId(), null)));
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
                Thread.sleep(TimeUnit.SECONDS.toMillis(logCommandOptions.delay));
            }
        }
    }


    private boolean jobLogLoop(String jobId) throws ClientException {
        boolean eof = true;
        AtomicInteger printedLines = this.printedLines.computeIfAbsent(jobId, k -> new AtomicInteger());
        FileContent content = jobs.get(jobId);
        while (logCommandOptions.follow || printedLines.get() < maxLines) {
            ObjectMap params = new ObjectMap(this.params);
            if (content == null) {
                // First file content
                if (logCommandOptions.tailLines == null || logCommandOptions.tailLines < 0) {
                    // Undefined tail. Print all.
                    params.append("lines", BATCH_SIZE);
                    content = secureOp(() -> openCGAClient.getJobClient().headLog(jobId, params).firstResult());
                } else {
                    params.append("lines", logCommandOptions.tailLines);
                    content = secureOp(() -> openCGAClient.getJobClient().tailLog(jobId, params).firstResult());
                }
            } else {
                params.put("lines", Math.min(maxLines - printedLines.get(), BATCH_SIZE));
                params.put("offset", content.getOffset());
                content = secureOp(() -> openCGAClient.getJobClient().headLog(jobId, params).firstResult());
            }
            jobs.put(jobId, content);
            printedLines.addAndGet(printContent(content));

            // Read fewer lines than expected. Check EOF
            if (content.getLines() < params.getInt("lines")) {
                if (logCommandOptions.follow) {
                    // Check job status
                    Job job = secureOp(() -> openCGAClient.getJobClient().info(jobId, new ObjectMap(ParamConstants.STUDY_PARAM, logCommandOptions.study)).firstResult());
                    if (job.getInternal().getStatus().getId().equals(RUNNING)) {
                        // The job is still running. eof=false and break
                        eof = false;
                        break;
                    } else {
                        // If the job is not running. Trust the content.eof
                        eof = content.isEof();
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

    interface Op<R> {
        R apply() throws ClientException;
    }

    interface OpConsumer {
        void apply() throws ClientException;

        default Op<Void> toOp() {
            return () -> {
                apply();
                return null;
            };
        }
    }

    private void secureOp(OpConsumer op) throws ClientException {
        secureOp(op.toOp());
    }

    private <T> T secureOp(Op<T> op) throws ClientException {
        int errors = 0;
        while (true) {
            try {
                return op.apply();
            } catch (Exception e) {
                errors++;
                if (errors > MAX_ERRORS) {
                    logger.error("Got " + errors + " consecutive errors trying to print Jobs Log");
                    throw e;
                }
            }
        }
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
