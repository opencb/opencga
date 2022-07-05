package org.opencb.opencga.app.cli.main.utils;

import com.google.common.base.Stopwatch;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.opencb.commons.datastore.core.Query;
import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.opencga.app.cli.main.io.Table;
import org.opencb.opencga.app.cli.main.io.TextOutputWriter;
import org.opencb.opencga.catalog.db.api.ExecutionDBAdaptor;
import org.opencb.opencga.client.exceptions.ClientException;
import org.opencb.opencga.client.rest.OpenCGAClient;
import org.opencb.opencga.core.common.GitRepositoryState;
import org.opencb.opencga.core.models.common.Enums;
import org.opencb.opencga.core.models.job.Execution;
import org.opencb.opencga.core.models.job.ExecutionInternal;
import org.opencb.opencga.core.models.job.ExecutionTop;
import org.opencb.opencga.core.models.job.Job;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.time.Instant;
import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import static org.opencb.opencga.app.cli.main.io.TextOutputWriter.ExecutionColumns.*;

public class ExecutionsTopManager {

    public static final int MAX_ERRORS = 4;
    private final Logger logger = LoggerFactory.getLogger(this.getClass());

    private final OpenCGAClient openCGAClient;
    private final Query baseQuery;
    private final int iterations;
    private final int executionsLimit;
    private final long delay;
    private final boolean plain;

    // FIXME: Use an intermediate buffer to prepare the table, and print in one system call to avoid flashes
    private final ByteArrayOutputStream buffer;
    private final Table<Execution> executionTable;
    private PrintStream bufferStream;

    public ExecutionsTopManager(OpenCGAClient openCGAClient, Query query, Integer iterations, Integer executionsLimit, long delay, boolean plain) {
        this(openCGAClient, query, iterations, executionsLimit, delay, plain, parseColumns(null));
    }

    public ExecutionsTopManager(OpenCGAClient openCGAClient, Query query, Integer iterations, Integer executionsLimit, long delay, boolean plain, String columns) {
        this(openCGAClient, query, iterations, executionsLimit, delay, plain, parseColumns(columns));
    }

    public ExecutionsTopManager(OpenCGAClient openCGAClient, Query query, Integer iterations, Integer executionsLimit, long delay, boolean plain, List<TextOutputWriter.ExecutionColumns> columns) {
        this.openCGAClient = openCGAClient;
        this.baseQuery = new Query(query)
                .append(QueryOptions.SORT, ExecutionDBAdaptor.QueryParams.CREATION_DATE.key())
                .append(QueryOptions.ORDER, QueryOptions.DESCENDING);
        this.buffer = new ByteArrayOutputStream();
        this.iterations = iterations == null || iterations <= 0 ? -1 : iterations;
        if (executionsLimit == null || executionsLimit <= 0) {
            String lines = System.getenv("LINES");
            if (StringUtils.isNumeric(lines)) {
                int HEADER_SIZE = 9;
                this.executionsLimit = Integer.parseInt(lines) - HEADER_SIZE;
            } else {
                this.executionsLimit = 20;
            }
        } else {
            this.executionsLimit = executionsLimit;
        }
        this.delay = delay < 0 ? 2 : delay;
        this.plain = plain;

        buffer.reset();
        bufferStream = new PrintStream(buffer);

        Table.TablePrinter tablePrinter = new Table.JAnsiTablePrinter(bufferStream);
        executionTable = new Table<>(tablePrinter);
        for (TextOutputWriter.ExecutionColumns column : columns) {
            executionTable.addColumn(column.getColumnSchema());
        }
        executionTable.setMultiLine(false);

    }

    private static List<TextOutputWriter.ExecutionColumns> parseColumns(String columnsStr) {
        if (StringUtils.isBlank(columnsStr) || columnsStr.equalsIgnoreCase("default")) {
            return Arrays.asList(ID, DEPENDENCIES, STATUS, STUDY, SUBMISSION, PRIORITY, RUNNING_TIME);
        } else {
            List<TextOutputWriter.ExecutionColumns> columns = new LinkedList<>();
            for (String c : columnsStr.split(",")) {
                columns.add(TextOutputWriter.ExecutionColumns.valueOf(c.toUpperCase()));
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

                print(openCGAClient.getExecutionClient().top(baseQuery).firstResult());

                Thread.sleep(TimeUnit.SECONDS.toMillis(this.delay));
                // Reset errors counter
                errors = 0;
            } catch (InterruptedException e) {
                // Do not ignore InterruptedException!!
                throw e;
            } catch (Exception e) {
                errors++;
                if (errors > MAX_ERRORS) {
                    logger.error("Got " + errors + " consecutive errors trying to print Executions Top");
                    throw e;
                }
            }
        }
    }

    public void print(ExecutionTop top) {
        buffer.reset();
        List<Execution> executionList = processExecutions(top.getExecutions());
        executionTable.updateTable(executionList);

        executionTable.restoreCursorPosition();
        executionTable.println("OpenCGA executions TOP");
        executionTable.println("  Version " + GitRepositoryState.get().getBuildVersion());
        executionTable.println("  " + TextOutputWriter.SIMPLE_DATE_FORMAT.format(Date.from(Instant.now())));
        executionTable.println();
        executionTable.print(Enums.ExecutionStatus.RUNNING + ": " + top.getStats().getRunning() + ", ");
        executionTable.print(Enums.ExecutionStatus.PROCESSED + ": " + top.getStats().getProcessed() + ", ");
        executionTable.print(Enums.ExecutionStatus.QUEUED + ": " + top.getStats().getQueued() + ", ");
        executionTable.print(Enums.ExecutionStatus.PENDING + ": " + top.getStats().getPending() + ", ");
        executionTable.print(Enums.ExecutionStatus.DONE + ": " + top.getStats().getDone() + ", ");
        executionTable.print(Enums.ExecutionStatus.SKIPPED + ": " + top.getStats().getSkipped() + ", ");
        executionTable.print(Enums.ExecutionStatus.ERROR + ": " + top.getStats().getError() + ", ");
        executionTable.print(Enums.ExecutionStatus.ABORTED + ": " + top.getStats().getAborted());
        executionTable.println();
        executionTable.println();
        executionTable.printTable();

        bufferStream.flush();
        System.out.print(buffer);
    }

    private Execution convertJob(Job job) {
        Execution execution = new Execution()
                .setId(job.getTool() != null ? job.getId() + " (" + job.getTool().getId() + ")" : job.getId())
                .setInternal(new ExecutionInternal(job.getInternal().getStatus()))
                .setStudy(job.getStudy())
                .setPriority(job.getPriority())
                .setCreationDate(job.getCreationDate());
        if (job.getResult() != null) {
            execution.getInternal().setStart(job.getResult().getStart());
            execution.getInternal().setEnd(job.getResult().getEnd());
        }
        if (CollectionUtils.isNotEmpty(job.getDependsOn())) {
            List<Execution> dependsOn = new ArrayList<>(job.getDependsOn().size());
            for (Job tmpJob : job.getDependsOn()) {
                dependsOn.add(convertJob(tmpJob));
            }
            execution.setDependsOn(dependsOn);
        }
        return execution;
    }

    private List<Execution> processExecutions(List<Execution> executions) {
        List<Execution> executionList = new LinkedList<>();
        executions.sort(Comparator.comparing(j -> j.getInternal().getStatus().getId().equals(Enums.ExecutionStatus.RUNNING) ? 0 : 1));
        executions = trimExecutions(executions);

        int executionDependsMax = 5;
        for (Execution execution : executions) {
            executionList.add(execution);
            if (CollectionUtils.isNotEmpty(execution.getJobs())) {
                List<Job> jobs = execution.getJobs();
                // executionJobs are actually a list of jobs. We fill in the execution fields that are processed by the table with
                // the corresponding job fields
                List<Execution> executionJobs;
                jobs.removeIf(Objects::isNull);
                if (jobs.size() > executionDependsMax) {
                    int size = jobs.size();
                    TreeMap<String, Integer> byType = jobs
                            .stream()
                            .collect(Collectors.groupingBy(
                                    j -> j.getInternal().getStatus().getId(),
                                    TreeMap::new,
                                    Collectors.summingInt(j -> 1)));
                    int maxStatus = byType.keySet().stream().mapToInt(String::length).max().orElse(0);
                    executionJobs = new ArrayList<>(byType.size());
                    for (Map.Entry<String, Integer> entry : byType.entrySet()) {
                        executionJobs.add(new Execution()
                                .setId(StringUtils.rightPad(entry.getKey(), maxStatus) + " : " + entry.getValue() + "/" + size)
                                .setInternal(new ExecutionInternal(new Enums.ExecutionStatus(entry.getKey()))));
                    }
                } else {
                    executionJobs = new ArrayList<>(jobs.size());
                    for (Job job : jobs) {
                        executionJobs.add(convertJob(job));
                    }
                }
                if (!plain) {
                    for (int i = 0; i < executionJobs.size(); i++) {
                        Execution auxExecution = executionJobs.get(i);
                        if (i + 1 < executionJobs.size()) {
                            auxExecution.setId("├── " + auxExecution.getId());
                        } else {
                            auxExecution.setId("└── " + auxExecution.getId());
                        }
                        executionList.add(auxExecution);
                    }
                }
            }
        }

        executionList = trimExecutions(executionList);

        return executionList;
    }

    private List<Execution> trimExecutions(List<Execution> executions) {
        if (executions.size() <= executionsLimit) {
            return executions;
        }
        List<Execution> executionList = executions.subList(0, executionsLimit);
        for (int i = executionList.size() - 1; i >= 0; i--) {
            if (executionList.get(i).getId().startsWith("└") || (!executionList.get(i).getId().startsWith("├") && (executionList.get(i).getDependsOn() == null || executionList.get(i).getDependsOn().isEmpty()))) {
                break;
            }
            executionList.remove(i);
        }
        return executionList;

    }

}
