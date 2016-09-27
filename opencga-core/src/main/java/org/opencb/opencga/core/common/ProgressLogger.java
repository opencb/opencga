package org.opencb.opencga.core.common;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.text.DecimalFormat;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Supplier;

/**
 * Created on 13/04/16
 *
 * @author Jacobo Coll &lt;jacobo167@gmail.com&gt;
 */
public class ProgressLogger {

    private static final int DEFAULT_BATCH_SIZE = 5000;
    private static final int MIN_BATCH_SIZE = 200;
    private static final DecimalFormat DECIMAL_FORMAT = new DecimalFormat("#%");

    private final String message;
    private final int numLinesLog;
    private long totalCount;
    private Future<Long> futureTotalCount;
    private final AtomicLong count;

    private int batchSize;

    private Logger logger = LoggerFactory.getLogger(ProgressLogger.class);

    public ProgressLogger(String message) {
        this(message, 0, null, 200);
    }

    public ProgressLogger(String message, long totalCount) {
        this(message, totalCount, null, 200);
    }

    public ProgressLogger(String message, long totalCount, int numLinesLog) {
        this(message, totalCount, null, numLinesLog);
    }

    public ProgressLogger(String message, Future<Long> futureTotalCount) {
        this(message, 0, futureTotalCount, 200);
    }

    public ProgressLogger(String message, Future<Long> futureTotalCount, int numLinesLog) {
        this(message, 0, futureTotalCount, numLinesLog);
    }

    private ProgressLogger(String message, long totalCount, Future<Long> futureTotalCount, int numLinesLog) {
        if (message.endsWith(" ")) {
            this.message = message;
        } else {
            this.message = message + " ";
        }
        this.numLinesLog = numLinesLog;
        this.totalCount = totalCount;
        this.futureTotalCount = futureTotalCount;
        this.count = new AtomicLong();
        if (totalCount == 0) {
            batchSize = DEFAULT_BATCH_SIZE;
        } else {
            batchSize = (int) Math.max(totalCount / numLinesLog, MIN_BATCH_SIZE);
        }
    }

    public ProgressLogger setLogger(Logger logger) {
        this.logger = logger;
        return this;
    }

    public ProgressLogger setBatchSize(int batchSize) {
        this.batchSize = batchSize;
        return this;
    }

    public void increment(long delta) {
        increment(delta, "", null);
    }

    public void increment(long delta, String message) {
        increment(delta, message, null);
    }

    public void increment(long delta, Supplier<String> supplier) {
        increment(delta, null, supplier);
    }

    private void increment(long delta, String message, Supplier<String> supplier) {
        long previousCount = count.addAndGet(delta);
        long count = previousCount + delta;

        if (previousCount / batchSize != count / batchSize) {
            log(count, supplier == null ? message : supplier.get());
        }

    }

    protected void log(long count, String extraMessage) {
        long totalCount = getTotalCount();
        String space;
        if (extraMessage.isEmpty() || (extraMessage.startsWith(" ") && extraMessage.startsWith(",") && extraMessage.startsWith("."))) {
            space = "";
        } else {
            space = " ";
        }
        if (totalCount <= 0) {
            print(message + (count) + space + extraMessage);
        } else {
            print(message + (count) + "/" + totalCount + " " + DECIMAL_FORMAT.format(((float) (count)) / totalCount) + space + extraMessage);
        }
    }

    protected void print(String m) {
        logger.info(m);
    }

    private long getTotalCount() {
        if (futureTotalCount != null) {
            try {
                if (futureTotalCount.isDone()) {
                    totalCount = futureTotalCount.get();
                    batchSize = (int) Math.max(totalCount / numLinesLog, MIN_BATCH_SIZE);
                }
            } catch (InterruptedException | ExecutionException ignore) {
                logger.warn("There was a problem calculating the total number of elements");
            } finally {
                futureTotalCount = null;
            }
        }
        return this.totalCount;
    }


}
