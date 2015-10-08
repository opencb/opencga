package org.opencb.opencga.storage.core.runner;

import org.opencb.commons.io.DataReader;
import org.opencb.commons.io.DataWriter;
import org.opencb.commons.run.Task;
import org.opencb.opencga.lib.common.ExceptionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.*;

/**
 * Created by hpccoll1 on 26/02/15.
 */
public class SimpleThreadRunner {

    final List POISON_PILL = new LinkedList();
    final BlockingQueue<List> readBlockingQueue;
    final BlockingQueue<List> writeBlockingQueue;
    final int batchSize;
    final int capacity;
    final Integer numTasks;
    private final ExecutorService executorService;
    private final DataReader reader;
    private final List<DataWriter> writers;
    private final List<Task> tasks;
    protected static Logger logger = LoggerFactory.getLogger(SimpleThreadRunner.class);

    public SimpleThreadRunner(DataReader reader, List<Task> tasks, DataWriter writer, int batchSize, int capacity, Integer numTasks) {
        this(reader, tasks, Collections.singletonList(writer), batchSize, capacity, numTasks);
    }
    public SimpleThreadRunner(DataReader reader, List<Task> tasks, List<DataWriter> writers, int batchSize, int capacity, Integer numTasks) {
        this.batchSize = batchSize;
        this.capacity = capacity;
        this.reader = reader;
        this.writers = writers;
        this.tasks = tasks;
        readBlockingQueue = new ArrayBlockingQueue<>(capacity);
        if (tasks.isEmpty()) {
            this.numTasks = 0;
            writeBlockingQueue = readBlockingQueue;
        } else {
            this.numTasks = numTasks;
            writeBlockingQueue = new ArrayBlockingQueue<>(capacity);
        }

        executorService = Executors.newFixedThreadPool(numTasks + 1 + writers.size());
    }
    public void run() throws Exception {
        reader.open();
        reader.pre();

        for (DataWriter writer : writers) {
            writer.open();
        }
        for (DataWriter writer : writers) {
            writer.pre();
        }

        for (Task task : tasks) {
            task.pre();
        }

        List<Future> futures = new ArrayList<>(1 + numTasks + writers.size());
        futures.add(executorService.submit(new ReaderCallable(reader)));
        TaskCallable taskCallable = new TaskCallable(this.tasks);
        for (Integer i = 0; i < numTasks; i++) {
            futures.add(executorService.submit(taskCallable));
        }
        for (DataWriter writer : writers) {
            futures.add(executorService.submit(new WriterCallable(writer)));
        }

        try {
            for (Future future : futures) {
                future.get();   // this will force the retrieval of the exceptions thrown by the callables
            }
            executorService.shutdown();
            executorService.awaitTermination(Long.MAX_VALUE, TimeUnit.DAYS);
        } catch (Exception runnerFailed) {
            logger.error("caught executorService Exception:", runnerFailed);
            executorService.shutdownNow();
            throw runnerFailed;
        } finally {
            try {
                postAndclose(); // closing resources even if the loop was abruptly stopped
            } catch (Exception closeFailed) { // ignoring close failures, the important exception is "why the runnerFailed"
                logger.warn("ignoring exception thrown by resources closing: ", closeFailed);
            }
        }
    }

    private void postAndclose() {
        logger.debug("starting resources closing");
        for (Task task : tasks) {
            task.post();
        }

        reader.post();
        reader.close();

        for (DataWriter writer : writers) {
            writer.post();
        }
        for (DataWriter writer : writers) {
            writer.close();
        }
        logger.debug("ending resources closing");
    }

    class ReaderCallable implements Callable<Void> {

        final DataReader dataReader;

        ReaderCallable(DataReader dataReader) {
            this.dataReader = dataReader;
        }

        @Override
        public Void call() throws Exception {
            List<String> batch;
//            System.out.println("reader: init");
            batch = dataReader.read(batchSize);
//            System.out.println("reader: batch.size = " + batch.size());

            while (batch != null && !batch.isEmpty()) {
                logger.trace("reader: prePut readBlockingQueue.size: " + readBlockingQueue.size());
                readBlockingQueue.put(batch);
                logger.trace("reader: postPut, readqueue.size: " + readBlockingQueue.size());
//                System.out.println("reader: preRead");
                batch = dataReader.read(batchSize);
//                System.out.println("reader: batch.size = " + batch.size());
            }
//                logger.debug("reader: putting POISON_PILL");
//                readBlockingQueue.put(POISON_PILL);
            logger.debug("reader: putting POISON_PILL");
            readBlockingQueue.put(POISON_PILL);
            return null;
        }
    }

    class TaskCallable implements Callable<Void> {

        private long timeBlockedAtSendWrite;
        private long timeTaskApply;

        final List<Task> tasks;

        TaskCallable(List<Task> tasks) {
            this.tasks = tasks;
        }


        @Override
        public Void call() throws Exception {
            List<String> batch = new ArrayList<>(batchSize);
            long timeBlockedAtSendWrite = 0;
            long timeTaskApply = 0;
            batch = getBatch();
            while (!batch.isEmpty()) {
                long s;
                s = System.nanoTime();
                for (Task task : tasks) {
                    task.apply(batch);
                }
                timeTaskApply += s - System.nanoTime();
//                    logger.trace("task: apply done, pre put, writeBlockingQueue.size: " + writeBlockingQueue.size());

                s = System.nanoTime();
                writeBlockingQueue.put(batch);
                logger.trace("task: apply done (post put) writeQueue.size: " + writeBlockingQueue.size());
                timeBlockedAtSendWrite += s - System.nanoTime();
                batch = getBatch();
            } synchronized (numTasks) {
                this.timeBlockedAtSendWrite += timeBlockedAtSendWrite;
                this.timeTaskApply += timeTaskApply;
                finishedTasks++;
                if (numTasks == finishedTasks) {
                    logger.debug("task; timeBlockedAtSendWrite = " + timeBlockedAtSendWrite / -1000000000.0 + "s");
                    logger.debug("task; timeTaskApply = " + timeTaskApply / -1000000000.0 + "s");
                    logger.trace("task: putting POISON PILL");
                    writeBlockingQueue.put(POISON_PILL);
                }
            }

            logger.trace("task: leaving run(). sizes: read: " + readBlockingQueue.size() + ", write: " + writeBlockingQueue.size());
            return null;
        }
        private int finishedTasks = 0;
        private List<String> getBatch() throws InterruptedException {
            List<String> batch;
            batch = readBlockingQueue.take();
            logger.trace("task: taken batch, readBlockingQueue.size = " + readBlockingQueue.size() + " batch.size : " + batch.size() + " : " + batchSize);
            if (batch == POISON_PILL) {
                logger.debug("task: received POISON_PILL");
                readBlockingQueue.put(POISON_PILL);
            }
            return batch;
        }
    }

    class WriterCallable implements Callable<Void> {

        long timeBlockedWatingDataToWrite = 0;
        final DataWriter dataWriter;

        WriterCallable(DataWriter dataWriter) {
            this.dataWriter = dataWriter;
        }

        @Override
        public Void call() throws Exception {
            List<String> batch;
            long s, timeWriting = 0;
            batch = getBatch();
            while (batch != POISON_PILL) {
                s = System.nanoTime();
                logger.trace("writer: writing...");
                dataWriter.write(batch);
                logger.trace("writer: wrote");
                timeWriting += s - System.nanoTime();
                batch = getBatch();
            }
            logger.debug("write: timeWriting = " + timeWriting / -1000000000.0 + "s");
            logger.debug("write: timeBlockedWatingDataToWrite = " + timeBlockedWatingDataToWrite / -1000000000.0 + "s");
            return null;
        }

        private List<String> getBatch() throws InterruptedException {
            List<String> batch;
            logger.trace("writer: about to block, writeBlockingQueue.size() = " + writeBlockingQueue.size());

            long s = System.nanoTime();
            batch = writeBlockingQueue.take();
            logger.trace("writer: received batch, writing...");
            timeBlockedWatingDataToWrite += s - System.nanoTime();
            if (batch == POISON_PILL) {
                logger.debug("writer: POISON_PILL");
                writeBlockingQueue.put(POISON_PILL);
            }
            return batch;
        }
    }

}
