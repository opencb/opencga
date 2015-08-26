/*
 * Copyright 2015 OpenCB
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

package org.opencb.opencga.storage.core.runner;

import org.opencb.commons.io.DataReader;
import org.opencb.commons.io.DataWriter;
import org.opencb.commons.run.Task;
import org.opencb.opencga.core.common.ExceptionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.*;

/**
 * @author Jacobo Coll <jacobo167@gmail.com>
 */
@Deprecated
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
    public void run() {
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

        executorService.submit(new ReaderRunnable(reader));
        TaskRunnable taskRunnable = new TaskRunnable(this.tasks);
        for (Integer i = 0; i < numTasks; i++) {
            executorService.submit(taskRunnable);
        }
        for (DataWriter writer : writers) {
            executorService.submit(new WriterRunnable(writer));
        }

        executorService.shutdown();
        try {
            executorService.awaitTermination(Long.MAX_VALUE, TimeUnit.DAYS);
        } catch (Exception e) {
            logger.error(ExceptionUtils.getExceptionString(e));
        }

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

    }

    class ReaderRunnable implements Runnable {

        final DataReader dataReader;

        ReaderRunnable(DataReader dataReader) {
            this.dataReader = dataReader;
        }

        @Override
        public void run() {
            List<String> batch;
//            System.out.println("reader: init");
            batch = dataReader.read(batchSize);
//            System.out.println("reader: batch.size = " + batch.size());

            while (batch != null && !batch.isEmpty()) {
                try {
                    logger.trace("reader: prePut readBlockingQueue.size: " + readBlockingQueue.size());
                    readBlockingQueue.put(batch);
                    logger.trace("reader: postPut, readqueue.size: " + readBlockingQueue.size());
                } catch (InterruptedException e) {
                    logger.error(ExceptionUtils.getExceptionString(e));
                }
//                System.out.println("reader: preRead");
                batch = dataReader.read(batchSize);
//                System.out.println("reader: batch.size = " + batch.size());
            }
            try {
                logger.debug("reader: putting POISON_PILL");
                readBlockingQueue.put(POISON_PILL);
            } catch (InterruptedException e) {
                logger.error(ExceptionUtils.getExceptionString(e));
            } catch (Exception e) {
                logger.error("UNEXPECTED ENDING of a reader thread due to an error:\n" + ExceptionUtils.getExceptionString(e));
            }
        }
    }

    class TaskRunnable implements Runnable {

        private long timeBlockedAtSendWrite;
        private long timeTaskApply;

        final List<Task> tasks;

        TaskRunnable(List<Task> tasks) {
            this.tasks = tasks;
        }


        @Override
        public void run() {
            List<String> batch = new ArrayList<>(batchSize);
            long timeBlockedAtSendWrite = 0;
            long timeTaskApply = 0;
            try {
                batch = getBatch();
            } catch (InterruptedException e) {
                logger.error(ExceptionUtils.getExceptionString(e));
            }
            while (!batch.isEmpty()) {
                try {
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
                } catch (InterruptedException | IOException e) {
                    logger.error(ExceptionUtils.getExceptionString(e));
                } catch (Exception e) {
                    logger.error("UNEXPECTED ENDING of a task thread due to an error:\n" + ExceptionUtils.getExceptionString(e));
                    return;
                }
            }
            synchronized (numTasks) {
                this.timeBlockedAtSendWrite += timeBlockedAtSendWrite;
                this.timeTaskApply += timeTaskApply;
                finishedTasks++;
                if (numTasks == finishedTasks) {
                    logger.debug("task; timeBlockedAtSendWrite = " + timeBlockedAtSendWrite / -1000000000.0 + "s");
                    logger.debug("task; timeTaskApply = " + timeTaskApply / -1000000000.0 + "s");
                    logger.trace("task: putting POISON PILL");
                    try {
                        writeBlockingQueue.put(POISON_PILL);
                    } catch (InterruptedException e) {
                        logger.error(ExceptionUtils.getExceptionString(e));
                    }
                }
            }

            logger.trace("task: leaving run(). sizes: read: " + readBlockingQueue.size() + ", write: " + writeBlockingQueue.size());

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

    class WriterRunnable implements Runnable {

        long timeBlockedWatingDataToWrite = 0;
        final DataWriter dataWriter;

        WriterRunnable(DataWriter dataWriter) {
            this.dataWriter = dataWriter;
        }

        @Override
        public void run() {
            List<String> batch = new ArrayList<>(batchSize);
            try {
                batch = getBatch();
            } catch (InterruptedException e) {
                logger.error(ExceptionUtils.getExceptionString(e));
            }
            long s, timeWriting = 0;
//            while (!batch.isEmpty()) {
            while (batch != POISON_PILL) {
                try {
                    s = System.nanoTime();
                    logger.trace("writer: writing...");
                    dataWriter.write(batch);
                    logger.trace("writer: wrote");
                    timeWriting += s - System.nanoTime();
                    batch = getBatch();
                } catch (InterruptedException e) {
                    logger.error(ExceptionUtils.getExceptionString(e));
                } catch (Exception e) {
                    logger.error("UNEXPECTED ENDING of a writer thread due to an error:\n" + ExceptionUtils.getExceptionString(e));
                }
            }
            logger.debug("write: timeWriting = " + timeWriting / -1000000000.0 + "s");
            logger.debug("write: timeBlockedWatingDataToWrite = " + timeBlockedWatingDataToWrite / -1000000000.0 + "s");
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
