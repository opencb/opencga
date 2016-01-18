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

    private final List POISON_PILL = new LinkedList();
    private final BlockingQueue<List> readBlockingQueue;
    private final BlockingQueue<List> writeBlockingQueue;
    private final int batchSize;
    private final int capacity;
    private final Integer numTasks;
    private final ExecutorService executorService;
    private final DataReader reader;
    private final List<DataWriter> writers;
    private final List<Task> tasks;
    protected Logger logger = LoggerFactory.getLogger(SimpleThreadRunner.class);

    public SimpleThreadRunner(DataReader reader, List<Task> tasks, DataWriter writer, int batchSize, int capacity, Integer numTasks) {
        this(reader, tasks, Collections.singletonList(writer), batchSize, capacity, numTasks);
    }

    public SimpleThreadRunner(DataReader reader, List<Task> tasks, List<DataWriter> writers, int batchSize, int capacity, Integer
            numTasks) {
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
        } catch (InterruptedException e) {
            e.printStackTrace();
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

        private final DataReader dataReader;

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
//                    System.out.println("reader: prePut readBlockingQueue " + readBlockingQueue.size());
                    readBlockingQueue.put(batch);
//                    System.out.println("reader: postPut");
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
//                System.out.println("reader: preRead");
                batch = dataReader.read(batchSize);
//                System.out.println("reader: batch.size = " + batch.size());
            }
            try {
                logger.debug("reader: POISON_PILL");
                readBlockingQueue.put(POISON_PILL);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
    }

    class TaskRunnable implements Runnable {

        private long timeBlockedAtSendWrite;
        private long timeTaskApply;

        private final List<Task> tasks;

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
                e.printStackTrace();
            }
            while (!batch.isEmpty()) {
                try {
                    long s;
//                        System.out.println("task: apply");
                    s = System.nanoTime();
                    for (Task task : tasks) {
                        task.apply(batch);
                    }
                    timeTaskApply += s - System.nanoTime();
//                    System.out.println("task: apply done " + writeBlockingQueue.size());

                    s = System.nanoTime();
                    writeBlockingQueue.put(batch);
//                    System.out.println("task: apply done");
                    timeBlockedAtSendWrite += s - System.nanoTime();
                    batch = getBatch();
                } catch (InterruptedException | IOException e) {
                    e.printStackTrace();
                }
            }
            synchronized (numTasks) {
                this.timeBlockedAtSendWrite += timeBlockedAtSendWrite;
                this.timeTaskApply += timeTaskApply;
                finishedTasks++;
                if (numTasks == finishedTasks) {
                    logger.debug("task; timeBlockedAtSendWrite = " + timeBlockedAtSendWrite / -1000000000.0 + "s");
                    logger.debug("task; timeTaskApply = " + timeTaskApply / -1000000000.0 + "s");
                    try {
                        writeBlockingQueue.put(POISON_PILL);
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }
                }
            }


        }

        private int finishedTasks = 0;

        private List<String> getBatch() throws InterruptedException {
            List<String> batch;
            batch = readBlockingQueue.take();
//                System.out.println("task: readBlockingQueue = " + readBlockingQueue.size() + " batch.size : " + batch.size() + " : " +
// batchSize);
            if (batch == POISON_PILL) {
                logger.debug("task: POISON_PILL");
                readBlockingQueue.put(POISON_PILL);
            }
            return batch;
        }
    }

    class WriterRunnable implements Runnable {

        private long timeBlockedWatingDataToWrite = 0;
        private final DataWriter dataWriter;

        WriterRunnable(DataWriter dataWriter) {
            this.dataWriter = dataWriter;
        }

        @Override
        public void run() {
            List<String> batch = new ArrayList<>(batchSize);
            try {
                batch = getBatch();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
            long s, timeWriting = 0;
//            while (!batch.isEmpty()) {
            while (batch != POISON_PILL) {
                try {
                    s = System.nanoTime();
//                    System.out.println("writer: write");
                    dataWriter.write(batch);
//                    System.out.println("writer: wrote");
                    timeWriting += s - System.nanoTime();
                    batch = getBatch();
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
            logger.debug("write: timeWriting = " + timeWriting / -1000000000.0 + "s");
            logger.debug("write: timeBlockedWatingDataToWrite = " + timeBlockedWatingDataToWrite / -1000000000.0 + "s");
        }

        private List<String> getBatch() throws InterruptedException {
            List<String> batch;
//                System.out.println("writer: writeBlockingQueue = " + writeBlockingQueue.size());

            long s = System.nanoTime();
            batch = writeBlockingQueue.take();
            timeBlockedWatingDataToWrite += s - System.nanoTime();
            if (batch == POISON_PILL) {
                logger.debug("writer: POISON_PILL");
                writeBlockingQueue.put(POISON_PILL);
            }
            return batch;
        }
    }

}
