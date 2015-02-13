package org.opencb.opencga.storage.core;

import org.opencb.commons.io.DataReader;
import org.opencb.commons.io.DataWriter;
import org.opencb.commons.run.Runner;
import org.opencb.commons.run.Task;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.*;

/**
 * Created by jacobo on 5/02/15.
 */
public class ThreadRunner<T> extends Runner<T> {
    private ExecutorService executor;
    private T lastElement;
    private Logger logger = LoggerFactory.getLogger(ThreadRunner.class);
    private final int numWriters;
    private final int numReaders;
    private final int numTasks;
    private List<BlockingQueue<T>> writersQueues;
//    private List<DataWriter<T>> writers;
    private final List<List<Task<T>>> tasksList;
    private final Set<List<DataWriter<T>>> writersSet;
    private List<BlockingQueue<T>> taskQueues;
    private final List<Future<?>> futures;

    public ThreadRunner(DataReader<T> reader,
                                              Set<List<DataWriter<T>>> writersSet,
                                              List<List<Task<T>>> taskList,
                                              int batchSize, T lastElement) {
        super(reader, null, null, batchSize);
        this.writersSet = writersSet;
        this.tasksList = taskList;
        this.writers = new LinkedList<>();
        this.futures = new LinkedList<>();

        int w = 0;
        for (List<? extends DataWriter<T>> dataWriters : this.writersSet) {
            w+=dataWriters.size();
        }
        int t = 0;
        for (List<? extends Task<T>> tasks : taskList) {
            t += tasks.size();
        }

        numTasks = t;
        numWriters = w;
        numReaders = 1;
        this.lastElement = lastElement;

    }

    private void setQueues() throws IOException {
        executor = Executors.newFixedThreadPool(numReaders + numWriters + numTasks);

        List<BlockingQueue<T>> lastQueues;

        writersQueues = new ArrayList<BlockingQueue<T>>(writersSet.size());
        for (List<? extends DataWriter<T>> dataWriters : writersSet) {
            BlockingQueue queue = new ArrayBlockingQueue(batchSize * dataWriters.size() * 2);
            writersQueues.add(queue);
            for (DataWriter<T> dataWriter : dataWriters) {
                futures.add(executor.submit(new Writer(queue, dataWriter)));
            }
        }
        lastQueues = writersQueues;

        taskQueues = new ArrayList<>(tasksList.size());
        for (int i = tasksList.size() - 1; i >= 0; i--) {
            List<? extends Task<T>> tasks = tasksList.get(i);

            BlockingQueue<T> queue = new ArrayBlockingQueue(batchSize * tasks.size() * 2);
            taskQueues.add(queue);
            ThreadConfig config = new ThreadConfig(lastQueues, queue, tasks.size());
            for (Task<T> task : tasks) {
//                executor.execute(new TaskRunner(queue, task, lastQueues));
                futures.add(executor.submit(new TaskRunner(task, config)));
            }
            lastQueues = Collections.singletonList(queue);
        }

        for (int i = 0; i < numReaders; i++) {
            executor.execute(new Reader(lastQueues, reader));
        }
    }

    @Override
    public void run() throws IOException {

        this.readerInit();
        this.writerInit();
        this.launchPre();

        setQueues();
//
//        for (Future<?> future : futures) {
//            Object o = future.get();
//        }
        setQueues();
        executor.shutdown();
        try {
            executor.awaitTermination(Integer.MAX_VALUE, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            logger.error("ThreadRunner interrupted");
            e.printStackTrace();
        }
//        executor.shutdown();

        this.launchPost();

        this.readerClose();
        this.writerClose();
    }

    @Override
    public void writerInit() {
        for (List<? extends DataWriter<T>> writers : writersSet) {
            for (DataWriter<T> dw : writers) {
                dw.open();
                dw.pre();
            }
        }
    }

    @Override
    public void writerClose() {
        for (List<? extends DataWriter<T>> writers : writersSet) {
            for (DataWriter<T> dw : writers) {
                dw.post();
                dw.close();
            }
        }
    }

    @Override
    public void launchPre() throws IOException {
        for (List<? extends Task<T>> tasks : tasksList) {
            for (Task<T> t : tasks) {
                t.pre();
            }
        }
    }

    @Override
    public void launchPost() throws IOException {
        for (List<? extends Task<T>> tasks : tasksList) {
            for (Task<T> t : tasks) {
                t.post();
            }
        }
    }

    class Writer implements Runnable {
        private BlockingQueue<T> inputQueue;
        private DataWriter<T> writer;

        public Writer(BlockingQueue inputQueue, DataWriter<T> writer) {
            this.inputQueue = inputQueue;
            this.writer = writer;
        }

        @Override
        public void run() {
            try {
                List<T> batch = new ArrayList<>(batchSize);
                T elem = inputQueue.take();
                while (elem != lastElement) {
                    batch.add(elem);
                    if (batch.size() == batchSize) {
                        writer.write(batch);
                        batch.clear();
                    }
                    elem = inputQueue.take();
                }
                inputQueue.put(lastElement);
                if (!batch.isEmpty()) { //Upload remaining elements
                    writer.write(batch);
                }
                logger.debug("Thread finished writing");
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
    }

    class ThreadConfig{
        private final List<BlockingQueue<T>> outputQueues;
        private final BlockingQueue<T> inputQueue;
        private final Integer numTasks;
        private Integer numFinishedTasks;

        public ThreadConfig(List<BlockingQueue<T>> outputQueues, BlockingQueue<T> inputQueue, Integer numTasks) {
            this.outputQueues = outputQueues;
            this.inputQueue = inputQueue;
            this.numTasks = numTasks;
            numFinishedTasks = 0;
        }

        @Override
        public String toString() {
            return super.toString() + "ThreadConfig {" +
                    "numTasks=" + numTasks +
                    ", numFinishedTasks=" + numFinishedTasks +
                    '}';
        }
    }

    class TaskRunner implements Runnable {
        private final Task<T> task;
        private final ThreadConfig config;
        private List<T> batch;

        public TaskRunner(Task<T> task, ThreadConfig config) {
            this.task = task;
            this.config = config;
            this.batch = new ArrayList<>(batchSize);
        }

        @Override
        public void run() {
            try {
                T elem = config.inputQueue.take();
                while (elem != lastElement) {
                    batch.add(elem);
                    if (batch.size() == batchSize) {
                        apply(batch);
                    }
                    elem = config.inputQueue.take();
                }
                if (!batch.isEmpty()) { //Upload remaining elements
                    apply(batch);
                }
                config.inputQueue.put(lastElement);
                synchronized (config) {
                    config.numFinishedTasks++;
                    logger.info(" " + config);
                    if (config.numFinishedTasks == config.numTasks) {
                        for (BlockingQueue<T> outputQueue : config.outputQueues) {
                            outputQueue.put(lastElement);
                        }
                    }
                }
                logger.info("thread finished task");
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }

//        @Override
//        public void run() {
//            try {
//                T elem = config.inputQueue.poll();
//                if (elem == null) {
//                    executor.submit(this);
////                    executor.submit(this);
//                    return;
//                }
//                if (elem != lastElement) {
//                    batch.add(elem);
//                    if (batch.size() == batchSize) {
//                        apply(batch);
//                    } else {
//                        executor.submit(this);
//                    }
//                    return;
////                    elem = config.inputQueue.take();
//                }
//                if (!batch.isEmpty()) { //Upload remaining elements
//                    apply(batch);
//                }
//                config.inputQueue.put(lastElement);
//                synchronized (config) {
//                    config.numFinishedTasks++;
//                    logger.info(" " + config);
//                    if (config.numFinishedTasks == config.numTasks) {
//                        for (BlockingQueue<T> outputQueue : config.outputQueues) {
//                            outputQueue.put(lastElement);
//                        }
//                    }
//                }
//                logger.info("thread finished task");
//            } catch (InterruptedException e) {
//                e.printStackTrace();
//            }
//        }

        private void apply(List<T> batch) throws InterruptedException {
            try {
                task.apply(batch);
            } catch (IOException e) {
                e.printStackTrace();
            }
            for (BlockingQueue<T> outputQueue : config.outputQueues) {
                for (T t : batch) {
                    outputQueue.put(t);
                }
            }
            batch.clear();
        }
    }

    class Reader implements Runnable {
        private List<BlockingQueue<T>> outputQueue;
        private DataReader<T> reader;
        public Reader(List<BlockingQueue<T>> outputQueue, DataReader<T> reader) {
            this.outputQueue = outputQueue;
            this.reader = reader;
        }

        @Override
        public void run() {
            try {
                List<T> read;
                int readeBatchSize = batchSize / 10 + 1;
                read = reader.read(readeBatchSize);

                while(read != null && !read.isEmpty()) {
                    for (BlockingQueue<T> queue : outputQueue) {
                        for (T t : read) {
                            queue.put(t);
                        }
                        read = reader.read(readeBatchSize);
                    }
                }
                //Add a lastElement marker. Consumers will stop reading when read this element.
                for (BlockingQueue<T> queue : outputQueue) {
                    queue.put(lastElement);
                }
                logger.info("thread finished reading");
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
    }

}
