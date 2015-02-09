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
    ExecutorService executor;
    private T lastElement;
    private Logger logger = LoggerFactory.getLogger(ThreadRunner.class);
    private final int numWriters;
    private final int numReaders;
    private List<BlockingQueue<T>> queues;
    private List<DataWriter<T>> writers;
    private Set<List<? extends DataWriter<T>>> writersSet;

    public ThreadRunner(DataReader<T> reader, Set<List<? extends DataWriter<T>>> writersSet, List<Task<T>> tasks, int batchSize, T lastElement) {
        super(reader, null, tasks, batchSize);
        this.writersSet = writersSet;
        this.writers = new LinkedList<>();

        int w = 0;
        for (List<? extends DataWriter<T>> dataWriters : this.writersSet) {
            writers.addAll(dataWriters);
            w+=dataWriters.size();
        }
        numWriters = w;
        numReaders = 1;
        this.lastElement = lastElement;

    }

    private void setQueues() throws IOException {
        executor = Executors.newFixedThreadPool(numReaders + numWriters);

        queues = new ArrayList<BlockingQueue<T>>(writersSet.size());
        for (List<? extends DataWriter<T>> dataWriters : writersSet) {
            BlockingQueue queue = new ArrayBlockingQueue(batchSize * dataWriters.size() * 2);
            queues.add(queue);
            for (DataWriter<T> dataWriter : dataWriters) {
                executor.execute(new Writer(queue, dataWriter));
            }
        }

        for (int i = 0; i < numReaders; i++) {
            executor.execute(new Reader(queues, reader));
        }
    }

    @Override
    public void run() throws IOException {

        this.readerInit();
        this.writerInit();

        this.launchPre();

        setQueues();
        executor.shutdown();
        try {
            executor.awaitTermination(Integer.MAX_VALUE, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            logger.error("ThreadRunner interrupted");
            e.printStackTrace();
        }

        this.launchPost();

        this.readerClose();
        this.writerClose();
        for (DataWriter<T> writer : writers) {
            writer.post();
            writer.close();
        }
    }

    @Override
    public void writerInit() {

        for (DataWriter<T> dw : writers) {
            dw.open();
            dw.pre();
        }

    }

    @Override
    public void writerClose() {
        for (DataWriter<T> dw : writers) {
            dw.post();
            dw.close();
        }
    }

    class Writer implements Runnable {
        private BlockingQueue<T> queue;
        private DataWriter<T> writer;

        public Writer(BlockingQueue queue, DataWriter<T> writer) {
            this.queue = queue;
            this.writer = writer;
        }

        @Override
        public void run() {
            try {
                List<T> batch = new ArrayList<>(batchSize);
                T elem = queue.take();
                while (elem != lastElement) {
                    batch.add(elem);
                    if (batch.size() == batchSize) {
                        writer.write(batch);
                        batch.clear();
                    }
                    elem = queue.take();
                }
                queue.put(lastElement);
                if (!batch.isEmpty()) { //Upload remaining elements
                    writer.write(batch);
                }
                logger.info("thread finished writing");
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
    }


    class Reader implements Runnable {
        private List<BlockingQueue<T>> queues;
        private DataReader<T> reader;
//        private int numConsumers;

        public Reader(List<BlockingQueue<T>> queues, DataReader<T> reader/*, int numConsumers*/) {
            this.queues = queues;
            this.reader = reader;
//            this.numConsumers = numConsumers;
        }

//        public Reader(List<BlockingQueue<T>> queues, DataReader<T> reader, Map<DataWriter<T>, Integer> threadsPerWriters) {
//            this.queues = queues;
//            this.reader = reader;
//            this.threadsPerWriters = threadsPerWriters;
//        }

        @Override
        public void run() {
            try {
                List<T> read;
                int readeBatchSize = batchSize / 10 + 1;
                read = reader.read(readeBatchSize);

                while(read != null && !read.isEmpty()) {
                    for (BlockingQueue<T> queue : queues) {
                        for (T t : read) {
                            queue.put(t);
                        }
                        read = reader.read(readeBatchSize);
                    }
                }
//                for (int i = 0; i < numConsumers; i++) {    //Add a lastElement marker. Consumers will stop reading when read this element.
                    for (BlockingQueue<T> queue : queues) {
                        queue.put(lastElement);
                    }
//                }
                logger.info("thread finished reading");
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
    }

}
