package org.opencb.opencga.storage.core.runner;

import org.junit.Test;
import org.opencb.commons.io.DataReader;
import org.opencb.commons.io.DataWriter;
import org.opencb.commons.run.Task;
import org.opencb.opencga.lib.common.ExceptionUtils;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.Assert.*;

/**
 * Created by jmmut on 2015-10-06.
 *
 * @author Jose Miguel Mut Lopez &lt;jmmut@ebi.ac.uk&gt;
 */
public class SimpleThreadRunnerTest {

    @Test(timeout = 5000)
    public void correctTest() throws Exception {
        final AtomicInteger processedElems = new AtomicInteger(0);
        List<String> data = Arrays.asList("1", "2", "3", "4", "5", "6", "7", "8", "9");

        //Reader
        DataReader dataReader = new TestDataReader(data, processedElems, false);

        // Task
        Task task = new TestTask(processedElems, false);

        //Writer
        DataWriter dataWriter = new TestDataWriter(processedElems, false);

        SimpleThreadRunner runner = new SimpleThreadRunner(
                dataReader,
                Collections.singletonList(task),
                dataWriter,
                3,
                4,
                1
        );

        runner.run();
        System.out.println(processedElems.get());
        assertEquals(data.size()*3, processedElems.get());
    }

//    @Test
//    public void finallyTest () throws Exception {
//        try {
//            throw new InterruptedException("interrupted");
//        } catch (Exception e) {
//            throw new Exception(e);
//        } finally {
//            System.out.println("on finally");
//        }
//    }

    @Test(timeout = 5000)
    public void interruptedReaderTest () throws Exception {
        final AtomicInteger processedElems = new AtomicInteger(0);
        List<String> data = Arrays.asList("1", "2", "3", "4", "5", "6", "7", "8", "9");

        //Reader
        DataReader dataReader = new TestDataReader(data, processedElems, true);

        // Task
        Task task = new TestTask(processedElems, false);

        //Writer
        DataWriter dataWriter = new TestDataWriter(processedElems, false);

        SimpleThreadRunner runner = new SimpleThreadRunner(
                dataReader,
                Collections.singletonList(task),
                dataWriter,
                3,
                4,
                1
        );

        try {
            runner.run();
            fail();
        } catch (Exception e) {
            System.out.println("expected exception");
        }
    }

    @Test(timeout = 5000)
    public void interruptedTaskTest () throws Exception {
        final AtomicInteger processedElems = new AtomicInteger(0);
        List<String> data = Arrays.asList("1", "2", "3", "4", "5", "6", "7", "8", "9");

        //Reader
        DataReader dataReader = new TestDataReader(data, processedElems, false);

        // Task
        Task task = new TestTask(processedElems, true);

        //Writer
        DataWriter dataWriter = new TestDataWriter(processedElems, false);

        SimpleThreadRunner runner = new SimpleThreadRunner(
                dataReader,
                Collections.singletonList(task),
                dataWriter,
                3,
                4,
                1
        );

        try {
            runner.run();
            fail();
        } catch (Exception e) {
            System.out.println("expected exception");
        }
    }

    @Test(timeout = 5000)
    public void interruptedWriterTest () throws Exception {
        final AtomicInteger processedElems = new AtomicInteger(0);
        List<String> data = Arrays.asList("1", "2", "3", "4", "5", "6", "7", "8", "9");

        //Reader
        DataReader dataReader = new TestDataReader(data, processedElems, false);

        // Task
        Task task = new TestTask(processedElems, false);

        //Writer
        DataWriter dataWriter = new TestDataWriter(processedElems, true);

        SimpleThreadRunner runner = new SimpleThreadRunner(
                dataReader,
                Collections.singletonList(task),
                dataWriter,
                3,
                4,
                1
        );

        try {
            runner.run();
            fail();
        } catch (Exception e) {
            System.out.println("expected exception");
        }
    }

    public class TestDataReader implements DataReader {
        private List<String> data;
        private int cursor = 0;
        private final AtomicInteger processedElems;
        private boolean interrupt;

        public TestDataReader(List<String> data, AtomicInteger processedElems, boolean interrupt) {
            this.data = data;
            this.processedElems = processedElems;
            this.interrupt = interrupt;
        }

        @Override
        public boolean open() {
            return false;
        }

        @Override
        public boolean close() {
            return false;
        }

        @Override
        public boolean pre() {
            return false;
        }

        @Override
        public boolean post() {
            return false;
        }

        @Override
        public List read() {
            return read(1);
        }

        /**
         * return at most a list of `batchSize` elements. return null if there aren't any elements.
         * @param batchSize
         * @return
         */
        @Override
        public List read(int batchSize) {
            List<String> ret = new ArrayList<>();
            for (int i = 0; i < batchSize && cursor < data.size(); i++, cursor++) {
                try {
                    Thread.sleep(100);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
                ret.add(data.get(cursor));
                System.out.println("read elem " + data.get(cursor));
                processedElems.incrementAndGet();
                if (cursor == 7 && interrupt) {
                    throw new RuntimeException("Fake read interrupt");
                }
            }
            if (ret.isEmpty()) {
                System.out.println("read null");
                ret = null;
            }

            return ret;
        }
    }

    private class TestTask extends Task {
        private final AtomicInteger processedElems;
        private boolean interrupt;
        private int processed = 0;

        public TestTask(AtomicInteger processedElems, boolean interrupt) {
            this.processedElems = processedElems;
            this.interrupt = interrupt;
        }

        @Override
        public boolean apply(List batch) throws IOException {
            if (batch == null) {
                System.out.println("apply null");
                return false;
            }
            for (Object elem : batch) {
                System.out.println("apply elem " + elem);
                processedElems.incrementAndGet();
                processed++;
                if (processed == 7 && interrupt) {
                    throw new RuntimeException("Fake task interrupt");
                }
            }
            return true;
        }

        @Override
        public int compareTo(Object o) {
            return 0;
        }
    }

    private class TestDataWriter implements DataWriter {
        private final AtomicInteger processedElems;
        private boolean interrupt;
        private int processed = 0;

        public TestDataWriter(AtomicInteger processedElems, boolean interrupt) {
            this.processedElems = processedElems;
            this.interrupt = interrupt;
        }

        @Override
        public boolean open() {
            return false;
        }

        @Override
        public boolean close() {
            return false;
        }

        @Override
        public boolean pre() {
            return false;
        }

        @Override
        public boolean post() {
            return false;
        }

        @Override
        public boolean write(Object elem) {
            return write(1);
        }

        @Override
        public boolean write(List batch) {
            if (batch == null) {
                System.out.println("write null");
                return false;
            }
            for (Object elem : batch) {
                System.out.println("write elem " + elem);
                processedElems.incrementAndGet();
                processed++;
                if (processed == 1 && interrupt) {
                    throw new RuntimeException("Fake writer interrupt");
                }
            }
            return true;
        }
    }
}

