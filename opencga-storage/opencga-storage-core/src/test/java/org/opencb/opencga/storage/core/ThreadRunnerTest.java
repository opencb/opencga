package org.opencb.opencga.storage.core;

import org.junit.Test;
import org.opencb.commons.io.DataReader;
import org.opencb.commons.io.DataWriter;
import org.opencb.commons.run.Task;
import org.opencb.commons.test.GenericTest;
import org.opencb.opencga.lib.common.StringUtils;

import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.util.*;

import static org.junit.Assert.*;

public class ThreadRunnerTest extends GenericTest {

    class StringReader implements DataReader<String> {
        int num = 0;
        int max;

        public StringReader(int max) {
            this.max = max;
        }

        @Override
        public boolean open() {return true;}
        @Override
        public boolean close() {return true;}
        @Override
        public boolean pre() {return true;}
        @Override
        public boolean post() {return true;}
        @Override
        public List<String> read() {
            return read(1);
        }

        @Override
        public List<String> read(int batchSize) {
            ArrayList<String> batch = new ArrayList<String>(batchSize);
            for (int i = 0; i < batchSize && num < max; i++) {
                num++;
//                batch.add( String.format("%7d %s", num, StringUtils.randomString(10)));
                batch.add( StringUtils.randomString(10) );
            }
            return batch;
        }
    }

    static class StringTask extends Task<String> {
        @Override
        public boolean apply(List<String> batch) throws IOException {
            List<String> batchCopy = new ArrayList<>(batch);
            batch.clear();
            for (String s : batchCopy) {
                batch.add("(" + s + ")\n");
            }
            return false;
        }
    }

    static class StringNumerateTask extends Task<String> {
        static Integer num = 0;
        static final private Object lock = new Object();
        @Override
        public boolean apply(List<String> batch) throws IOException {
            List<String> batchCopy = new ArrayList<>(batch);
            batch.clear();
            for (String s : batchCopy) {
                synchronized (lock) {
                    batch.add(num++ + " - " + s);
                }
            }
            return false;
        }
    }

    class StringWriter implements DataWriter<String> {
        int num = 0;
        OutputStream os;

        public StringWriter(OutputStream os) {
            this.os = os;
        }

        @Override
        public boolean open() {return true;}
        @Override
        public boolean close() {
            try {
                os.flush();
                os.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
            return true;
        }
        @Override
        public boolean pre() {return true;}
        @Override
        public boolean post() {
            System.out.println("num = " + num);
            return true;
        }
        @Override
        public boolean write(String elem) {return write(Collections.singletonList(elem));}

        @Override
        public boolean write(List<String> batch) {
            for (String s : batch) {
                num++;
                try {
                    synchronized (os) {
                        os.write(s.getBytes());
                    }
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
            return true;
        }
    }

    @Test
    public void testRun() throws Exception {
        FileOutputStream os = new FileOutputStream("/tmp/test.txt");
        FileOutputStream os2 = new FileOutputStream("/tmp/test2.txt");

        Set<List<DataWriter<String>>> writerSet = new HashSet<>();
        writerSet.add(
                Arrays.<DataWriter<String>>asList(
                        new StringWriter(os),
                        new StringWriter(os),
                        new StringWriter(os),
                        new StringWriter(os),
                        new StringWriter(os),
                        new StringWriter(os),
                        new StringWriter(os),
                        new StringWriter(os),
                        new StringWriter(os)
                )
        );
        writerSet.add(
                Arrays.<DataWriter<String>>asList(
                        new StringWriter(os2),
                        new StringWriter(os2),
                        new StringWriter(os2)
                )
        );

        List<List<Task<String>>> tasksLists = Arrays.asList(
                Arrays.<Task<String>>asList(
                        new StringTask(),
                        new StringTask(),
                        new StringTask(),
                        new StringTask(),
                        new StringTask(),
                        new StringTask(),
                        new StringTask(),
                        new StringTask(),
                        new StringTask())
                , Arrays.<Task<String>>asList(
                        new StringNumerateTask(),
                        new StringNumerateTask(),
                        new StringNumerateTask(),
                        new StringNumerateTask(),
                        new StringNumerateTask(),
                        new StringNumerateTask(),
                        new StringNumerateTask(),
                        new StringNumerateTask()
                )
        );

        ThreadRunner<String> runner = new ThreadRunner<>(
                new StringReader(10000),
                writerSet,
                tasksLists,
                1,
                "");
        runner.run();
    }
}