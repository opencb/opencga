package org.opencb.opencga.storage.core;

import org.junit.Test;
import org.opencb.commons.io.DataReader;
import org.opencb.commons.io.DataWriter;
import org.opencb.commons.run.Task;
import org.opencb.commons.test.GenericTest;
import org.opencb.opencga.lib.common.StringUtils;
import org.opencb.opencga.lib.common.TimeUtils;

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
                batch.add( String.format("%7d %s\n", num++, StringUtils.randomString(10)));
            }
            return batch;
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
        public boolean close() {return true;}
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

        List<DataWriter<String>> stringWriters = Arrays.<DataWriter<String>>asList(new StringWriter(os), new StringWriter(os), new StringWriter(os), new StringWriter(os), new StringWriter(os), new StringWriter(os), new StringWriter(os), new StringWriter(os),new StringWriter(os), new StringWriter(os), new StringWriter(os), new StringWriter(os), new StringWriter(os), new StringWriter(os), new StringWriter(os), new StringWriter(os));
        ThreadRunner<String> runner = new ThreadRunner<>(
                new StringReader(10000),
                Collections.<List<DataWriter<String>>>singleton(stringWriters),
                Collections.<Task<String>>emptyList(), 100, "");
        runner.run();
        os.flush();
        os.close();
    }
}