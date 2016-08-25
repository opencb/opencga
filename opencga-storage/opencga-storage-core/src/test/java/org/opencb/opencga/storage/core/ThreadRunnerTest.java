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

package org.opencb.opencga.storage.core;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.commons.lang3.RandomStringUtils;
import org.junit.Test;
import org.opencb.commons.io.DataReader;
import org.opencb.commons.io.DataWriter;
import org.opencb.commons.run.Task;
import org.opencb.commons.test.GenericTest;
import org.opencb.datastore.core.ObjectMap;
import org.opencb.opencga.core.common.StringUtils;
import org.opencb.opencga.storage.core.runner.ThreadRunner;

import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

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
//            try {
//                Thread.sleep(10);
//            } catch (InterruptedException e) {
//                e.printStackTrace();
//            }
            Random random = new Random(System.currentTimeMillis());
            for (int i = 0; i < batchSize && num < max; i++) {
                num++;

//                batch.add( String.format("%7d %s", num, StringUtils.randomString(10)));
//                batch.add( StringUtils.randomString(10) );
                batch.add( String.format("{ \"id\": %d, \"string\": \"%s\", \"array\": [%d, %d, %d]}",
                        num, StringUtils.randomString(10), random.nextInt()%1024, random.nextInt()%1024, random.nextInt()%1024));
            }
//            System.out.println("batchSize " + batch.size());
            return batch;
        }
    }

    static class Parser extends ThreadRunner.Task<String, ObjectMap> {
        private final ObjectMapper mapper;

        public Parser() {
            super();
            JsonFactory factory = new JsonFactory();
            mapper = new ObjectMapper(factory);
        }

        @Override
        public List<ObjectMap> apply(List<String> batch) throws IOException {
            List<ObjectMap> newBatch = new ArrayList<>(batch.size());
            for (String s : batch) {
                ObjectMap objectMap = mapper.readValue(s.toString(), ObjectMap.class);
                newBatch.add(objectMap);
            }
            return newBatch;
        }
    }

    static class Sum extends org.opencb.commons.run.Task<ObjectMap> {
        @Override
        public boolean apply(List<ObjectMap> batch) throws IOException {
            for (ObjectMap objectMap : batch) {
                List<Integer> array = (List) objectMap.get("array");
                int sum = 0;
                for (Integer integer : array) {
                    sum += integer;
                }
                objectMap.put("sum", sum);
            }
            return true;
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

        @Override public boolean open() {return true;}
        @Override public boolean close() {
            try {
                os.flush();
                os.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
            return true;
        }
        @Override public boolean pre() {return true;}
        @Override public boolean post() {
            System.out.println("num = " + num);
            return true;
        }
        @Override public boolean write(String elem) {return write(Collections.singletonList(elem));}
        @Override public boolean write(List<String> batch) {
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
    class ObjectMapWriter implements DataWriter<ObjectMap> {
        int num = 0;
        OutputStream os;

        public ObjectMapWriter(OutputStream os) {
            this.os = os;
        }

        @Override public boolean open() {return true;}
        @Override public boolean close() {
            try {
                os.flush();
                os.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
            return true;
        }
        @Override public boolean pre() {return true;}
        @Override public boolean post() {
            System.out.println("num = " + num);
            return true;
        }
        @Override public boolean write(ObjectMap elem) {return write(Collections.singletonList(elem));}
        @Override public boolean write(List<ObjectMap> batch) {
            for (ObjectMap o : batch) {
                num++;
                try {
                    synchronized (os) {
                        os.write(o.toJson().getBytes());
                        os.write('\n');
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

        ExecutorService executorService = Executors.newFixedThreadPool(2);
        ThreadRunner threadDGARunner = new ThreadRunner(executorService, 100);

        ThreadRunner.ReadNode<String> readerNode = threadDGARunner.newReaderNode(new StringReader(10000), 10);
        ThreadRunner.TaskNode<String, ObjectMap> parserNode = threadDGARunner.newTaskNode(new Parser(), 100);
        ThreadRunner.TaskNode<ObjectMap, ObjectMap> sumNode = threadDGARunner.newTaskNode(new ThreadRunner.Task<ObjectMap, ObjectMap>() {
            public List<ObjectMap> apply(List<ObjectMap> batch) throws IOException {
                ArrayList<ObjectMap> objectMaps = new ArrayList<>(batch.size());
                for (ObjectMap objectMap : batch) {
                    ObjectMap om = new ObjectMap(objectMap);
                    List<Integer> array = (List) objectMap.get("array");
                    int sum = 0;
                    for (Integer integer : array) {
                        sum += integer;
                    }
                    om.put("sum", sum);
                    objectMaps.add(om);
                }
                return objectMaps;
            }
        }, 10);
//        ThreadRunner.SimpleTaskNode<ObjectMap> sumNode = threadDGARunner.newSimpleTaskNode(new Sum(), 10);
        ThreadRunner.WriterNode<ObjectMap> objectMapWriterNode = threadDGARunner.newWriterNode(new ObjectMapWriter(os), 10);
        ThreadRunner.WriterNode<ObjectMap> objectMapWriterNode2 = threadDGARunner.newWriterNode(new ObjectMapWriter(os2), 10);

        readerNode.append(parserNode);
        parserNode.append(sumNode);
        parserNode.append(objectMapWriterNode2);
        sumNode.append(objectMapWriterNode);

        threadDGARunner.run();

        os.flush();
        os.close();

        os2.flush();
        os2.close();
    }
}