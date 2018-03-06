/*
 * Copyright 2015-2017 OpenCB
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

package org.opencb.opencga.storage.hadoop.variant.archive;

import com.google.common.base.Throwables;
import org.apache.hadoop.conf.Configuration;
import org.junit.Before;
import org.junit.Test;
import org.opencb.biodata.models.variant.Variant;
import org.opencb.biodata.models.variant.VariantFileMetadata;
import org.opencb.biodata.models.variant.protobuf.VcfSliceProtos;
import org.opencb.commons.io.DataWriter;
import org.opencb.commons.run.ParallelTaskRunner;
import org.opencb.opencga.storage.core.io.VcfVariantReader;
import org.opencb.opencga.storage.core.io.VcfVariantReaderTest;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import static org.junit.Assert.assertEquals;

/**
 * Created by mh719 on 04/05/16.
 */
public class VariantHbaseTransformTaskTest {

    @Before
    public void setUp() throws Exception {
    }

    public ParallelTaskRunner<Variant, VcfSliceProtos.VcfSlice> createParallelRunner(int size, DataWriter<VcfSliceProtos.VcfSlice> collector) throws Exception {
        VcfVariantReader reader = VcfVariantReaderTest.createReader(size);
        Configuration conf = new Configuration();
        ArchiveTableHelper helper = new ArchiveTableHelper(conf, 1, new VariantFileMetadata("1", "1"));
        ParallelTaskRunner.Task<Variant, VcfSliceProtos.VcfSlice> task = new VariantHbaseTransformTask(helper);
        ParallelTaskRunner.Config config = ParallelTaskRunner.Config.builder()
                .setNumTasks(1)
                .setBatchSize(10)
                .setAbortOnFail(true)
                .setSorted(false).build();
        return new ParallelTaskRunner<>(
                reader,
                () -> task,
                collector,
                config
        );
    }

    public Runnable createSerialRunner(int size, DataWriter<VcfSliceProtos.VcfSlice> collector) throws Exception {
        VcfVariantReader reader = VcfVariantReaderTest.createReader(size);
        Configuration conf = new Configuration();
        ArchiveTableHelper helper = new ArchiveTableHelper(conf, 1, new VariantFileMetadata("", ""));
        ParallelTaskRunner.Task<Variant, VcfSliceProtos.VcfSlice> task = new VariantHbaseTransformTask(helper);


        return () -> {
            try {
                List<Variant> read = Collections.emptyList();
                while( !(read = reader.read(100)).isEmpty()) {
                    List<VcfSliceProtos.VcfSlice> slices = task.apply(read);
                    if (!slices.isEmpty())
                        collector.write(slices);
                }
                collector.write(task.drain());
            } catch (Exception e) {
                throw Throwables.propagate(e);
            }
        };
    }

    @Test
    public void testApply() throws Exception {
        int size = 1000;
        final List<VcfSliceProtos.VcfSlice> lst = new ArrayList<>();
        DataWriter<VcfSliceProtos.VcfSlice> collector = new DataWriter<VcfSliceProtos.VcfSlice>(){

            @Override
            public boolean write(List<VcfSliceProtos.VcfSlice> batch) {
                return lst.addAll(batch);
            }

            @Override
            public boolean write(VcfSliceProtos.VcfSlice elem) {
                return lst.add(elem);
            }
        }
                ;
        ParallelTaskRunner<Variant, VcfSliceProtos.VcfSlice> parallelRunner = createParallelRunner(size, collector);

        parallelRunner.run();

        assertEquals(Integer.valueOf(2), Integer.valueOf(lst.size()));

    }


    @Test
    public void testApplySpeed() throws Exception {
        int size = 1000;
        final List<VcfSliceProtos.VcfSlice> lst = new ArrayList<>();
        DataWriter<VcfSliceProtos.VcfSlice> collector = new DataWriter<VcfSliceProtos.VcfSlice>(){

            @Override
            public boolean write(List<VcfSliceProtos.VcfSlice> batch) {
                return lst.addAll(batch);
            }

            @Override
            public boolean write(VcfSliceProtos.VcfSlice elem) {
                return lst.add(elem);
            }
        };
        long curr = System.currentTimeMillis();
        for (int i = 0; i < 10; i++) {
            ParallelTaskRunner<Variant, VcfSliceProtos.VcfSlice> parallelRunner = createParallelRunner(size, collector);

            parallelRunner.run();

        }
        assertEquals(Integer.valueOf(2*10), Integer.valueOf(lst.size()));
        System.out.println(System.currentTimeMillis() - curr);
    }

    @Test
    public void testApplySpeedSingle() throws Exception {
        int size = 1000;
        final List<VcfSliceProtos.VcfSlice> lst = new ArrayList<>();
        DataWriter<VcfSliceProtos.VcfSlice> collector = new DataWriter<VcfSliceProtos.VcfSlice>(){

            @Override
            public boolean write(List<VcfSliceProtos.VcfSlice> batch) {
                return lst.addAll(batch);
            }

            @Override
            public boolean write(VcfSliceProtos.VcfSlice elem) {
                return lst.add(elem);
            }
        };
        long curr = System.currentTimeMillis();
        for (int i = 0; i < 10; i++) {
            Runnable parallelRunner = createSerialRunner(size, collector);

            parallelRunner.run();

        }
        assertEquals(Integer.valueOf(2*10), Integer.valueOf(lst.size()));
        System.out.println(System.currentTimeMillis() - curr);
    }
}