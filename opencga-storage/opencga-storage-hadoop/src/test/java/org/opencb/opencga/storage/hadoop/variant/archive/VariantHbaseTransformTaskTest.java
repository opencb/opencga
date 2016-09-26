package org.opencb.opencga.storage.hadoop.variant.archive;

import org.apache.hadoop.conf.Configuration;
import org.junit.Before;
import org.junit.Test;
import org.opencb.biodata.models.variant.Variant;
import org.opencb.biodata.models.variant.VariantSource;
import org.opencb.biodata.models.variant.protobuf.VcfMeta;
import org.opencb.biodata.models.variant.protobuf.VcfSliceProtos;
import org.opencb.commons.io.DataWriter;
import org.opencb.commons.run.ParallelTaskRunner;
import org.opencb.opencga.storage.core.runner.VcfVariantReader;
import org.opencb.opencga.storage.core.runner.VcfVariantReaderTest;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.function.Supplier;

import static org.junit.Assert.*;

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
        VariantSource source = new VariantSource("1","1", "1","1");
        VcfMeta vs = new VcfMeta(source);
        ArchiveHelper helper = new ArchiveHelper(conf, vs);
        ParallelTaskRunner.Task<Variant, VcfSliceProtos.VcfSlice> task = new VariantHbaseTransformTask(helper, null);
        ParallelTaskRunner.Config config = new ParallelTaskRunner.Config(1,10,2,false);
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
        VariantSource source = new VariantSource("1","1", "1","1");
        VcfMeta vs = new VcfMeta(source);
        ArchiveHelper helper = new ArchiveHelper(conf, vs);
        ParallelTaskRunner.Task<Variant, VcfSliceProtos.VcfSlice> task = new VariantHbaseTransformTask(helper, null);


        return () -> {
            List<Variant> read = Collections.emptyList();
            while( !(read = reader.read(100)).isEmpty()) {
                List<VcfSliceProtos.VcfSlice> slices = task.apply(read);
                if (!slices.isEmpty())
                    collector.write(slices);
            }
            collector.write(task.drain());
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