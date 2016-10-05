/*
 * Copyright 2015-2016 OpenCB
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

package org.opencb.opencga.storage.hadoop.variant.index;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.RawComparator;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.task.MapContextImpl;
import org.apache.hadoop.security.Credentials;
import org.apache.zookeeper.AsyncCallback;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.opencb.biodata.models.variant.Variant;
import org.opencb.biodata.models.variant.VariantSource;
import org.opencb.biodata.models.variant.protobuf.VcfMeta;
import org.opencb.biodata.models.variant.protobuf.VcfSliceProtos;
import org.opencb.biodata.tools.variant.merge.VariantMerger;
import org.opencb.commons.datastore.core.Query;
import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.commons.datastore.core.QueryResult;
import org.opencb.opencga.storage.core.metadata.StudyConfiguration;
import org.opencb.opencga.storage.core.variant.StudyConfigurationManager;
import org.opencb.opencga.storage.core.variant.adaptors.VariantDBAdaptor;
import org.opencb.opencga.storage.core.variant.adaptors.VariantSourceDBAdaptor;
import org.opencb.opencga.storage.core.variant.io.VariantVcfExporter;
import org.opencb.opencga.storage.hadoop.variant.GenomeHelper;
import org.opencb.opencga.storage.hadoop.variant.archive.ArchiveResultToVariantConverter;

import java.io.*;
import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;
import java.util.zip.GZIPInputStream;
import static org.mockito.Mockito.*;

import static org.junit.Assert.*;

/**
 * Created by mh719 on 02/08/2016.
 */
public class VariantTableMapperTest {

    @Test
    public void generateRegion() throws Exception {
        VariantTableMapper mapper = new VariantTableMapper();
        Set<Integer> integers = mapper.generateRegion(10, 20);
        assertEquals(new HashSet(Arrays.asList(10,11,12,13,14,15,16,17,18,19,20)),integers);
    }

    @Test
    public void generateRegionIndel() throws Exception {
        VariantTableMapper mapper = new VariantTableMapper();
        Set<Integer> integers = mapper.generateRegion(10, 9);
        assertEquals(new HashSet(Arrays.asList(10)),integers);
    }

    @Test
    public void testDoMap() throws Exception {
        long TIMESTAMP = 2;
        StudyConfiguration sc = new StudyConfiguration(1, "Test");
        sc.setSampleIds(new HashMap<>());
        Configuration config = new Configuration();
        VariantTableHelper.setStudyId(config, sc.getStudyId());
        VariantTableHelper gh = new VariantTableHelper(config, "intable", "outtable", null);
        File dir = new File("/Users/mh719/data/proto/44-reg_12-39039797");
        List<File> inputFiles = Arrays.stream(dir.listFiles()).filter(f -> f.getName().endsWith("variants.proto.gz"))
                .collect(Collectors.toList());

        AtomicInteger cnt = new AtomicInteger(0);
        Set<Integer> fileIds = new HashSet<>();
        Set<Integer> sampleIds = new HashSet<>();
        Set<String> sampleNames = new HashSet<>();
        List<KeyValue> kv = new ArrayList<>();
        Map<Integer, VcfMeta> conf = new HashMap<>();
        ObjectMapper objectMapper = new ObjectMapper();

        Set<String> chrSet = new HashSet<>();
        Set<Integer> posSet = new HashSet<>();
        List<VariantSource> variantSourceList = new ArrayList<>();
        inputFiles.stream().forEach(file -> {
            File confFile = new File(file.getAbsolutePath().replace("vcf.gz.variants.proto.gz","vcf.gz.file.json.gz"));
            System.out.println("file = " + file);
            try( InputStream in = new GZIPInputStream(new FileInputStream(file));
            InputStream inconf = new GZIPInputStream(new FileInputStream(confFile));) {
                VcfSliceProtos.VcfSlice vcfSlice = VcfSliceProtos.VcfSlice.parseDelimitedFrom(in);
                chrSet.add(vcfSlice.getChromosome());
                posSet.add(vcfSlice.getPosition());
                byte[] confArr = IOUtils.toByteArray(inconf);
                KeyValue keyValue = new KeyValue(gh.generateVariantRowKey(vcfSlice.getChromosome(), vcfSlice
                        .getPosition()), gh.getColumnFamily(), Bytes.toBytes(Integer.toString(cnt.get())), vcfSlice.toByteArray());
                kv.add(keyValue);
                fileIds.add(cnt.get());
                sampleIds.add(cnt.get());
                VariantSource vs = objectMapper.readValue(confArr, VariantSource.class);
                vs.setStudyId("1");
                variantSourceList.add(vs);
                sampleNames.addAll(vs.getSamples());
                sc.getSampleIds().put(vs.getSamples().get(0), cnt.get());
                sc.getSamplesInFiles().put(cnt.get(), new LinkedHashSet<Integer>(Collections.singleton(cnt.get())));
                conf.put(cnt.getAndIncrement(), new VcfMeta(vs));
            } catch (IOException e) {
                throw new IllegalStateException(e.getMessage(), e);
            }
        });

        String chrom = chrSet.stream().findFirst().get();
        Integer position = posSet.stream().findFirst().get();
        byte[] rowKey = gh.generateVariantRowKey(chrom, position);


        VariantTableMapper mapper = new VariantTableMapper();
//        TestInvocationHandler handler = new TestInvocationHandler();
//        Mapper.Context context = (Mapper.Context) Proxy.newProxyInstance(Mapper.Context.class.getClassLoader(),
//                new Class<?>[]{MapContext.class}, handler);

        Mapper.Context context = mock(Mapper.Context.class) ;
        when(context.getConfiguration()).thenReturn(new Configuration());
        when(context.getCounter(anyString(), anyString())).thenReturn(mock(Counter.class));
        when(context.getCounter(anyObject())).thenReturn(mock(Counter.class));

        ArgumentCaptor<ImmutableBytesWritable> immuteCapture = ArgumentCaptor.forClass(ImmutableBytesWritable.class);
        ArgumentCaptor<Put> putCaptor = ArgumentCaptor.forClass(Put.class);


        Result value = new Result(kv);

        long startPos = position;
        long nextStartPos = position + 1000;
        VariantTableMapper.VariantMapReduceContext ctx =
                new VariantTableMapper.VariantMapReduceContext(
                        rowKey, context, value, fileIds, sampleIds, chrom, startPos, nextStartPos);
        mapper.setHelper(gh);
        mapper.resultConverter = new ArchiveResultToVariantConverter(conf, gh.getColumnFamily());
        mapper.hbaseToVariantConverter = new HBaseToVariantConverter(gh).setFailOnEmptyVariants(true);
        mapper.variantMerger = new VariantMerger();
        mapper.timestamp = TIMESTAMP;
        mapper.studyConfiguration = sc;


        QueryOptions options = new QueryOptions();
        options.add(VariantDBAdaptor.VariantQueryParams.RETURNED_SAMPLES.key(), StringUtils.join(sampleNames, ","));
        OutputStream out = System.out;
        VariantSourceDBAdaptor source = mock(VariantSourceDBAdaptor.class);
        when(source.iterator(anyObject(), anyObject())).thenReturn(variantSourceList.iterator());

//        VariantVcfExporter exporter = new VariantVcfExporter(sc, source, out, options);
//        exporter.open();
//        exporter.pre();

        StudyConfigurationManager scm = mock(StudyConfigurationManager.class);
        when(scm.getStudyConfiguration(anyInt(), anyObject())).thenReturn(new QueryResult<StudyConfiguration>("a",-1,-1,-1l, "","",Collections.singletonList(sc)));

        HBaseToVariantConverter variantConverter = new HBaseToVariantConverter(gh, scm);
        long start = System.currentTimeMillis();
        for (long i = 0; i < 1l; i++) {
            mapper.doMap(ctx);
//            sc.getIndexedFiles().addAll(sc.getSamplesInFiles().keySet());
//            verify(context, atLeastOnce()).write(immuteCapture.capture(), putCaptor.capture());
//            List<Put> values = putCaptor.getAllValues();
//            List<ImmutableBytesWritable> immuteValues = immuteCapture.getAllValues();
//            for (int j = 0; j < values.size(); j++) {
//                Put put = values.get(j);
//                ImmutableBytesWritable ibytes = immuteValues.get(j);
//                String table = Bytes.toString(ibytes.get());
//                if (table.contains("outtable")) {
//                    NavigableMap<byte[], List<Cell>> familyCellMap = put.getFamilyCellMap();
//                    List<Cell> cells = familyCellMap.get(gh.getColumnFamily());
//                    Result result = Result.create(cells);
//                    Variant convert = variantConverter.convert(result);
//                    exporter.write(Collections.singletonList(convert));
//                }
//            }
        }
        long end = System.currentTimeMillis();

//        exporter.post();
//        exporter.close();

        System.out.println("(end - start) = " + (end - start));
    }

    public class TestInvocationHandler implements InvocationHandler {
        public List<Put> data = new ArrayList<>();

        @Override
        public Object invoke(Object proxy, Method method, Object[] args) throws Throwable {
            switch (method.getName()){
                case "getCounter":
                    return mock(Counter.class);
                case "getConfiguration":
                    return new Configuration();
                case "write":
                    data.add((Put) args[1]);
                    break;
            }
            return null;
        }
    }

}