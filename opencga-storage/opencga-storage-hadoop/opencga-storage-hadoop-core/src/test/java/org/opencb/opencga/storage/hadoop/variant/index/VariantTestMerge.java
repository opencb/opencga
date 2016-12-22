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
import com.google.common.collect.BiMap;
import com.google.common.collect.HashBiMap;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.Mapper;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
import org.opencb.biodata.models.variant.StudyEntry;
import org.opencb.biodata.models.variant.Variant;
import org.opencb.biodata.models.variant.VariantSource;
import org.opencb.biodata.models.variant.avro.AlternateCoordinate;
import org.opencb.biodata.models.variant.protobuf.VcfMeta;
import org.opencb.biodata.models.variant.protobuf.VcfSliceProtos;
import org.opencb.biodata.tools.variant.merge.VariantMerger;
import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.commons.datastore.core.QueryResult;
import org.opencb.opencga.storage.core.metadata.StudyConfiguration;
import org.opencb.opencga.storage.core.metadata.StudyConfigurationManager;
import org.opencb.opencga.storage.core.variant.adaptors.VariantDBAdaptor;
import org.opencb.opencga.storage.core.variant.adaptors.VariantSourceDBAdaptor;
import org.opencb.opencga.storage.core.variant.io.VariantVcfExporter;
import org.opencb.opencga.storage.hadoop.utils.HBaseManager;
import org.opencb.opencga.storage.hadoop.variant.GenomeHelper;
import org.opencb.opencga.storage.hadoop.variant.archive.ArchiveResultToVariantConverter;
import org.opencb.opencga.storage.hadoop.variant.converters.HBaseToVariantConverter;

import java.io.*;
import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Method;
import java.util.*;
import java.util.concurrent.ForkJoinPool;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.zip.GZIPInputStream;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.*;

/**
 * Created by mh719 on 02/08/2016.
 */
public class VariantTestMerge {

    @Test
    public void bla() {
        GenomeHelper genomeHelper = new GenomeHelper(new Configuration());
        byte[] bytes1 = genomeHelper.generateVariantRowKey("6", 27598877, "G", "C");
        byte[] bytes = Bytes.fromHex("360001a5201d00000000360001a526de470043");
        byte[] a = Bytes.copy(bytes, 0, 10);
        byte[] b = Bytes.copy(bytes, 10, 9);
        Variant varianta = genomeHelper.extractVariantFromVariantRowKey(a);
        Variant variantb = genomeHelper.extractVariantFromVariantRowKey(b);
        System.out.println(varianta);
        System.out.println(variantb);
    }

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

    private void loadVData(List<File> vData, List<KeyValue> kv, byte[] rowkey, byte[] cf) {
        vData.forEach(f -> {
            String name = f.getName();
            try(InputStream in= new GZIPInputStream(new FileInputStream(f))) {
                kv.add(new KeyValue(rowkey, cf, Bytes.toBytes(name), IOUtils.toByteArray(in)));
            } catch (FileNotFoundException e) {
                e.printStackTrace();
            } catch (IOException e) {
                e.printStackTrace();
            }
        });
    }

    @Test
    public void testDoMap() throws Exception {
        long TIMESTAMP = 3;
//        StudyConfiguration sc = new StudyConfiguration(1, "Test");
//        sc.setSampleIds(new HashMap<>());
        Configuration config = new Configuration();
//        File dir = new File("/Users/mh719/data/proto/10_000000125014.local.2");
        File dir = new File("/Users/mh719/data/proto/2_000000037696");
//        File odir = new File("/Users/mh719/data/proto/10_000000125014.local");
        List<File> inputFiles = Arrays.stream(dir.listFiles()).filter(f -> f.getName().endsWith("variants.proto.gz"))
                .collect(Collectors.toList());
        List<File> vData = Arrays.stream(dir.listFiles()).filter(f -> f.getName().startsWith("_V_"))
                .collect(Collectors.toList());

        StudyConfiguration studyConfiguration = loadStudyConfiguration(dir);
        StudyConfiguration scPost = loadStudyConfiguration(dir);
        VariantTableHelper.setStudyId(config, studyConfiguration.getStudyId());



//        AtomicInteger cnt = new AtomicInteger(0);
        Set<Integer> fileIds = new HashSet<>();
        Set<Integer> sampleIds = new HashSet<>();
//                studyConfiguration.getBatches().get(studyConfiguration.getBatches().size()-1)
//                .getFileIds().stream().flatMap(id ->
//            studyConfiguration.getSamplesInFiles().get(id).stream()).collect(Collectors.toSet());
        BiMap<String, Integer> indexedSampleNames = HashBiMap.create();
        List<KeyValue> kv = new ArrayList<>();
        List<KeyValue> archiveKv = new ArrayList<>();
        Map<Integer, VcfMeta> conf = new HashMap<>();
        ObjectMapper objectMapper = new ObjectMapper();

//        loadVData(vData, kv, gh.generateVariantRowKey("10",125013123), gh.getColumnFamily());
        VariantTableHelper gh = new VariantTableHelper(config, "intable", "outtable", null);

        Set<String> chrSet = new HashSet<>();
        Set<Integer> posSet = new HashSet<>();
        List<VariantSource> variantSourceList = new ArrayList<>();
        inputFiles.stream().forEach(file -> {
            File confFile = new File(file.getAbsolutePath().replace("vcf.gz.variants.proto.gz","vcf.gz.file.json.gz"));
            Integer fid = Integer.valueOf(confFile.getName().replace(".vcf.gz.file.json.gz",""));
            boolean asArchive = fid < 72677;
            boolean isIncluded = fid < (72677 + 500);
            asArchive = fid < 73200;
//            isIncluded = !asArchive;
//            System.out.println("file = " + file);
            isIncluded = true;
            asArchive=false;
            if (isIncluded) {
                try ( InputStream in = new GZIPInputStream(new FileInputStream(file));
                      InputStream inconf = new GZIPInputStream(new FileInputStream(confFile)); ) {
                    VcfSliceProtos.VcfSlice vcfSlice = VcfSliceProtos.VcfSlice.parseFrom(in);
                    chrSet.add(vcfSlice.getChromosome());
                    posSet.add(vcfSlice.getPosition());
                    byte[] confArr = IOUtils.toByteArray(inconf);
                    KeyValue keyValue = new KeyValue(gh.generateVariantRowKey(vcfSlice.getChromosome(), vcfSlice
                            .getPosition()), gh.getColumnFamily(), Bytes.toBytes(fid.toString()), vcfSlice.toByteArray());

                    if (!asArchive) {
                        kv.add(keyValue);
                        fileIds.add(fid);
                    } else {
                        archiveKv.add(keyValue);
                    }
                    sampleIds.addAll(studyConfiguration.getSamplesInFiles().get(fid));
                    VariantSource vs = objectMapper.readValue(confArr, VariantSource.class);
                    vs.setStudyId(studyConfiguration.getStudyId() + "");
                    variantSourceList.add(vs);
//                    sc.getSampleIds().put(vs.getSamples().get(0), fid);
//                    sc.getSamplesInFiles().put(fid, new LinkedHashSet<>(Collections.singleton(fid)));
//                    scPost.getSampleIds().put(vs.getSamples().get(0), fid);
//                    scPost.getSamplesInFiles().put(fid, new LinkedHashSet<>(Collections.singleton(fid)));
//                    scPost.getIndexedFiles().add(fid);
//                    if (asArchive) {
//                        sc.getIndexedFiles().add(fid);
//                        indexedSampleNames.put(vs.getSamples().get(0), fid);
//                    } else {
//                        sampleNames.addAll(vs.getSamples());
//                    }
                    conf.put(fid, new VcfMeta(vs));
                } catch (IOException e) {
                    throw new IllegalStateException(e.getMessage(), e);
                }
            }
        });
        Set<String> sampleNames = sampleIds.stream().map(sid -> studyConfiguration.getSampleIds().inverse().get(sid)).collect(Collectors.toSet());

        // 72677
        String chrom = chrSet.stream().findFirst().get();
        Integer position = posSet.stream().findFirst().get();
        byte[] rowKey = gh.generateVariantRowKey(chrom, position);

        LinkedHashSet<Integer> indexedFiles = new LinkedHashSet<>();
        indexedFiles.addAll(scPost.lastBatch().getFileIds());
        scPost.setIndexedFiles(indexedFiles);

        TestMapper mapper = new TestMapper();
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
        Result queryValue = new Result(archiveKv);

        long startPos = position;
        long nextStartPos = position + 1000;
        VariantTableMapper.VariantMapReduceContext ctx =
                new VariantTableMapper.VariantMapReduceContext(
                        rowKey, context, value, fileIds, sampleIds, chrom, startPos, nextStartPos);

        VariantTableHelper ghSpy = spy(gh);
        HBaseManager hbm = mock(HBaseManager.class);
        when(ghSpy.getHBaseManager()).thenReturn(hbm);
        byte[] intputTable = gh.getIntputTable();
        when(hbm.act(eq(intputTable), anyObject())).thenReturn(queryValue);



        StudyConfigurationManager scm = mock(StudyConfigurationManager.class);
        when(scm.getStudyConfiguration(anyInt(), anyObject())).thenReturn(
                new QueryResult<>("a",-1,-1,-1l, "","",
                        Collections.singletonList(studyConfiguration)));

        HBaseToVariantConverter variantConverter = new HBaseToVariantConverter(ghSpy, scm);
        variantConverter.setFailOnEmptyVariants(true);

        mapper.setHelper(ghSpy);
        mapper.resultConverter = new ArchiveResultToVariantConverter(studyConfiguration.getStudyId(), gh.getColumnFamily(), studyConfiguration);
        ForkJoinPool pool = VariantTableMapper.createForkJoinPool("MyLocaLBla", 100);
        mapper.resultConverter.setParallel(true);
        mapper.setHbaseToVariantConverter(variantConverter);
        mapper.variantMerger = new VariantMerger(true);
        mapper.setTimestamp(TIMESTAMP);
        mapper.setStudyConfiguration(studyConfiguration);
        mapper.setIndexedSamples(indexedSampleNames);
        mapper.currentIndexingSamples = new HashSet<>(sampleNames);
        mapper.archiveBatchSize = 1000;

        QueryOptions options = new QueryOptions();
        options.add(VariantDBAdaptor.VariantQueryParams.RETURNED_SAMPLES.key(), StringUtils.join(sampleNames, ","));
        VariantSourceDBAdaptor source = mock(VariantSourceDBAdaptor.class);
        when(source.iterator(anyObject(), anyObject())).thenReturn(variantSourceList.iterator());


        StudyConfigurationManager scmPost = mock(StudyConfigurationManager.class);
        when(scmPost.getStudyConfiguration(anyInt(), anyObject())).thenReturn(new QueryResult<>("a",-1,-1,-1l, "","",Collections.singletonList(scPost)));
        HBaseToVariantConverter variantConverterPost = new HBaseToVariantConverter(ghSpy, scmPost);

        List<Variant> recoveredVariants = new ArrayList<>();
        long start = System.currentTimeMillis();
        for (long i = 0; i < 1l; i++) {
            mapper.doMap(ctx);
            studyConfiguration.getIndexedFiles().addAll(studyConfiguration.getSamplesInFiles().keySet());
            verify(context, atLeastOnce()).write(immuteCapture.capture(), putCaptor.capture());
            List<Put> values = putCaptor.getAllValues();
            List<ImmutableBytesWritable> immuteValues = immuteCapture.getAllValues();
            for (int j = 0; j < values.size(); j++) {
                Put put = values.get(j);
                ImmutableBytesWritable ibytes = immuteValues.get(j);
                String table = Bytes.toString(ibytes.get());
                NavigableMap<byte[], List<Cell>> familyCellMap = put.getFamilyCellMap();
                List<Cell> cells = familyCellMap.get(gh.getColumnFamily());
                if (table.contains("intable")) {
                    cells.stream().filter(c -> Bytes.startsWith(CellUtil.cloneQualifier(c), Bytes.toBytes("_V_")))
                            .forEach(c -> {
                                byte[] qualifier = CellUtil.cloneQualifier(c);
                                String name = Bytes.toString(qualifier);
//                                File out = new File(odir, name);
//                                try(OutputStream os = new GZIPOutputStream(new FileOutputStream(out))) {
//                                    os.write(CellUtil.cloneValue(c));
//                                } catch (IOException e) {
//                                    throw new RuntimeException(e);
//                                }
                            });
                }
                if (table.contains("outtable")) {
                    Result result = Result.create(cells);
                    Variant convert = variantConverterPost.convert(result);
                    recoveredVariants.add(convert);
                }
            }
        }
        long end = System.currentTimeMillis();


        System.out.println("(end - start) = " + (end - start));
        compareResults(mapper.finalVars, recoveredVariants);

        try(OutputStream out = System.out) {
//        try(OutputStream out = new FileOutputStream(new File(dir.getAbsolutePath() + ".vcf"))) {
            VariantVcfExporter exporter = new VariantVcfExporter(scPost, source, out, options);
//            exporter.setSampleNameConverter(s -> s+"_XXX");
            exporter.open();
            exporter.pre();
            for (Variant variant : recoveredVariants) {
                exporter.write(Collections.singletonList(variant));
            }
            exporter.post();
            exporter.close();
        }
    }

    private StudyConfiguration loadStudyConfiguration(File dir) throws IOException {
        File[] files = dir.listFiles(f -> StringUtils.equals(f.getName(), "studycolumn"));
        if (files.length == 0) {
            return null;
        }
        ObjectMapper objectMapper = new ObjectMapper();
        try (FileInputStream in = new FileInputStream(files[0])) {
            return objectMapper.readValue(IOUtils.toByteArray(in), StudyConfiguration.class);
        }
    }

    private void compareResults(List<Variant> finalVars, List<Variant> recoveredVariants) {
        System.out.println("finalVars.size() = " + finalVars.size());
        System.out.println("recoveredVariants.size() = " + recoveredVariants.size());

        for (Variant submitted : finalVars) {
            List<Variant> matching = recoveredVariants.stream()
                    .filter(v ->
                            v.onSameRegion(submitted)
                            && v.getReference().equals(submitted.getReference())
                            && v.getAlternate().equals(submitted.getAlternate()))
                    .collect(Collectors.toList());
            if (matching.size() > 1 || matching.isEmpty()) {
                throw new IllegalStateException();
            }
            Variant recovered = matching.get(0);
            boolean equals = submitted.equals(recovered);
//            System.out.println("equals = " + equals);

            boolean equalSampleNames = equalSampleNames(submitted, recovered);
//            System.out.println("equalSampleNames = " + equalSampleNames);
            assertTrue(equalSampleNames);

            boolean gt = equalData(submitted, recovered, "GT");
//            System.out.println("gt = " + gt);
            assertTrue(gt);

            boolean ft = equalData(submitted, recovered, "FT");
//            System.out.println("ft = " + ft);
            assertTrue(ft);

            boolean equalSecAlts = equalSecAlts(submitted, recovered);
//            System.out.println("equalSecAlts(submitted, recovered) = " + equalSecAlts);
            assertTrue(equalSecAlts);
        }

    }

    public static boolean equalSecAlts(Variant a, Variant b) {
        HashSet<AlternateCoordinate> aAlts = new HashSet<>(a.getStudies().get(0)
                .getSecondaryAlternates());
        HashSet<AlternateCoordinate> bAlts = new HashSet<>(b.getStudies().get(0)
                .getSecondaryAlternates());
        return aAlts.equals(bAlts);
    }

    public static boolean equalSampleNames(Variant a, Variant b) {
        List<String> namesA = a.getStudies().get(0).getOrderedSamplesName();
        List<String> namesB = b.getStudies().get(0).getOrderedSamplesName();
        Collections.sort(namesA);
        Collections.sort(namesB);
        boolean equals = namesA.equals(namesB);
        if (! equals) {
            System.out.println("namesA = " + namesA);
            System.out.println("namesB = " + namesB);
        }
        return equals;
    }

    public static boolean equalData(Variant a, Variant b, String key) {
        StudyEntry sea = a.getStudies().get(0);
        StudyEntry seb = b.getStudies().get(0);
        ArrayList<String> sampleNames = new ArrayList<>(sea.getSamplesName());
        List<String> gta = sampleNames.stream().map(s -> sea.getSampleData(s, key)).map(g -> g.replace("./.",".")).collect(Collectors.toList()); // Change  ./. to .
        List<String> gtb = sampleNames.stream().map(s -> seb.getSampleData(s, key)).map(g -> g.replace("./.",".")).collect(Collectors.toList());
        IntStream.range(0, sampleNames.size()-1).boxed().filter(i -> !gta.get(i).equals(gtb.get(i))).forEach(i -> System.out.println("[" + a + "]" + sampleNames.get(i) + " : " + gta.get(i) + " - " + gtb.get(i)));
        return gta.equals(gtb);
    }

    public static class TestMapper extends VariantTableMapper {
        private List<Variant> finalVars = new ArrayList<>();

        @Override
        protected void updateOutputTable(Context context, Collection<Variant> analysisVar, List<VariantTableStudyRow> rows, Set<Integer> newSampleIds) {
            finalVars.addAll(analysisVar);
            super.updateOutputTable(context, analysisVar, rows, newSampleIds);
        }

        @Override
        protected void updateOutputTable(Context context, Collection<VariantTableStudyRow> variants) {
            super.updateOutputTable(context, variants);
        }

        @Override
        protected void updateArchiveTable(byte[] rowKey, Context context, List<VariantTableStudyRow> tableStudyRows) {
            super.updateArchiveTable(rowKey, context, tableStudyRows);
        }
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