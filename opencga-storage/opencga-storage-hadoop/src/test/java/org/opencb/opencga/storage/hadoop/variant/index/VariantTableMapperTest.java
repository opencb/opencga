package org.opencb.opencga.storage.hadoop.variant.index;

import static org.junit.Assert.*;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.zip.GZIPInputStream;

import org.apache.avro.file.DataFileReader;
import org.apache.avro.io.DatumReader;
import org.apache.avro.mapred.AvroKey;
import org.apache.avro.specific.SpecificDatumReader;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.MapContext;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.mrunit.types.Pair;
import org.junit.After;
import org.junit.Assume;
import org.junit.Before;
import org.junit.Test;
import org.opencb.biodata.models.feature.Genotype;
import org.opencb.biodata.models.variant.Variant;
import org.opencb.biodata.models.variant.avro.VariantAvro;
import org.opencb.biodata.models.variant.avro.VariantType;
import org.opencb.opencga.storage.core.StudyConfiguration;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.databind.ObjectMapper;

public class VariantTableMapperTest {

    private class CounterProxy implements InvocationHandler{

        @Override
        public Object invoke(Object proxy, Method method, Object[] args) throws Throwable {
            return null; // do nothing
        }
        
    }
    
    private class ContextProxy implements InvocationHandler {
        @Override
        public Object invoke(Object proxy, Method method, Object[] args) throws Throwable {
            if(method.getName().equals("getCounter")){
                return Proxy.newProxyInstance(this.getClass().getClassLoader(),new Class[]{Counter.class},new CounterProxy());
            }
            return null;
        }
    }
    
    private static final String AVRO_FILE = "VCF_To_Avro_Conversion/example.vcf.gz.variants.avro.gz";
//    private static final String AVRO_STUDY_FILE = "VCF_To_Avro_Conversion/example.vcf.gz.file.json.gz";
    List<Variant> variantCollection;
    VariantTableMapper tm;
    StudyConfiguration studyConfiguration;
    private MapContext cxt;
    private VariantTableHelper helper;

    @Before
    public void setUp() throws Exception {
        variantCollection = new ArrayList<Variant>();
        for(Pair<AvroKey<VariantAvro>, NullWritable> v : getInput(0, 1)){
            variantCollection.add(new Variant(v.getFirst().datum()));
        }
        studyConfiguration = new StudyConfiguration(1, "1-test");
        studyConfiguration.getSampleIds().put("Sample1", 1);
        variantCollection = variantCollection.stream().
                sorted( (a,b) -> a.getStart().compareTo(b.getStart())).collect(Collectors.toList());
        
        Configuration conf = new Configuration();
        VariantTableHelper.setInputTableName(conf, "test");
        VariantTableHelper.setOutputTableName(conf, "outtable");
        helper = new VariantTableHelper(conf );
        tm = new VariantTableMapper(){public StudyConfiguration getStudyConfiguration() {
            return studyConfiguration;}};
        tm.setHelper(helper);
            
       cxt = (MapContext) Proxy.newProxyInstance(this.getClass().getClassLoader(),new Class[]{MapContext.class},new ContextProxy());
    } 
               
    @After
    public void tearDown() throws Exception {
    }

    @Test
    public void testGenerateCoveredPositions() {
        Set<Integer> positions = tm.generateCoveredPositions(
                this.variantCollection.subList(0, 3).stream());
        assertEquals(positions, new HashSet<Integer>(Arrays.asList(167154617,167154547,167154918)));
    }

    @Test
    public void testFilterForVariantEmpty(){
        List<Variant> collect = tm.filterForVariant(this.variantCollection.stream()).collect(Collectors.toList());
        assertEquals(0, collect.size());
    }

    @Test
    public void testFilterForVariantSNP(){
        List<Variant> collect = tm.filterForVariant(this.variantCollection.stream(),VariantType.SNP).collect(Collectors.toList());
        assertNotEquals(0, collect.size());
    }

    @Test
    public void testFilterForVariantSNV(){
        List<Variant> collect = tm.filterForVariant(this.variantCollection.stream(),VariantType.SNV).collect(Collectors.toList());
        assertNotEquals(0, collect.size());
    }
    
    @Test
    public void testVariantCoveringPosition(){
        Variant var = this.variantCollection.stream().filter(v -> v.getLength() > 1).findFirst().get();
        assertTrue(tm.variantCoveringPosition(var, var.getStart()));
        assertTrue(tm.variantCoveringPosition(var, var.getEnd()));
        assertTrue(tm.variantCoveringPosition(var, var.getEnd()-1)); // in region

        assertFalse(tm.variantCoveringPosition(var, var.getStart()-1));
        assertFalse(tm.variantCoveringPosition(var, var.getEnd()+1));
        
    }
    
    @Test
    public void testVariantCoveringRegion(){
        Variant var = this.variantCollection.stream().filter(v -> v.getLength() > 1).findFirst().get();
        assertTrue(tm.variantCoveringRegion(var, var.getStart(), var.getEnd(), true));
        assertTrue(tm.variantCoveringRegion(var, var.getStart(), var.getEnd(), false));

        assertTrue(tm.variantCoveringRegion(var, var.getStart(), var.getStart(), true));
        assertFalse(tm.variantCoveringRegion(var, var.getStart(), var.getStart(), false));
        assertTrue(tm.variantCoveringRegion(var, var.getEnd(), var.getEnd(), true));
        assertFalse(tm.variantCoveringRegion(var, var.getEnd(), var.getEnd(), false));
    }
    
    @Test
    public void testCreateGenotypeIndex(){
        Variant var = this.variantCollection.stream().filter(v -> v.getLength() > 1).findFirst().get();
        HashMap<String, Integer> map = new HashMap<String, Integer>();
        map.put("Sample1", 0);
        var.getStudies().get(0).setSamplesPosition(map);
        Map<String, List<Integer>> createGenotypeIndex = tm.createGenotypeIndex(var);
        assertEquals(createGenotypeIndex.get("1/1"), Arrays.asList(1));
    }
    
    @Test
    public void testExtractGts(){
        Variant var = this.variantCollection.stream().findFirst().get();
        List<Genotype> extractGts = tm.extractGts(var);
        assertEquals(1, extractGts.size());
        assertEquals("1/1", extractGts.get(0).toString());
    }
    
    @Test
    public void testHasVariant(){
        Variant var = this.variantCollection.stream().findFirst().get();
        List<Genotype> extractGts = tm.extractGts(var);
        assertTrue(tm.hasVariant(extractGts));
        assertFalse(tm.hasVariant(Arrays.asList( new Genotype("./.", "A", "T"))));
        assertFalse(tm.hasVariant(Arrays.asList( new Genotype("./1", "A", "T"))));
        assertFalse(tm.hasVariant(Arrays.asList( // ignore multi-allelic
                new Genotype("1/2", "A", Arrays.asList("T","G")))));
    }
    
    @Test
    public void testNewVariantOne(){
        HashMap<String, Integer> map = new HashMap<String, Integer>();
        map.put("Sample1", 0);
        List<Variant> sublist = this.variantCollection.subList(0, 1);
        Map<Integer, List<Variant>> varlst = sublist.stream()
                .filter(v -> {v.getStudies().get(0).setSamplesPosition(map);return true;})
                .collect(Collectors.groupingBy(v -> v.getStart()));

        Map<String, VariantTableStudyRow> res = tm.createNewVar(cxt, sublist,sublist.stream().map(v -> v.getStart()).collect(Collectors.toSet()));
        assertEquals(res.values().stream().findFirst().get().getHomRefCount().intValue(), 0);
        assertEquals(res.values().stream().findFirst().get().getSampleIds(VariantTableStudyRow.HOM_VAR).size(), 1);
        System.out.println(res);
    }
    
    private <A,B> Map<A,B> asMap(A key,B val){
        HashMap<A, B> map = new HashMap<A, B>();
        map.put(key, val);
        return map;
    }
    @Test
    public void testNewVariantReg(){
        // Represent GVCFs instead of merged VCF

        studyConfiguration.getSampleIds().put("Sample2", 2);
        studyConfiguration.getSampleIds().put("Sample3", 3);
        studyConfiguration.getSampleIds().put("Sample4", 4);
        
        List<Variant> subList = this.variantCollection.subList(0, 1);
        Variant var = subList.get(0);
        var.getStudies().get(0).setSamplesPosition(asMap("Sample1",0));
        
        Variant varModRef = new Variant(VariantAvro.newBuilder(var.getImpl()).build());
        varModRef.setAlternate("");
        varModRef.getStudies().get(0).getSamplesData().get(0).set(0, "0/0");
        varModRef.getStudies().get(0).setSamplesPosition(asMap("Sample4",0));

        Variant varMod2 = new Variant(VariantAvro.newBuilder(var.getImpl()).build());
        varMod2.setAlternate("X");
        varMod2.getStudies().get(0).setSamplesPosition(asMap("Sample3",0));
        
        Variant varMod = new Variant(VariantAvro.newBuilder(var.getImpl()).build());
        varMod.setEnd(varMod.getStart()+3);
        varMod.setStart(varMod.getStart()-1);
        varMod.setAlternate("XXXX");
        varMod.setReference("AAAA");
        varMod.getStudies().get(0).setSamplesPosition(asMap("Sample2",0));

        subList.add(varModRef);
        subList.add(varMod);
        subList.add(varMod2);
        
        Map<String, VariantTableStudyRow> res = tm.createNewVar(cxt, subList,new HashSet<Integer>(Arrays.asList(var.getStart())));
        assertEquals(2, res.size());
        assertEquals(1, res.get(res.keySet().stream().filter(k -> k.endsWith("_T")).findFirst().get()).getHomRefCount().intValue());
        assertEquals(1,res.values().stream().findFirst().get().getSampleIds(VariantTableStudyRow.HOM_VAR).size());
        assertEquals(0, res.values().stream().findFirst().get().getSampleIds(VariantTableStudyRow.HET_REF).size());
        assertEquals(2, res.values().stream().findFirst().get().getSampleIds(VariantTableStudyRow.OTHER).size());
        System.out.println(res);
    }
    
    private List<Pair<AvroKey<VariantAvro>, NullWritable>> getInput(int max, int repeat) throws IOException {
        File f = new File(AVRO_FILE);
        List<Pair<AvroKey<VariantAvro>, NullWritable>> lst = new ArrayList<Pair<AvroKey<VariantAvro>, NullWritable>>();
        DatumReader<VariantAvro> userDatumReader = new SpecificDatumReader<VariantAvro>(VariantAvro.class);
        try (DataFileReader<VariantAvro> dataFileReader = new DataFileReader<VariantAvro>(f, userDatumReader)) {
            int cnt = 0;
            while (dataFileReader.hasNext()) {
                VariantAvro v = dataFileReader.next();
                for (int i = 0; i < repeat; ++i) {
                    lst.add(new Pair<AvroKey<VariantAvro>, NullWritable>(new AvroKey<VariantAvro>(v), NullWritable.get()));
                }
                ++cnt;
                if (max > 0 && cnt >= max) {
                    break;
                }
            }
            return lst;
        }
    }

}
