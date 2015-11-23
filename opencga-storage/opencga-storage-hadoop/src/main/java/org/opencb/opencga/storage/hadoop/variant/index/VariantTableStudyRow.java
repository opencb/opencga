/**
 * 
 */
package org.opencb.opencga.storage.hadoop.variant.index;

import java.io.UncheckedIOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.stream.Collectors;
import java.util.NavigableMap;
import java.util.Set;

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;
import org.opencb.biodata.models.feature.Genotype;
import org.opencb.opencga.storage.hadoop.variant.GenomeHelper;
import org.opencb.opencga.storage.hadoop.variant.index.models.protobuf.VariantCallProtos.VariantCallProt;

import com.google.protobuf.InvalidProtocolBufferException;

/**
 * @author Matthias Haimel mh719+git@cam.ac.uk
 *
 */
public class VariantTableStudyRow {
    public static final String HOM_REF = "0/0";
    public static final String HET_REF = "0/1";
    public static final String HOM_VAR = "1/1";
    public static final String OTHER = "?";
    public static final char COLUMN_KEY_SEPARATOR = '_';
    private Integer studyId;
    private Integer homRefCount = 0;
    private String chromosome;
    private Long pos;
    private String ref;
    private String alt;
    private Map<String, Set<Integer>> callMap = new HashMap<String, Set<Integer>>();

    public VariantTableStudyRow(VariantTableStudyRow row){
        this.studyId = row.studyId;
        this.homRefCount = row.homRefCount;
        this.chromosome = row.chromosome;
        this.pos = row.pos;
        this.ref = row.ref;
        this.alt = row.alt;
        this.callMap.putAll(
                row.callMap.entrySet().stream().collect(Collectors.toMap(p -> p.getKey(), p -> new HashSet<Integer>(p.getValue()))));
    }

    /**
     * 
     */
    public VariantTableStudyRow (Integer studyId, String chr, Long pos, String ref, String alt) {
        this.studyId = studyId;
        this.homRefCount = 0;
        this.chromosome = chr;
        this.pos = pos;
        this.ref = ref;
        this.alt = alt;
    }

    private VariantTableStudyRow(Integer studyId, String[] arr){
        this(studyId, arr[0], Long.parseLong(arr[1]), arr[2], arr[3]);
    }

    public VariantTableStudyRow(Integer studyId, Result row, GenomeHelper helper) {
        this(studyId, helper.splitVariantRowkey(Bytes.toString(row.getRow())));
        parse(this, row.getFamilyMap(helper.getColumnFamily()));
    }

    public static List<VariantTableStudyRow> parse(Result result, GenomeHelper helper) {
        NavigableMap<byte[], byte[]> familyMap = result.getFamilyMap(helper.getColumnFamily());
        Set<Integer> studyIds = familyMap.keySet().stream()
                .map(columnKey -> extractStudyId(Bytes.toString(columnKey)))
                .collect(Collectors.toSet());

        List<VariantTableStudyRow> rows = new ArrayList<>(studyIds.size());
        for (Integer studyId : studyIds) {
            VariantTableStudyRow row = new VariantTableStudyRow(studyId, helper.splitVariantRowkey(Bytes.toString(result.getRow())));
            rows.add(parse(row, familyMap, true));
        }

        return rows;
    }

    public static VariantTableStudyRow parse(VariantTableStudyRow variantTableStudyRow, NavigableMap<byte[], byte[]> familyMap) {
        return parse(variantTableStudyRow, familyMap, false);
    }

    public static VariantTableStudyRow parse(VariantTableStudyRow variantTableStudyRow, NavigableMap<byte[], byte[]> familyMap,
                                             boolean skipOtherStudies) {
        for(Entry<byte[], byte[]> entry : familyMap.entrySet()){
            String colStr = Bytes.toString(entry.getKey());
            String[] colSplit = colStr.split("_", 2);
            if(!colSplit[0].equals(variantTableStudyRow.studyId.toString())) { // check study ID for consistency check
                if (skipOtherStudies) {
                    continue;
                } else {
                    throw new IllegalStateException(
                            String.format("Expected study id %s, but found %s in row %s", variantTableStudyRow.studyId.toString(), colSplit[0], colStr));
                }
            }
            String gt = colSplit[1];
            if(gt.equals(HOM_REF)){
                variantTableStudyRow.homRefCount = Bytes.toInt(entry.getValue());
            } else {
                try {
                    VariantCallProt vcp = VariantCallProt.parseFrom(entry.getValue());
                    variantTableStudyRow.callMap.put(gt, new HashSet<>(vcp.getSampleIdsList()));
                } catch (InvalidProtocolBufferException e) {
                    throw new UncheckedIOException(e);
                }
            }
        }
        return variantTableStudyRow;
    }

    public Set<String> getGenotypes() {
        return callMap.keySet();
    }

    public Set<Integer> getSampleIds(String gt){
        Set<Integer> set = this.callMap.get(gt);
        if(null == set){
            return Collections.emptySet();
        }
        return set;
    }
    
    public Set<Integer> getSampleIds(Genotype gt){
        return getSampleIds(gt.toString());
    }

    public Integer getStudyId() {
        return studyId;
    }

    /**
     * 
     * @param gt
     * @param sampleIds
     * @throws IllegalStateException in case the sample already exists in the collection
     */
    public void addSampleId(String gt, Collection<Integer> sampleIds) throws IllegalStateException{
        Set<Integer> set = this.callMap.get(gt);
        if(null == set){
            set = new HashSet<Integer>();
            this.callMap.put(gt, set);
        }
        set.addAll(sampleIds);
    }

    /**
     *
     * @param gt
     * @param sampleId
     * @throws IllegalStateException in case the sample already exists in the collection
     */
    public void addSampleId(String gt, Integer sampleId) throws IllegalStateException{
        Set<Integer> set = this.callMap.get(gt);
        if(null == set){
            set = new HashSet<Integer>();
            this.callMap.put(gt, set);
        }
        if(!set.add(sampleId)){
            throw new IllegalStateException(String.format("Sample id %s already in gt set %s", sampleId,gt));
        }
    }

    public String generateRowKey(VariantTableHelper helper) {
        return helper.generateVariantRowKey(this.chromosome, this.pos, this.ref , this.alt);
    }
    
    public void addHomeRefCount(Integer cnt){
        this.homRefCount += cnt;
    }
    public Integer getHomRefCount() {
        return homRefCount;
    }
    public void setHomRefCount(Integer homRefCount) {
        this.homRefCount = homRefCount;
    }

    public Put createPut(VariantTableHelper helper) {
        String generateRowKey = generateRowKey(helper);
        if(this.callMap.containsKey(HOM_REF)){
            throw new IllegalStateException(
                    String.format("HOM_REF data found for row %s for sample IDs %s",
                            generateRowKey,StringUtils.join(this.callMap.get(HOM_REF),",")));
        }
        byte[] cf = helper.getColumnFamily();
        Integer sid = helper.getStudyId();
        Put put = new Put(Bytes.toBytes(generateRowKey));
        put.addColumn(cf, Bytes.toBytes(buildColumnKey(sid, HOM_REF)), Bytes.toBytes(this.homRefCount));
        for(Entry<String, Set<Integer>> entry : this.callMap.entrySet()){
            byte[] column = Bytes.toBytes(buildColumnKey(sid, entry.getKey()));
            
            List<Integer> value = new ArrayList<Integer>(entry.getValue());
            Collections.sort(value);
            byte[] protValue = VariantCallProt.newBuilder().addAllSampleIds(value).build().toByteArray();
            put.addColumn(cf, column, protValue);
        }
        return put;
    }

    public static String buildColumnKey(Integer sid, String gt) {
        return new StringBuilder().append(sid).append(COLUMN_KEY_SEPARATOR).append(gt).toString();
    }

    public static int extractStudyId(String columnKey) {
        return Integer.parseInt(StringUtils.split(columnKey, COLUMN_KEY_SEPARATOR)[0]);
    }

//    public static Map<Integer, List<byte[]>> extractStudyIds(Collection<byte[]> columnKeys) {
//        Map<Integer, List<byte[]>> map = new HashMap<>();
//        for (byte[] columnKey : columnKeys) {
//            int studyId = extractStudyId(Bytes.toString(columnKey));
//            if (map.containsKey(studyId)) {
//                map.get(studyId).add(columnKey);
//            } else {
//                ArrayList<byte[]> value = new ArrayList<>();
//                value.add(columnKey);
//                map.put(studyId, value);
//            }
//        }
//        return map;
//    }

}
