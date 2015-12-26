/**
 *
 */
package org.opencb.opencga.storage.hadoop.variant.index;

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.phoenix.schema.types.PUnsignedIntArray;
import org.apache.phoenix.schema.types.PhoenixArray;
import org.opencb.biodata.models.feature.Genotype;
import org.opencb.biodata.models.variant.Variant;
import org.opencb.opencga.storage.hadoop.variant.GenomeHelper;
import org.opencb.opencga.storage.hadoop.variant.index.phoenix.VariantPhoenixHelper;

import java.sql.Array;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.*;
import java.util.Map.Entry;
import java.util.stream.Collectors;

/**
 * @author Matthias Haimel mh719+git@cam.ac.uk.
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
    private int pos;
    private String ref;
    private String alt;
    private Map<String, Set<Integer>> callMap = new HashMap<>();

    public VariantTableStudyRow(VariantTableStudyRow row) {
        this.studyId = row.studyId;
        this.homRefCount = row.homRefCount;
        this.chromosome = row.chromosome;
        this.pos = row.pos;
        this.ref = row.ref;
        this.alt = row.alt;
        this.callMap.putAll(row.callMap.entrySet().stream().collect(Collectors.toMap(p -> p.getKey(), p -> new HashSet<>(p.getValue()))));
    }

    public VariantTableStudyRow(Integer studyId, String chr, int pos, String ref, String alt) {
        this.studyId = studyId;
        this.homRefCount = 0;
        this.chromosome = chr;
        this.pos = pos;
        this.ref = ref;
        this.alt = alt;
    }

    public int getPos() {
        return pos;
    }

    public VariantTableStudyRow(Integer studyId, Variant variant) {
        this(studyId, variant.getChromosome(), variant.getStart(), variant.getReference(), variant.getAlternate());
    }

    public VariantTableStudyRow(Integer studyId, Result row, GenomeHelper helper) {
        this(studyId, helper.extractVariantFromVariantRowKey(row.getRow()));
        parse(this, row.getFamilyMap(helper.getColumnFamily()));
    }

    public static List<VariantTableStudyRow> parse(Result result, GenomeHelper helper) {
        NavigableMap<byte[], byte[]> familyMap = result.getFamilyMap(helper.getColumnFamily());
        Set<Integer> studyIds = familyMap.entrySet().stream()
                .filter(entry -> entry.getValue() != null && entry.getValue().length > 0)
                .map(entry -> extractStudyId(Bytes.toString(entry.getKey())))
                .filter(integer -> integer != null)
                .collect(Collectors.toSet());

        List<VariantTableStudyRow> rows = new ArrayList<>(studyIds.size());
        for (Integer studyId : studyIds) {
            VariantTableStudyRow row = new VariantTableStudyRow(studyId, helper.extractVariantFromVariantRowKey(result.getRow()));
            rows.add(parse(row, familyMap, true));
        }

        return rows;
    }

    public static List<VariantTableStudyRow> parse(Variant variant, ResultSet resultSet, GenomeHelper helper) throws SQLException {
        ResultSetMetaData metaData = resultSet.getMetaData();
        Set<Integer> studyIds = new HashSet<>();
        for (int i = 0; i < metaData.getColumnCount(); i++) {
            String columnName = metaData.getColumnName(i + 1);
            if (columnName != null && !columnName.isEmpty()) {
                Integer studyId = extractStudyId(columnName);
                if (studyId != null) {
                    studyIds.add(studyId);
                }
            }
        }

        List<VariantTableStudyRow> rows = new ArrayList<>(studyIds.size());
        for (Integer studyId : studyIds) {
            VariantTableStudyRow row = new VariantTableStudyRow(studyId, variant);
            rows.add(parse(row, resultSet, studyId));
        }

        return rows;
    }

    public static VariantTableStudyRow parse(VariantTableStudyRow variantTableStudyRow, NavigableMap<byte[], byte[]> familyMap) {
        return parse(variantTableStudyRow, familyMap, false);
    }

    public static VariantTableStudyRow parse(VariantTableStudyRow variantTableStudyRow, NavigableMap<byte[], byte[]> familyMap,
                                             boolean skipOtherStudies) {
        for (Entry<byte[], byte[]> entry : familyMap.entrySet()) {
            String colStr = Bytes.toString(entry.getKey());
            String[] colSplit = colStr.split("_", 2);
            if (!colSplit[0].equals(variantTableStudyRow.studyId.toString())) { // check study ID for consistency check
                if (skipOtherStudies) {
                    continue;
                } else {
                    throw new IllegalStateException(String.format("Expected study id %s, but found %s in row %s",
                            variantTableStudyRow.studyId.toString(), colSplit[0], colStr));
                }
            }
            String gt = colSplit[1];
            if (gt.equals(HOM_REF)) {
                if (entry.getValue() == null || entry.getValue().length == 0) {
                    variantTableStudyRow.homRefCount = 0;
                } else {
                    variantTableStudyRow.homRefCount = Bytes.toInt(entry.getValue());
                }
            } else {
                if (entry.getValue() == null || entry.getValue().length == 0) {
                    variantTableStudyRow.callMap.put(gt, Collections.emptySet());
                } else {
//                    try {
//                        VariantCallProt vcp = VariantCallProt.parseFrom(entry.getValue());
//                        variantTableStudyRow.callMap.put(gt, new HashSet<>(vcp.getSampleIdsList()));
//                    } catch (InvalidProtocolBufferException e) {
//                        throw new UncheckedIOException(e);
//                    }
                    PhoenixArray phoenixArray = ((PhoenixArray) PUnsignedIntArray.INSTANCE.toObject(entry.getValue()));
                    try {
                        HashSet<Integer> value = new HashSet<>();
                        if (phoenixArray.getArray() != null) {
                            int[] array = (int[]) phoenixArray.getArray();
                            for (int i : array) {
                                value.add(i);
                            }
                        }
                        variantTableStudyRow.callMap.put(gt, value);
                    } catch (SQLException e) {
                        //Impossible
                        throw new RuntimeException(e);
                    }
                }
            }
        }
        return variantTableStudyRow;
    }

    public static VariantTableStudyRow parse(VariantTableStudyRow variantTableStudyRow, ResultSet resultSet, int studyId)
            throws SQLException {
        variantTableStudyRow.homRefCount = resultSet.getInt(buildColumnKey(studyId, HOM_REF));
        for (String gt : new String[]{HET_REF, HOM_VAR, OTHER}) {
            Array sqlArray = resultSet.getArray(buildColumnKey(studyId, gt));
            HashSet<Integer> value = new HashSet<>();
            if (sqlArray != null && sqlArray.getArray() != null) {
                int[] array = (int[]) sqlArray.getArray();
                for (int i : array) {
                    value.add(i);
                }
            }
            variantTableStudyRow.callMap.put(gt, value);
        }

        return variantTableStudyRow;
    }

    public Set<String> getGenotypes() {
        return callMap.keySet();
    }

    public Set<Integer> getSampleIds(String gt) {
        Set<Integer> set = this.callMap.get(gt);
        if (null == set) {
            return Collections.emptySet();
        }
        return set;
    }

    public Set<Integer> getSampleIds(Genotype gt) {
        return getSampleIds(gt.toString());
    }

    public Integer getStudyId() {
        return studyId;
    }

    /**
     * @param gt Genotype code for the samples
     * @param sampleIds Sample numeric codes
     * @throws IllegalStateException in case the sample already exists in the collection
     */
    public void addSampleId(String gt, Collection<Integer> sampleIds) throws IllegalStateException {
        Set<Integer> set = this.callMap.get(gt);
        if (null == set) {
            set = new HashSet<>();
            this.callMap.put(gt, set);
        }
        set.addAll(sampleIds);
    }

    /**
     * @param gt Genotype code for the samples
     * @param sampleId Sample numeric codes
     * @throws IllegalStateException in case the sample already exists in the collection
     */
    public void addSampleId(String gt, Integer sampleId) throws IllegalStateException {
        Set<Integer> set = this.callMap.get(gt);
        if (null == set) {
            set = new HashSet<>();
            this.callMap.put(gt, set);
        }
        if (!set.add(sampleId)) {
            throw new IllegalStateException(String.format("Sample id %s already in gt set %s", sampleId, gt));
        }
    }

    public byte[] generateRowKey(VariantTableHelper helper) {
        return helper.generateVariantRowKey(this.chromosome, this.pos, this.ref, this.alt);
    }

    public void addHomeRefCount(Integer cnt) {
        this.homRefCount += cnt;
    }

    public Integer getHomRefCount() {
        return homRefCount;
    }

    public void setHomRefCount(Integer homRefCount) {
        this.homRefCount = homRefCount;
    }

    public Put createPut(VariantTableHelper helper) {
        byte[] generateRowKey = generateRowKey(helper);
        if (this.callMap.containsKey(HOM_REF)) {
            throw new IllegalStateException(
                    String.format("HOM_REF data found for row %s for sample IDs %s",
                            Arrays.toString(generateRowKey), StringUtils.join(this.callMap.get(HOM_REF), ",")));
        }
        byte[] cf = helper.getColumnFamily();
        Integer sid = helper.getStudyId();
        Put put = new Put(generateRowKey);
        put.addColumn(cf, Bytes.toBytes(buildColumnKey(sid, HOM_REF)), Bytes.toBytes(this.homRefCount));
        for (Entry<String, Set<Integer>> entry : this.callMap.entrySet()) {
            byte[] column = Bytes.toBytes(buildColumnKey(sid, entry.getKey()));

            List<Integer> value = new ArrayList<>(entry.getValue());
            if (!value.isEmpty()) {
                Collections.sort(value);
//                byte[] bytesArray = VariantCallProt.newBuilder().addAllSampleIds(value).build().toByteArray();
                byte[] bytesArray = VariantPhoenixHelper.toBytes(value, PUnsignedIntArray.INSTANCE);
                put.addColumn(cf, column, bytesArray);
            }
        }
        return put;
    }

    public static String buildColumnKey(Integer sid, String gt) {
        return String.valueOf(sid) + COLUMN_KEY_SEPARATOR + gt;
    }

    public static Integer extractStudyId(String columnKey) {
        String study = StringUtils.split(columnKey, COLUMN_KEY_SEPARATOR)[0];
        if (StringUtils.isNumeric(study)) {
            return Integer.parseInt(study);
        } else {
            return null;
        }
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
