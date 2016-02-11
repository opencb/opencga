/**
 *
 */
package org.opencb.opencga.storage.hadoop.variant.index;

import static org.opencb.biodata.tools.variant.merge.VariantMerger.GT_KEY;
import static org.opencb.biodata.tools.variant.merge.VariantMerger.PASS_KEY;

import java.io.IOException;
import java.sql.Array;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.NavigableMap;
import java.util.Set;
import java.util.stream.Collectors;

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.phoenix.schema.types.PUnsignedIntArray;
import org.apache.phoenix.schema.types.PhoenixArray;
import org.opencb.biodata.models.feature.Genotype;
import org.opencb.biodata.models.variant.StudyEntry;
import org.opencb.biodata.models.variant.Variant;
import org.opencb.biodata.models.variant.protobuf.VariantAnalysisProtos.ComplexVariant;
import org.opencb.biodata.models.variant.protobuf.VariantProto;
import org.opencb.biodata.models.variant.protobuf.VariantProto.AlternateCoordinate;
import org.opencb.biodata.models.variant.protobuf.VariantProto.VariantType;
import org.opencb.biodata.tools.variant.merge.VariantMerger;
import org.opencb.opencga.storage.hadoop.variant.GenomeHelper;
import org.opencb.opencga.storage.hadoop.variant.index.phoenix.VariantPhoenixHelper;

import com.google.protobuf.InvalidProtocolBufferException;

/**
 * @author Matthias Haimel mh719+git@cam.ac.uk.
 */
public class VariantTableStudyRow {
    public static final String NOCALL = ".";
    public static final String HOM_REF = "0/0";
    public static final String HET_REF = "0/1";
    public static final String HOM_VAR = "1/1";
    public static final String OTHER = "?";
    public static final String COMPLEX = "X";
    public static final String PASS_CNT = "P";
    public static final String CALL_CNT = "C";

    public static final List<String> STUDY_COLUMNS = Collections.unmodifiableList(
            Arrays.asList(NOCALL, HOM_REF, HET_REF, HOM_VAR, OTHER, COMPLEX, PASS_CNT, CALL_CNT));

    public static final char COLUMN_KEY_SEPARATOR = '_';

    private Integer studyId;
    private Integer homRefCount = 0;
    private Integer passCount = 0;
    private Integer callCount = 0;
    private String chromosome;
    private int pos;
    private String ref;
    private String alt;
    private Map<String, Set<Integer>> callMap = new HashMap<>();
    private Map<Integer, VariantProto.Genotype> sampleToGenotype = new HashMap<>();
    private List<AlternateCoordinate> secAlternate = new ArrayList<>();

    public VariantTableStudyRow(Integer studyId, String chr, int pos, String ref, String alt) {
        this.studyId = studyId;
        this.chromosome = chr;
        this.pos = pos;
        this.ref = ref;
        this.alt = alt;
    }

    public VariantTableStudyRow(VariantTableStudyRow row) {
        this(row.studyId, row.chromosome, row.pos, row.ref, row.alt);
        this.homRefCount = row.homRefCount;
        this.callCount = row.callCount;
        this.passCount = row.passCount;
        this.callMap.putAll(row.callMap.entrySet().stream().collect(Collectors.toMap(p -> p.getKey(), p -> new HashSet<>(p.getValue()))));
        this.secAlternate.addAll(row.secAlternate != null ? row.secAlternate : Collections.emptyList());
        this.sampleToGenotype.putAll(row.sampleToGenotype != null ? row.sampleToGenotype : Collections.emptyMap());
    }

    /**
     * Calls {@link #VariantTableStudyRow(Integer, String, int, String, String)} using the Variant information.
     * @param studyId Study id
     * @param variant Variant to extrac the region from
     */
    public VariantTableStudyRow(Integer studyId, Variant variant) {
        this(studyId, variant.getChromosome(), variant.getStart(), variant.getReference(), variant.getAlternate());
    }

    public int getPos() {
        return pos;
    }

    public ComplexVariant getComplexVariant() {
        return ComplexVariant.newBuilder()
                .putAllSampleToGenotype(this.sampleToGenotype)
                .addAllSecondaryAlternates(this.secAlternate).build();
    }

    public void setComplexVariant(ComplexVariant complexVariant) {
        Map<Integer, VariantProto.Genotype> map = complexVariant.getSampleToGenotype();
        if (map != null && map.size() > 0) {
            this.sampleToGenotype.putAll(map);
        }
        List<AlternateCoordinate> secAlt = complexVariant.getSecondaryAlternatesList();
        if (secAlt != null && secAlt.size() > 0) {
            this.secAlternate.addAll(secAlt);
        }
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
     * @param gt       Genotype code for the samples
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

    public void addPassCount(Integer cnt) {
        passCount += cnt;
    }

    public Integer getPassCount() {
        return passCount;
    }

    public void setPassCount(Integer passCount) {
        this.passCount = passCount;
    }

    public void addCallCount(Integer cnt) {
        callCount += cnt;
    }

    public void setCallCount(Integer callCount) {
        this.callCount = callCount;
    }

    public Integer getCallCount() {
        return callCount;
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
        put.addColumn(cf, Bytes.toBytes(buildColumnKey(sid, PASS_CNT)), Bytes.toBytes(this.passCount));
        put.addColumn(cf, Bytes.toBytes(buildColumnKey(sid, CALL_CNT)), Bytes.toBytes(this.callCount));
        if (this.secAlternate.size() > 0 || this.sampleToGenotype.size() > 0) { //add complex genotype column if required
            put.addColumn(cf, Bytes.toBytes(buildColumnKey(sid, COMPLEX)), this.getComplexVariant().toByteArray());
        }
        for (Entry<String, Set<Integer>> entry : this.callMap.entrySet()) {
            byte[] column = Bytes.toBytes(buildColumnKey(sid, entry.getKey()));

            List<Integer> value = new ArrayList<>(entry.getValue());
            if (!value.isEmpty()) {
                Collections.sort(value);
                byte[] bytesArray = VariantPhoenixHelper.toBytes(value, PUnsignedIntArray.INSTANCE);
                put.addColumn(cf, column, bytesArray);
            }
        }
        return put;
    }

    public static List<VariantTableStudyRow> parse(Result result, GenomeHelper helper) {
        NavigableMap<byte[], byte[]> familyMap = result.getFamilyMap(helper.getColumnFamily());
        Set<Integer> studyIds = familyMap.entrySet().stream()
                .filter(entry -> entry.getValue() != null && entry.getValue().length > 0)
                .map(entry -> extractStudyId(Bytes.toString(entry.getKey()), true))
                .filter(integer -> integer != null)
                .collect(Collectors.toSet());

        if (studyIds.isEmpty()) {
            throw new IllegalStateException("No studies found!!!");
        }
        List<VariantTableStudyRow> rows = new ArrayList<>(studyIds.size());
        for (Integer studyId : studyIds) {
            Variant variant = helper.extractVariantFromVariantRowKey(result.getRow());
            rows.add(parse(variant, studyId, familyMap, true));
        }
        return rows;
    }

    public static VariantTableStudyRow parse(Variant variant, Integer studyId, NavigableMap<byte[], byte[]> familyMap,
                boolean skipOtherStudies) {
            VariantTableStudyRow row = new VariantTableStudyRow(studyId, variant);
            for (Entry<byte[], byte[]> entry : familyMap.entrySet()) {
                if (entry.getValue() == null || entry.getValue().length == 0) {
                    continue; // use default values, if no data for column exist
                }
                String colStr = Bytes.toString(entry.getKey());
                String[] colSplit = colStr.split("_", 2);
                if (!colSplit[0].equals(studyId.toString())) { // check study ID for consistency check
                    if (skipOtherStudies) {
                        continue;
                    } else {
                        throw new IllegalStateException(String.format("Expected study id %s, but found %s in row %s",
                                studyId.toString(), colSplit[0], colStr));
                    }
                }
                String gt = colSplit[1];
                switch (gt) {
                case HOM_REF:
                    row.homRefCount = parseCount(entry.getValue());
                    break;
                case CALL_CNT:
                    row.callCount = parseCount(entry.getValue());
                    break;
                case PASS_CNT:
                    row.passCount = parseCount(entry.getValue());
                    break;
                case COMPLEX:
                    try {
                        ComplexVariant complexVariant = ComplexVariant.parseFrom(entry.getValue());
                        row.setComplexVariant(complexVariant);
                    } catch (InvalidProtocolBufferException e) {
                        throw new RuntimeException(e);
                    }
                    break;
                default:
                    PhoenixArray phoenixArray = ((PhoenixArray) PUnsignedIntArray.INSTANCE.toObject(entry.getValue()));
                    try {
                        HashSet<Integer> value = new HashSet<>();
                        if (phoenixArray.getArray() != null) {
                            int[] array = (int[]) phoenixArray.getArray();
                            for (int i : array) {
                                value.add(i);
                            }
                        }
                        row.callMap.put(gt, value);
                    } catch (SQLException e) {
                        //Impossible
                        throw new RuntimeException(e);
                    }
                    break;
                }
            }
            return row;
        }

    public static List<VariantTableStudyRow> parse(Variant variant, ResultSet resultSet, GenomeHelper helper) throws SQLException {
        ResultSetMetaData metaData = resultSet.getMetaData();
        Set<Integer> studyIds = new HashSet<>();
        for (int i = 0; i < metaData.getColumnCount(); i++) {
            String columnName = metaData.getColumnName(i + 1);
            if (columnName != null && !columnName.isEmpty()) {
                Integer studyId = extractStudyId(columnName, false);
                if (studyId != null) {
                    studyIds.add(studyId);
                }
            }
        }
        List<VariantTableStudyRow> rows = new ArrayList<>(studyIds.size());
        for (Integer studyId : studyIds) {
            rows.add(parse(variant, resultSet, studyId));
        }
        return rows;
    }

    /**
     * Parse Phoenix ResultSet.
     * @param variant Variant to create {@link VariantTableStudyRow#VariantTableStudyRow(Integer, Variant)} with
     * @param resultSet Phoenix result set
     * @param studyId Study id
     * @return variantTableStudyRow {@link VariantTableStudyRow} object filled with data
     * @throws SQLException Problems accessing data in {@link ResultSet}
     */
    public static VariantTableStudyRow parse(Variant variant, ResultSet resultSet, int studyId) throws SQLException {
        VariantTableStudyRow row = new VariantTableStudyRow(studyId, variant);
        row.homRefCount = resultSet.getInt(buildColumnKey(studyId, HOM_REF));
        row.callCount = resultSet.getInt(buildColumnKey(studyId, CALL_CNT));
        row.passCount = resultSet.getInt(buildColumnKey(studyId, PASS_CNT));
        byte[] xArr = resultSet.getBytes(buildColumnKey(studyId, COMPLEX));
        if (xArr != null && xArr.length > 0) {
            try {
                ComplexVariant complexVariant = ComplexVariant.parseFrom(xArr);
                row.setComplexVariant(complexVariant);
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }
        for (String gt : new String[] { HET_REF, HOM_VAR, OTHER, NOCALL }) {
            Array sqlArray = resultSet.getArray(buildColumnKey(studyId, gt));
            HashSet<Integer> value = new HashSet<>();
            if (sqlArray != null && sqlArray.getArray() != null) {
                int[] array = (int[]) sqlArray.getArray();
                for (int i : array) {
                    value.add(i);
                }
            }
            row.callMap.put(gt, value);
        }
        return row;
    }

    private static Integer parseCount(byte[] value) {
        if (value == null || value.length == 0) {
            return 0;
        } else {
            return Bytes.toInt(value);
        }
    }

    public static String buildColumnKey(Integer sid, String gt) {
        return String.valueOf(sid) + COLUMN_KEY_SEPARATOR + gt;
    }

    public static Integer extractStudyId(String columnKey, boolean failOnMissing) {
        String study = StringUtils.split(columnKey, COLUMN_KEY_SEPARATOR)[0];
        if (StringUtils.isNumeric(study)) {
            return Integer.parseInt(study);
        } else {
            if (failOnMissing) {
                throw new IllegalStateException(String.format("Integer expected for study ID: extracted %s from %s ", study, columnKey));
            } else {
                return null;
            }
        }
    }

    public static VariantTableStudyRow build(Variant variant, Integer studyId, Map<String, Integer> idMapping) {
        int[] homRef = new Genotype("0/0").getAllelesIdx();
        int[] hetRef = new Genotype("0/1").getAllelesIdx();
        int[] hetRefOther = new Genotype("1|0").getAllelesIdx();
        int[] homVar = new Genotype("1/1").getAllelesIdx();
        int[] nocall = new Genotype(".").getAllelesIdx();
        int[] nocallBoth = new Genotype("./.").getAllelesIdx();

        Set<Integer> homref = new HashSet<Integer>();
        VariantTableStudyRow row = new VariantTableStudyRow(studyId, variant);
        StudyEntry se = variant.getStudy(studyId.toString());
        if (null == se) {
            throw new IllegalStateException("Study Entry of variant is null: " + variant);
        }
        if (null != se.getAllAttributes()) { // SET PASS counts
            String passStr = se.getFiles().get(0).getAttributes().getOrDefault(PASS_KEY, "0");
            row.setPassCount(Integer.valueOf(passStr));
        }
        Set<String> sampleSet = se.getSamplesName();
        // Create Secondary index
        List<VariantProto.AlternateCoordinate> arr = Collections.emptyList();
        if (null != se.getSecondaryAlternates() && se.getSecondaryAlternates().size() > 0) {
            arr = new ArrayList<>(se.getSecondaryAlternates().size());
            for (org.opencb.biodata.models.variant.avro.AlternateCoordinate altCoord : se.getSecondaryAlternates()) {
                VariantProto.AlternateCoordinate.Builder ac = AlternateCoordinate.newBuilder();
                ac.setChromosome(altCoord.getChromosome())
                    .setStart(altCoord.getStart())
                    .setEnd(altCoord.getEnd())
                    .setReference(altCoord.getReference())
                    .setAlternate(altCoord.getAlternate());
                VariantType vt = VariantType.valueOf(altCoord.getType().name());
                ac.setType(vt);
                arr.add(ac.build());
            }
            row.secAlternate = arr;
        }

        for (String sample : sampleSet) {
            Integer sid = idMapping.get(sample);
            // Work out Genotype
            String gtStr = se.getSampleData(sample, GT_KEY);
            Genotype gt = new Genotype(gtStr);
            int[] alleleIdx = gt.getAllelesIdx();
            if (Arrays.equals(alleleIdx, homRef)) {
                row.addCallCount(1);
                if (!homref.add(sid)) {
                    throw new IllegalStateException("Sample already exists as hom_ref " + sample);
                }
            } else if (Arrays.equals(alleleIdx, hetRef) || Arrays.equals(alleleIdx, hetRefOther)) {
                row.addSampleId(HET_REF, sid);
                row.addCallCount(1);
            } else if (Arrays.equals(alleleIdx, homVar)) {
                row.addSampleId(HOM_VAR, sid);
                row.addCallCount(1);
            } else if (Arrays.equals(alleleIdx, nocall) || Arrays.equals(alleleIdx, nocallBoth)) {
                row.addSampleId(NOCALL, sid);
            } else {
                row.addSampleId(OTHER, sid);
                row.addCallCount(1);
                row.sampleToGenotype.put(sid, gt.toProtobuf());
            }
            // Work out PASS / CALL count
            // Samples from Archive table have PASS/etc set. From Analysis table, the flag is empty (already counted)
            if (StringUtils.equals("PASS", se.getSampleData(sample, VariantMerger.PASS_KEY))) {
                row.addPassCount(1);
            }
        }
        row.addHomeRefCount(homref.size());
        return row;
    }

    public String toSummaryString() {
        return String.format(
                "Submit %s: pass: %s; call: %s; hr: %s; 0/1: %s; 1/1: %s; ?: %s; .: %s",
                getPos(),
                getPassCount(),
                getCallCount(),
                getHomRefCount(),
                Arrays.toString(getSampleIds(HET_REF).toArray()),
                Arrays.toString(getSampleIds(HOM_VAR).toArray()),
                Arrays.toString(getSampleIds(OTHER).toArray()),
                Arrays.toString(getSampleIds(NOCALL).toArray())
        );
    }

}
