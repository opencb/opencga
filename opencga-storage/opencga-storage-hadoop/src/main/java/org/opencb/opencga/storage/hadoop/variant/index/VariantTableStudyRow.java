/**
 *
 */
package org.opencb.opencga.storage.hadoop.variant.index;

import com.google.common.base.Objects;
import com.google.protobuf.InvalidProtocolBufferException;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.phoenix.schema.types.PUnsignedIntArray;
import org.apache.phoenix.schema.types.PhoenixArray;
import org.opencb.biodata.models.feature.Genotype;
import org.opencb.biodata.models.variant.StudyEntry;
import org.opencb.biodata.models.variant.Variant;
import org.opencb.biodata.models.variant.protobuf.VariantProto;
import org.opencb.biodata.models.variant.protobuf.VariantProto.AlternateCoordinate;
import org.opencb.biodata.models.variant.protobuf.VariantProto.VariantType;
import org.opencb.biodata.tools.variant.merge.VariantMerger;
import org.opencb.opencga.storage.hadoop.variant.GenomeHelper;
import org.opencb.opencga.storage.hadoop.variant.index.phoenix.VariantPhoenixHelper;
import org.opencb.opencga.storage.hadoop.variant.models.protobuf.*;
import org.opencb.opencga.storage.hadoop.variant.models.protobuf.ComplexFilter.Builder;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.sql.Array;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.*;
import java.util.Map.Entry;
import java.util.function.Function;
import java.util.stream.Collectors;

import static org.opencb.biodata.tools.variant.merge.VariantMerger.GT_KEY;

/**
 *
 * @author Matthias Haimel mh719+git@cam.ac.uk
 */
public class VariantTableStudyRow {
    public static final String NOCALL = ".";
    public static final String HOM_REF = "0/0";
    public static final String HET_REF = "0/1";
    public static final String HOM_VAR = "1/1";
    public static final String OTHER = "?";
    public static final String COMPLEX = "X";
    public static final String PASS_CNT = "P";
    public static final String FILTER_OTHER = "F";
    public static final String CALL_CNT = "C";

    public static final List<String> STUDY_COLUMNS = Collections.unmodifiableList(
            Arrays.asList(NOCALL, HOM_REF, HET_REF, HOM_VAR, OTHER, COMPLEX, PASS_CNT, CALL_CNT, FILTER_OTHER));
    public static final List<String> GENOTYPE_COLUMNS = Collections.unmodifiableList(Arrays.asList(NOCALL, HET_REF, HOM_VAR, OTHER));

    public static final char COLUMN_KEY_SEPARATOR = '_';

    private Integer studyId;
    private Integer homRefCount = 0;
    private Integer passCount = 0;
    private Integer callCount = 0;
    private final String chromosome;
    private final int pos;
    private final String ref;
    private final String alt;
    private final org.opencb.biodata.models.variant.avro.VariantType type;
    private Map<String, Set<Integer>> callMap = new HashMap<>();
    private Map<Integer, String> sampleToGenotype = new HashMap<>();
    private Map<String, Set<Integer>> filterToSamples = new HashMap<>();
    private List<AlternateCoordinate> secAlternate = new ArrayList<>();

    public VariantTableStudyRow(Integer studyId, String chr, int pos, String ref, String alt,
                                org.opencb.biodata.models.variant.avro.VariantType type) {
        this.studyId = studyId;
        this.chromosome = chr;
        this.pos = pos;
        this.ref = ref;
        this.alt = alt;
        this.type = type;
    }

    public VariantTableStudyRow(VariantTableStudyRow row) {
        this(row.studyId, row.chromosome, row.pos, row.ref, row.alt, row.type);
        this.homRefCount = row.homRefCount;
        this.callCount = row.callCount;
        this.passCount = row.passCount;
        this.callMap.putAll(row.callMap.entrySet().stream().collect(Collectors.toMap(p -> p.getKey(), p -> new HashSet<>(p.getValue()))));
        this.secAlternate.addAll(row.secAlternate != null ? row.secAlternate : Collections.emptyList());
        this.sampleToGenotype.putAll(row.sampleToGenotype != null ? row.sampleToGenotype : Collections.emptyMap());
    }

    public VariantTableStudyRow(VariantTableStudyRowProto proto, String chromosome, Integer studyId) {
        this.studyId = studyId;
        this.chromosome = chromosome;
        this.pos = proto.getStart();
        this.ref = proto.getReference();
        this.alt = proto.getAlternate();
        this.type = toAvro(proto.getType());
        this.callCount = proto.getCallCount();
        this.passCount = proto.getPassCount();
        this.homRefCount = proto.getHomRefCount();
        this.callMap = new HashMap<>(4);
        callMap.put(HOM_VAR, new HashSet<>(proto.getHomVarList()));
        callMap.put(HET_REF, new HashSet<>(proto.getHetList()));
        callMap.put(NOCALL, new HashSet<>(proto.getNocallList()));
        callMap.put(OTHER, new HashSet<>(proto.getOtherList()));
        for (Map.Entry<String, SampleList> entry : proto.getOtherGt().entrySet()) {
            String gt = entry.getKey();
            for (Integer sid : entry.getValue().getSampleIdsList()) {
                sampleToGenotype.put(sid, gt);
            }
        }
        this.filterToSamples = proto.getFilterNonPass().entrySet().stream()
                .collect(Collectors.toMap(k -> k.getKey(), e -> new HashSet<>(e.getValue().getSampleIdsList())));
        this.secAlternate = proto.getSecondaryAlternateList();
    }

    /**
     * Calls {@link #VariantTableStudyRow(Integer, String, int, String, String,
     * org.opencb.biodata.models.variant.avro.VariantType)} using the Variant information.
     * @param studyId Study id
     * @param variant Variant to extrac the region from
     */
    public VariantTableStudyRow(Integer studyId, Variant variant) {
        this(studyId, variant.getChromosome(), variant.getStart(), variant.getReference(), variant.getAlternate(), variant.getType());
    }

    public int getPos() {
        return pos;
    }

    public ComplexFilter getComplexFilter() {
        Builder b = ComplexFilter.newBuilder();
        Map<String, SampleList> map = toSampleListMap(this.filterToSamples);
        b.putAllFilterNonPass(map);
        return b.build();
    }

    private void setComplexFilter(ComplexFilter cf) {
        Map<String, Set<Integer>> map = cf.getFilterNonPass().entrySet().stream().collect(
                Collectors.toMap(e -> e.getKey(), e -> new HashSet<>(e.getValue().getSampleIdsList())));
        this.filterToSamples.putAll(map);
    }

    public ComplexVariant getComplexVariant() {
        return ComplexVariant.newBuilder()
                .putAllSampleToGenotype(this.sampleToGenotype)
                .addAllSecondaryAlternates(this.secAlternate).build();
    }

    public void setComplexVariant(ComplexVariant complexVariant) {
        Map<Integer, String> map = complexVariant.getSampleToGenotype();
        if (map != null && map.size() > 0) {
            this.sampleToGenotype.putAll(map);
        }
        List<AlternateCoordinate> secAlt = complexVariant.getSecondaryAlternatesList();
        if (secAlt != null && !secAlt.isEmpty()) {
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
    public void addSampleId(String gt, Collection<Integer> sampleIds) {
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
    public void addSampleId(String gt, Integer sampleId) {
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

    public String getAlt() {
        return alt;
    }

    public String getRef() {
        return ref;
    }

    public String getChromosome() {
        return chromosome;
    }

    public VariantTableStudyRow setStudyId(Integer studyId) {
        this.studyId = studyId;
        return this;
    }

    /**
     * Fills only changed columns of a PUT object. If no column changed, returns NULL
     * @param helper VariantTableHelper
     * @param newSampleIds Sample IDs which are loaded were not in the original variant
     * @return NULL if no changes, else PUT object with changed columns
     */
    public Put createSpecificPut(VariantTableHelper helper, Set<Integer> newSampleIds) {
        boolean doPut = false;
        byte[] generateRowKey = generateRowKey(helper);
        byte[] cf = helper.getColumnFamily();
        Integer sid = helper.getStudyId();
        Put put = new Put(generateRowKey);
        Set<Integer> newHomRef = new HashSet<>(newSampleIds);

        /***** Complex GT *****/
        Set<Integer> foundIds = this.sampleToGenotype.entrySet().stream().filter(e -> newSampleIds.contains(e.getKey()))
                .map(e -> e.getKey()).collect(Collectors.toSet());
        /***** Secondary Alt list *****/
        // newRow.secAlternate // not needed to filter down //TODO check if new alternate is referenced
        // Function to extract index list of all alleles
        Function<Map.Entry<Integer, String>, Set<Integer>> function = (e) -> Genotype.parse(e.getValue()).stream()
                .flatMap(g -> g.toProtobuf().getAllelesIdxList().stream()).collect(Collectors.toSet());
        Set<Integer> oldIdx = this.sampleToGenotype.entrySet().stream().filter(e -> !newSampleIds.contains(e.getKey()))
                .map(function).flatMap(l -> l.stream()).collect(Collectors.toSet());
        Set<Integer> newIdx = this.sampleToGenotype.entrySet().stream().filter(e -> newSampleIds.contains(e.getKey()))
                .map(function).flatMap(l -> l.stream()).collect(Collectors.toSet());
        newIdx.removeAll(oldIdx);
        if (!newIdx.isEmpty() || !foundIds.isEmpty()) {
            doPut = true;
            put.addColumn(cf, Bytes.toBytes(buildColumnKey(sid, COMPLEX)), this.getComplexVariant().toByteArray());
            newHomRef.removeAll(foundIds);
        }

        /***** Filter *****/
        long cntFilter = this.filterToSamples.entrySet().stream().filter(e -> !Collections.disjoint(e.getValue(), newSampleIds)).count();
        if (cntFilter > 0) {
            doPut = true;
            put.addColumn(cf, Bytes.toBytes(buildColumnKey(sid, FILTER_OTHER)), this.getComplexFilter().toByteArray());
        }
        /**** PASS CNT ***/
        Set<Integer> newPassIds = new HashSet<>(newSampleIds);
        this.filterToSamples.values().forEach(newPassIds::removeAll);
        if (!newPassIds.isEmpty()) {
            doPut = true;
            put.addColumn(cf, Bytes.toBytes(buildColumnKey(sid, PASS_CNT)), Bytes.toBytes(this.passCount));
        }

        /**** GT ***/
        Set<Integer> newCalls = new HashSet<>(newSampleIds);
        for (Entry<String, Set<Integer>> entry : this.callMap.entrySet()) {
            byte[] column = Bytes.toBytes(buildColumnKey(sid, entry.getKey()));
            boolean disjoint = Collections.disjoint(entry.getValue(), newSampleIds);
            if (!disjoint) {
                doPut = true;
                List<Integer> value = new ArrayList<>(entry.getValue());
                Collections.sort(value);
                byte[] bytesArray = VariantPhoenixHelper.toBytes(value, PUnsignedIntArray.INSTANCE);
                put.addColumn(cf, column, bytesArray);
                newHomRef.removeAll(value);
                if (StringUtils.equals(entry.getKey(), NOCALL)) {
                    newCalls.removeAll(value);
                }
            }
        }
        if (!newHomRef.isEmpty()) {
            doPut = true;
            put.addColumn(cf, Bytes.toBytes(buildColumnKey(sid, HOM_REF)), Bytes.toBytes(this.homRefCount));
        }

        if (!this.callCount.equals(newCalls.size())) {
            doPut = true;
            put.addColumn(cf, Bytes.toBytes(buildColumnKey(sid, CALL_CNT)), Bytes.toBytes(this.callCount));
        }

        if (this.callMap.containsKey(HOM_REF)) {
            throw new IllegalStateException(
                    String.format("HOM_REF data found for row %s for sample IDs %s",
                            Arrays.toString(generateRowKey), StringUtils.join(this.callMap.get(HOM_REF), ",")));
        }
        if (doPut) {
            return put;
        }
        return null;
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
        put.addColumn(cf, VariantPhoenixHelper.VariantColumn.TYPE.bytes(), Bytes.toBytes(this.type.toString()));
        put.addColumn(cf, Bytes.toBytes(buildColumnKey(sid, HOM_REF)), Bytes.toBytes(this.homRefCount));
        put.addColumn(cf, Bytes.toBytes(buildColumnKey(sid, PASS_CNT)), Bytes.toBytes(this.passCount));
        put.addColumn(cf, Bytes.toBytes(buildColumnKey(sid, CALL_CNT)), Bytes.toBytes(this.callCount));
        if (!this.secAlternate.isEmpty() || this.sampleToGenotype.size() > 0) { //add complex genotype column if required
            put.addColumn(cf, Bytes.toBytes(buildColumnKey(sid, COMPLEX)), this.getComplexVariant().toByteArray());
        }
        if (!this.filterToSamples.isEmpty()) {
            put.addColumn(cf, Bytes.toBytes(buildColumnKey(sid, FILTER_OTHER)), this.getComplexFilter().toByteArray());
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

    public Delete createDelete(VariantTableHelper helper) {
        byte[] generateRowKey = generateRowKey(helper);
        byte[] cf = helper.getColumnFamily();
        Integer sid = helper.getStudyId();
        Delete delete = new Delete(generateRowKey);
        for (String key : STUDY_COLUMNS) {
            delete.addColumn(cf, Bytes.toBytes(buildColumnKey(sid, key)));
        }
        return delete;
    }

    public static VariantTableStudyRowsProto toProto(List<VariantTableStudyRow> rows, long timeStamp) {
        return VariantTableStudyRowsProto.newBuilder()
                .addAllRows(rows.stream().map(VariantTableStudyRow::toProto).collect(Collectors.toList()))
                .setTimestamp(timeStamp)
                .build();
    }

    public VariantTableStudyRowProto toProto() {
        Map<String, List<Integer>> otherGt = new HashMap<>();
        for (Entry<Integer, String> entry : sampleToGenotype.entrySet()) {
            String gt = entry.getValue();
            List<Integer> samples = otherGt.get(gt);
            if (samples == null) {
                samples = new LinkedList<>();
                otherGt.put(gt, samples);
            }
            samples.add(entry.getKey());
        }
        return VariantTableStudyRowProto.newBuilder()
                .setStart(pos)
                .setReference(ref)
                .setAlternate(alt)
                .setType(toProto(type))
                .setCallCount(callCount)
                .setPassCount(passCount)
                .setHomRefCount(homRefCount)
                .addAllHomVar(callMap.getOrDefault(HOM_VAR, Collections.emptySet()))
                .addAllHet(callMap.getOrDefault(HET_REF, Collections.emptySet()))
                .addAllNocall(callMap.getOrDefault(NOCALL, Collections.emptySet()))
                .addAllOther(callMap.getOrDefault(OTHER, Collections.emptySet()))
                .addAllSecondaryAlternate(secAlternate)
                .putAllOtherGt(toSampleListMap(otherGt))
                .putAllFilterNonPass(toSampleListMap(this.filterToSamples))
                .build();
    }

    public VariantType toProto(org.opencb.biodata.models.variant.avro.VariantType type) {
        return VariantType.valueOf(type.toString());
    }

    public org.opencb.biodata.models.variant.avro.VariantType toAvro(VariantType type) {
        return org.opencb.biodata.models.variant.avro.VariantType.valueOf(type.toString());
    }

    private Map<String, SampleList> toSampleListMap(Map<String, ? extends Collection<Integer>> map) {
        return map.entrySet().stream()
                .collect(Collectors.toMap(
                        Entry::getKey,
                        entry -> SampleList.newBuilder().addAllSampleIds(entry.getValue()).build()));
    }

    public static List<VariantTableStudyRow> parse(Result result, GenomeHelper helper) {
        NavigableMap<byte[], byte[]> familyMap = result.getFamilyMap(helper.getColumnFamily());
        Set<Integer> studyIds = familyMap.entrySet().stream()
                .filter(entry -> entry.getValue() != null && entry.getValue().length > 0)
                .map(entry -> extractStudyId(Bytes.toString(entry.getKey()), false))
                .filter(integer -> integer != null)
                .collect(Collectors.toSet());

        if (studyIds.isEmpty()) {
            throw new IllegalStateException("No studies found!!!");
        }
        List<VariantTableStudyRow> rows = new ArrayList<>(studyIds.size());
        for (Integer studyId : studyIds) {
            Variant variant = helper.extractVariantFromVariantRowKey(result.getRow());
            rows.add(new VariantTableStudyRow(variant, studyId, familyMap, true));
        }
        return rows;
    }

    public VariantTableStudyRow(Variant variant, Integer studyId, NavigableMap<byte[], byte[]> familyMap,
                boolean skipOtherStudies) {
            this(studyId, variant);
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
                    homRefCount = parseCount(entry.getValue());
                    break;
                case CALL_CNT:
                    callCount = parseCount(entry.getValue());
                    break;
                case PASS_CNT:
                    passCount = parseCount(entry.getValue());
                    break;
                case COMPLEX:
                    try {
                        ComplexVariant complexVariant = ComplexVariant.parseFrom(entry.getValue());
                        setComplexVariant(complexVariant);
                    } catch (InvalidProtocolBufferException e) {
                        throw new UncheckedIOException(e);
                    }
                    break;
                case FILTER_OTHER:
                    try {
                        ComplexFilter complexFilter = ComplexFilter.parseFrom(entry.getValue());
                        setComplexFilter(complexFilter);
                    } catch (InvalidProtocolBufferException e) {
                        throw new UncheckedIOException(e);
                    }
                    break;
                default:
                    PhoenixArray phoenixArray = (PhoenixArray) PUnsignedIntArray.INSTANCE.toObject(entry.getValue());
                    try {
                        HashSet<Integer> value = new HashSet<>();
                        if (phoenixArray.getArray() != null) {
                            int[] array = (int[]) phoenixArray.getArray();
                            for (int i : array) {
                                value.add(i);
                            }
                        }
                        callMap.put(gt, value);
                    } catch (SQLException e) {
                        //Impossible
                        throw new IllegalStateException(e);
                    }
                    break;
                }
            }
        }

    public static List<VariantTableStudyRow> parse(Variant variant, ResultSet resultSet, GenomeHelper helper) throws SQLException {
        ResultSetMetaData metaData = resultSet.getMetaData();
        Set<Integer> studyIds = new HashSet<>();
        for (int i = 0; i < metaData.getColumnCount(); i++) {
            String columnName = metaData.getColumnName(i + 1);
            if (columnName != null && !columnName.isEmpty()) {
                if (resultSet.getBytes(columnName) != null) {
                    Integer studyId = extractStudyId(columnName, false);
                    if (studyId != null) {
                        studyIds.add(studyId);
                    }
                }
            }
        }
        List<VariantTableStudyRow> rows = new ArrayList<>(studyIds.size());
        for (Integer studyId : studyIds) {
            rows.add(new VariantTableStudyRow(variant, resultSet, studyId));
        }
        return rows;
    }

    /**
     * Parse Phoenix ResultSet.
     * @param variant Variant to create {@link VariantTableStudyRow#VariantTableStudyRow(Integer, Variant)} with
     * @param resultSet Phoenix result set
     * @param studyId Study id
     * @throws SQLException Problems accessing data in {@link ResultSet}
     */
    public VariantTableStudyRow(Variant variant, ResultSet resultSet, int studyId) throws SQLException {
        this(studyId, variant);
        homRefCount = resultSet.getInt(buildColumnKey(studyId, HOM_REF));
        callCount = resultSet.getInt(buildColumnKey(studyId, CALL_CNT));
        passCount = resultSet.getInt(buildColumnKey(studyId, PASS_CNT));
        byte[] xArr = resultSet.getBytes(buildColumnKey(studyId, COMPLEX));
        if (xArr != null && xArr.length > 0) {
            try {
                ComplexVariant complexVariant = ComplexVariant.parseFrom(xArr);
                setComplexVariant(complexVariant);
            } catch (IOException e) {
                throw new UncheckedIOException(e);
            }
        }
        byte[] fArr = resultSet.getBytes(buildColumnKey(studyId, FILTER_OTHER));
        if (fArr != null && fArr.length > 0) {
            try {
                ComplexFilter complexFilter = ComplexFilter.parseFrom(fArr);
                setComplexFilter(complexFilter);
            } catch (IOException e) {
                throw new UncheckedIOException(e);
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
            callMap.put(gt, value);
        }
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

    /**
     * Creates a new VariantTableStudyRow from a single Variant object.
     *
     * @param variant The variant to convert
     * @param studyId Study identifier
     * @param sampleIds Sample id mapping
     */
    public VariantTableStudyRow(Variant variant, Integer studyId, Map<String, Integer> sampleIds) {
        this(studyId, variant);
        int[] homRef = new Genotype("0/0").getAllelesIdx();
        int[] hetRef = new Genotype("0/1").getAllelesIdx();
        int[] hetRefOther = new Genotype("1|0").getAllelesIdx();
        int[] homVar = new Genotype("1/1").getAllelesIdx();
        int[] nocall = new Genotype(".").getAllelesIdx();
        int[] nocallBoth = new Genotype("./.").getAllelesIdx();

        Set<Integer> homref = new HashSet<>();
        StudyEntry se = variant.getStudy(studyId.toString());
        if (null == se) {
            throw new IllegalStateException("Study Entry of variant is null: " + variant);
        }
        // PASS flag should now be populated
//        if (se.getFiles() != null && !se.getFiles().isEmpty() && se.getFiles().get(0) != null
//                && se.getFiles().get(0).getAttributes() != null && !se.getFiles().get(0).getAttributes().isEmpty()) {
//            String passStr = se.getFiles().get(0).getAttributes().getOrDefault(VariantMerger.VCF_FILTER, "0");
//            setPassCount(Integer.valueOf(passStr));
//        }
        try {
            Set<String> sampleSet = se.getSamplesName();
            // Create Secondary index
            List<VariantProto.AlternateCoordinate> arr = Collections.emptyList();
            if (null != se.getSecondaryAlternates() && se.getSecondaryAlternates().size() > 0) {
                arr = new ArrayList<>(se.getSecondaryAlternates().size());
                for (org.opencb.biodata.models.variant.avro.AlternateCoordinate altCoord : se.getSecondaryAlternates()) {

                    VariantProto.AlternateCoordinate.Builder ac = AlternateCoordinate.newBuilder();
                    ac.setChromosome(Objects.firstNonNull(altCoord.getChromosome(), ""))
                            .setStart(Objects.firstNonNull(altCoord.getStart(), 0))
                            .setEnd(Objects.firstNonNull(altCoord.getEnd(), 0))
                            .setReference(Objects.firstNonNull(altCoord.getReference(), ""))
                            .setAlternate(Objects.firstNonNull(altCoord.getAlternate(), ""));
                    VariantType vt = toProto(altCoord.getType());
                    ac.setType(vt);
                    arr.add(ac.build());
                }
                secAlternate = arr;
            }

            for (String sample : sampleSet) {
                Integer sid = sampleIds.get(sample);
                // Work out Genotype
                String gtStr = se.getSampleData(sample, GT_KEY);
                List<Genotype> gtLst = Genotype.parse(gtStr);
                if (gtLst.isEmpty()) {
                    // No GT found for this individual
                    throw new IllegalStateException("No GT found for " + sample + ": "  + variant.toJson());
                } else if (gtLst.size() == 1) {
                    Genotype gt = gtLst.get(0);
                    int[] alleleIdx = gt.getAllelesIdx();
                    if (Arrays.equals(alleleIdx, homRef)) {
                        addCallCount(1);
                        if (!homref.add(sid)) {
                            throw new IllegalStateException("Sample already exists as hom_ref " + sample);
                        }
                    } else if (Arrays.equals(alleleIdx, hetRef) || Arrays.equals(alleleIdx, hetRefOther)) {
                        addSampleId(HET_REF, sid);
                        addCallCount(1);
                    } else if (Arrays.equals(alleleIdx, homVar)) {
                        addSampleId(HOM_VAR, sid);
                        addCallCount(1);
                    } else if (Arrays.equals(alleleIdx, nocall) || Arrays.equals(alleleIdx, nocallBoth)) {
                        addSampleId(NOCALL, sid);
                    } else {
                        addSampleId(OTHER, sid);
                        addCallCount(1);
                        sampleToGenotype.put(sid, gtStr);
                    }
                } else {
                    addSampleId(OTHER, sid);
                    addCallCount(1);
                    sampleToGenotype.put(sid, gtStr);
                }
                // Work out PASS / CALL count
                // Samples from Archive table have PASS/etc set. From Analysis table, the flag is empty (already counted)
                String filterString = se.getSampleData(sample, VariantMerger.VCF_FILTER);
                if (StringUtils.equals("PASS", filterString)) {
                    addPassCount(1);
                } else { // Must count missing filter values!
                    if (StringUtils.isBlank(filterString) || StringUtils.equals("-", filterString)) {
                        filterString = "."; // Blank and '-' filters are saved together as missing
                    }
                    Set<Integer> set = filterToSamples.get(filterString);
                    if (set == null) {
                        set = new HashSet<>();
                        filterToSamples.put(filterString, set);
                    }
                    set.add(sid);
                }
            }
            addHomeRefCount(homref.size());
        } catch (RuntimeException e) {
            throw new RuntimeException("Problems with " + variant.toJson(), e);
        }
    }

    @Override
    public String toString() {
        return chromosome + ':' + pos + ':' + ref + ':' + alt;
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
