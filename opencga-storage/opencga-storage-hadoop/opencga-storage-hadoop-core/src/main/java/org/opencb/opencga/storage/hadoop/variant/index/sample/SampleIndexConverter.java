package org.opencb.opencga.storage.hadoop.variant.index.sample;

import org.apache.commons.lang3.tuple.Pair;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.phoenix.schema.types.PInteger;
import org.apache.phoenix.schema.types.PVarchar;
import org.opencb.biodata.models.core.Region;
import org.opencb.biodata.models.variant.Variant;
import org.opencb.biodata.tools.Converter;

import java.util.*;

import static org.apache.hadoop.hbase.util.Bytes.SIZEOF_INT;

/**
 * Created on 18/05/18.
 *
 * @author Jacobo Coll &lt;jacobo167@gmail.com&gt;
 */
public class SampleIndexConverter implements Converter<Result, Collection<Variant>> {

    public static final Comparator<Variant> INTRA_CHROMOSOME_VARIANT_COMPARATOR = Comparator.comparing(Variant::getStart)
            .thenComparing(Variant::getEnd)
            .thenComparing(Variant::getReference)
            .thenComparing(Variant::getAlternate)
            .thenComparing(Variant::toString);

    private static final char META_PREFIX = '_';
    private static final String PENDING_VARIANT_PREFIX = META_PREFIX + "V_";
    private static final byte[] PENDING_VARIANT_PREFIX_BYTES = Bytes.toBytes(PENDING_VARIANT_PREFIX);
    private static final String GENOTYPE_COUNT_PREFIX = META_PREFIX + "C_";

    private final Region region;

    public SampleIndexConverter() {
        this(null);
    }

    public SampleIndexConverter(Region region) {
        this.region = region;
    }

    public static int getExpectedSize(String chromosome) {
        int expectedSize;
        if (chromosome == null) {
            expectedSize = SIZEOF_INT;
        } else {
            expectedSize = SIZEOF_INT + chromosome.length() + 1 + SIZEOF_INT;
        }
        return expectedSize;
    }

    public static byte[] toRowKey(int sample) {
        return toRowKey(sample, null, 0);
    }

    public static byte[] toRowKey(int sample, String chromosome, int position) {
        int expectedSize = getExpectedSize(chromosome);
        byte[] rk = new byte[expectedSize];

        toRowKey(sample, chromosome, position, rk);

        return rk;
    }

    private static int toRowKey(int sample, String chromosome, int position, byte[] rk) {
        int offset = 0;
        offset += PInteger.INSTANCE.toBytes(sample, rk, offset);


        if (chromosome != null) {
            offset += PVarchar.INSTANCE.toBytes(chromosome, rk, offset);
            rk[offset] = 0;
            offset++;
            offset += PInteger.INSTANCE.toBytes(position / SampleIndexDBLoader.BATCH_SIZE, rk, offset);
        }
        return offset;
    }

    public static String rowKeyToString(byte[] row) {
        if (row == null || row.length == 0) {
            return null;
        }
        Object sampleId = PInteger.INSTANCE.toObject(row, 0, 4);
        if (row.length > 5) {
            Object chr = PVarchar.INSTANCE.toObject(row, 4, row.length - 4 - 1 - 4);
            Object pos = PInteger.INSTANCE.toObject(row, row.length - 4, 4);
            return sampleId + "_" + chr + "_" + pos;
        } else {
            return sampleId + "_";
        }
    }

    public static byte[] toGenotypeColumn(String genotype) {
        return Bytes.toBytes(genotype);
    }

    public static byte[] toGenotypeCountColumn(String genotype) {
        return Bytes.toBytes(GENOTYPE_COUNT_PREFIX + genotype);
    }

    public static byte[] toPendingColumn(Variant variant, String gt) {
        return Bytes.toBytes(PENDING_VARIANT_PREFIX + variant.toString() + '_' + gt);
    }

    public static Pair<String, String> parsePendingColumn(byte[] column) {
        if (Bytes.startsWith(column, PENDING_VARIANT_PREFIX_BYTES)) {
            int lastIndexOf = 0;
            for (int i = column.length - 1; i >= 0; i--) {
                if (column[i] == '_') {
                    lastIndexOf = i;
                    break;
                }
            }
            return Pair.of(Bytes.toString(column, PENDING_VARIANT_PREFIX.length(), lastIndexOf - PENDING_VARIANT_PREFIX.length()),
                    Bytes.toString(column, lastIndexOf + 1));
        } else {
            return null;
        }
    }

    @Override
    public Collection<Variant> convert(Result result) {
        Set<Variant> variants = new TreeSet<>(INTRA_CHROMOSOME_VARIANT_COMPARATOR);

        for (Cell cell : result.rawCells()) {
            if (cell.getQualifierArray()[cell.getQualifierOffset()] != META_PREFIX) {
                for (String v : Bytes.toString(cell.getValueArray(), cell.getValueOffset(), cell.getValueLength()).split(",")) {
                    Variant e = new Variant(v);
                    if (region == null || region.contains(e.getChromosome(), e.getStart())) {
                        variants.add(e);
                    }
                }
            }
        }

        return variants;
    }

    public Map<String, List<Variant>> convertToMap(Result result) {
        Map<String, List<Variant>> map = new HashMap<>();
        for (Cell cell : result.rawCells()) {
            String gt = Bytes.toString(CellUtil.cloneQualifier(cell));
            if (gt.charAt(0) != META_PREFIX) {
                List<Variant> variants = getVariants(cell);
                map.put(gt, variants);
            }
        }
        return map;
    }

    public static List<Variant> getVariants(Cell cell) {
        List<Variant> variants;
        byte[] column = CellUtil.cloneQualifier(cell);
        if (column[0] != META_PREFIX) {
            String value = Bytes.toString(cell.getValueArray(), cell.getValueOffset(), cell.getValueLength());

            String[] split = value.split(",");
            variants = new ArrayList<>(split.length);

            for (String v : split) {
                variants.add(new Variant(v));
            }
        } else {
            variants = Collections.emptyList();
        }
        return variants;
    }

    public int convertToCount(Result result) {
        int count = 0;
        for (Cell cell : result.rawCells()) {
            String column = Bytes.toString(CellUtil.cloneQualifier(cell));
            if (column.startsWith(SampleIndexConverter.GENOTYPE_COUNT_PREFIX)) {
                count += Bytes.toInt(cell.getValueArray(), cell.getValueOffset(), cell.getValueLength());
            }
        }
        return count;
    }
}
