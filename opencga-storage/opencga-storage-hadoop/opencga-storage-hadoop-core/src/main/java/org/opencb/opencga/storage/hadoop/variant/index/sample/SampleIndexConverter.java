package org.opencb.opencga.storage.hadoop.variant.index.sample;

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

    protected static byte[] toRowKey(int sample) {
        return toRowKey(sample, null, 0);
    }

    protected static byte[] toRowKey(int sample, String chromosome, int position) {
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

    @Override
    public Collection<Variant> convert(Result result) {
        Set<Variant> variants = new TreeSet<>(Comparator.comparingInt(Variant::getStart).thenComparing(Variant::toString));

        for (Cell cell : result.rawCells()) {
            for (String v : Bytes.toString(cell.getValueArray(), cell.getValueOffset(), cell.getValueLength()).split(",")) {
                Variant e = new Variant(v);
                if (region == null || region.contains(e.getChromosome(), e.getStart())) {
                    variants.add(e);
                }
            }
        }

        return variants;
    }

    public Map<String, List<Variant>> convertToMap(Result result) {
        Map<String, List<Variant>> map = new HashMap<>();
        for (Cell cell : result.rawCells()) {
            String gt = Bytes.toString(CellUtil.cloneQualifier(cell));
            String[] split = Bytes.toString(cell.getValueArray(), cell.getValueOffset(), cell.getValueLength()).split(",");
            ArrayList<Variant> variants = new ArrayList<>(split.length);
            map.put(gt, variants);
            for (String v : split) {
                variants.add(new Variant(v));
            }
        }
        return map;
    }
}
