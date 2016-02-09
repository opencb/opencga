/**
 * Converter to convert Archive Result to Variants
 */
package org.opencb.opencga.storage.hadoop.variant.index;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.NavigableMap;
import java.util.stream.Collectors;

import org.apache.hadoop.hbase.client.Result;
import org.opencb.biodata.models.variant.StudyEntry;
import org.opencb.biodata.models.variant.Variant;
import org.opencb.biodata.models.variant.avro.VariantType;
import org.opencb.biodata.models.variant.protobuf.VcfMeta;
import org.opencb.biodata.models.variant.protobuf.VcfSliceProtos.VcfSlice;
import org.opencb.biodata.tools.variant.converter.VcfSliceToVariantListConverter;
import org.opencb.opencga.storage.hadoop.variant.archive.ArchiveHelper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.protobuf.InvalidProtocolBufferException;

/**
 * @author Matthias Haimel mh719+git@cam.ac.uk
 *
 */
public class ArchiveResultToVariantConverter {
    private final Logger LOG = LoggerFactory.getLogger(ArchiveResultToVariantConverter.class);
    private byte[] columnFamily;
    private HashMap<Integer, VcfMeta> metaIdx;
    private Map<Integer, VcfSliceToVariantListConverter> fileidToConverter = new HashMap<Integer, VcfSliceToVariantListConverter>();

    public ArchiveResultToVariantConverter(Map<Integer, VcfMeta> metaIdx, byte[] columnFamily) {
        this.columnFamily = columnFamily;
        this.metaIdx = new HashMap<Integer, VcfMeta>(metaIdx);
    }

    public List<Variant> convert(Result value, Long start, Long end, boolean resolveConflict) throws InvalidProtocolBufferException {
        return filter(convert(value, resolveConflict), start, end);
    }

    private List<Variant> filter(List<Variant> variantList, Long start, Long end) {
        return variantList.stream() // only keep variants in
                // overlapping this region
                .filter(v -> variantCoveringRegion(v, start, end, true)).collect(Collectors.toList());
    }

    public static boolean variantCoveringRegion(Variant v, Long start, Long end, boolean inclusive) {
        int iStart = start.intValue();
        int iEnd = end.intValue();
        if (inclusive) {
            return iEnd >= v.getStart() && iStart <= v.getEnd();
        } else {
            return iEnd > v.getStart() && iStart < v.getEnd();
        }
    }

    public List<Variant> convert(Result value, boolean resolveConflict) throws InvalidProtocolBufferException {
        List<Variant> variantList = new ArrayList<>();
        NavigableMap<byte[], byte[]> fm = value.getFamilyMap(columnFamily);
        for (Entry<byte[], byte[]> x : fm.entrySet()) {
            // for each FILE (column in HBase
            List<Variant> varList = archiveCellToVariants(x.getKey(), x.getValue());
            if (resolveConflict) {
                varList = resolveConflicts(varList);
            }
            LOG.info(String.format("For Column %s found %s entries", ArchiveHelper.getFileIdFromColumnName(x.getKey()), varList.size()));
            variantList.addAll(varList);
        }
        return variantList;
    }

    private List<Variant> archiveCellToVariants(byte[] key, byte[] value) throws InvalidProtocolBufferException {
        int fileId = ArchiveHelper.getFileIdFromColumnName(key);
        VcfSliceToVariantListConverter converter = loadConverter(fileId);
        VcfSlice vcfSlice = asSlice(value);
        return converter.convert(vcfSlice);
    }

    private VcfSliceToVariantListConverter loadConverter(int fileId) {
        VcfSliceToVariantListConverter converter = fileidToConverter.get(fileId);
        if (converter == null) {
            converter = new VcfSliceToVariantListConverter(this.metaIdx.get(fileId));
            fileidToConverter.put(fileId, converter);
        }
        return converter;
    }

    private VcfSlice asSlice(byte[] data) throws InvalidProtocolBufferException {
        return VcfSlice.parseFrom(data);
    }

    public static List<Variant> resolveConflicts(List<Variant> variants) {
        // sorted by position assumed
        List<Variant> resolved = new ArrayList<>(variants.size());
        Variant currVariant = null;
        for (Variant var : variants) {
            if (currVariant == null) { // init
                currVariant = var;
            } else if (currVariant.getEnd() < var.getStart()) { // no overlap
                resolved.add(currVariant);
                currVariant = var;
            } else { // partial or full overlap - keep max end and set to 0/0
                currVariant.setEnd(Math.max(var.getEnd(), currVariant.getEnd())); // Set max end position
                changeVariantToNoCall(currVariant);
            }
        }
        resolved.add(currVariant);
        return resolved;
    }

    public static void changeVariantToNoCall(Variant var) {
        String genotype = VariantTableStudyRow.NOCALL;
        var.setType(VariantType.NO_VARIATION);
        StudyEntry se = var.getStudies().get(0);
        int gtpos = se.getFormatPositions().get("GT");
        List<List<String>> sdLst = se.getSamplesData();
        List<List<String>> oLst = new ArrayList<>(sdLst.size());
        for (List<String> sd : sdLst) {
            List<String> o = new ArrayList<>(sd);
            o.set(gtpos, genotype);
            oLst.add(o);
        }
        se.setSamplesData(oLst);
    }

}
