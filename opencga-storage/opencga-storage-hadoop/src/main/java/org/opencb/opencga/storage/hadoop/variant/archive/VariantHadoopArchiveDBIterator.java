package org.opencb.opencga.storage.hadoop.variant.archive;

import com.google.protobuf.InvalidProtocolBufferException;
import org.apache.hadoop.hbase.client.Result;
import org.opencb.biodata.models.variant.Variant;
import org.opencb.biodata.models.variant.protobuf.VcfSliceProtos;
import org.opencb.biodata.tools.variant.converter.VcfRecordToVariantConverter;
import org.opencb.opencga.storage.core.variant.adaptors.VariantDBIterator;

import java.util.Collections;
import java.util.Iterator;

/**
 * Created on 04/11/15
 *
 * @author Jacobo Coll &lt;jacobo167@gmail.com&gt;
 */
public class VariantHadoopArchiveDBIterator extends VariantDBIterator {

    private final VcfRecordToVariantConverter converter;
    private Iterator<VcfSliceProtos.VcfRecord> vcfRecordIterator = Collections.emptyIterator();
    private VcfSliceProtos.VcfSlice vcfSlice;
    private final Iterator<Result> iterator;
    private final byte[] columnFamily;
    private final byte[] fileIdBytes;

    public VariantHadoopArchiveDBIterator(Iterator<Result> iterator, byte[] columnFamily, byte[] fileIdBytes, VcfSliceProtos.VcfMeta meta) {
        this.iterator = iterator;
        this.columnFamily = columnFamily;
        this.fileIdBytes = fileIdBytes;
        converter = new VcfRecordToVariantConverter(meta);
    }

    @Override
    public boolean hasNext() {
        return vcfRecordIterator.hasNext() || iterator.hasNext();
    }

    @Override
    public Variant next() {
        if (!vcfRecordIterator.hasNext()) {
            Result result = iterator.next();
            byte[] rid = result.getRow();
            try {
                byte[] value = result.getValue(columnFamily, fileIdBytes);
                vcfSlice = VcfSliceProtos.VcfSlice.parseFrom(value);
                vcfRecordIterator = vcfSlice.getRecordsList().iterator();

            } catch (InvalidProtocolBufferException e) {
                throw new RuntimeException(e);
            }
        }
        VcfSliceProtos.VcfRecord vcfRecord = vcfRecordIterator.next();

        Variant variant = null;
        try {
            variant = converter.convert(vcfRecord, vcfSlice.getChromosome(), vcfSlice.getPosition());
        } catch (IllegalArgumentException e ){
            e.printStackTrace(System.err);
            System.err.println("vcfSlice.getPosition() = " + vcfSlice.getPosition());
            System.err.println("vcfRecord.getRelativeStart() = " + vcfRecord.getRelativeStart());
            System.err.println("vcfRecord.getRelativeEnd() = " + vcfRecord.getRelativeEnd());
            variant = new Variant(vcfSlice.getChromosome(), vcfRecord.getRelativeStart() + vcfSlice.getPosition(), vcfRecord.getReference(), vcfRecord.getAlternate(0));
            System.err.println("variant " + variant.toString());
        }
        return variant;
    }
}
