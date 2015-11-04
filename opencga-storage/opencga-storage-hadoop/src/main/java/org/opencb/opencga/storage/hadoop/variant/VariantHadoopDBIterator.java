package org.opencb.opencga.storage.hadoop.variant;

import com.google.protobuf.InvalidProtocolBufferException;
import org.apache.hadoop.hbase.client.Result;
import org.opencb.biodata.models.variant.Variant;
import org.opencb.biodata.models.variant.protobuf.VcfSliceProtos;
import org.opencb.opencga.storage.core.variant.adaptors.VariantDBIterator;

import java.util.Collections;
import java.util.Iterator;

/**
 * Created on 04/11/15
 *
 * @author Jacobo Coll &lt;jacobo167@gmail.com&gt;
 */
public class VariantHadoopDBIterator extends VariantDBIterator {

    private Iterator<VcfSliceProtos.VcfRecord> vcfRecordIterator = Collections.emptyIterator();
    private VcfSliceProtos.VcfSlice vcfSlice;
    private final Iterator<Result> iter;
    private final byte[] columnFamily;
    private final byte[] studyIdBytes;

    public VariantHadoopDBIterator(Iterator<Result> iter, byte[] columnFamily, byte[] studyIdBytes) {
        this.iter = iter;
        this.columnFamily = columnFamily;
        this.studyIdBytes = studyIdBytes;
    }

    @Override
    public boolean hasNext() {
        return vcfRecordIterator.hasNext() || iter.hasNext();
    }

    @Override
    public Variant next() {
        if (!vcfRecordIterator.hasNext()) {
            Result result = iter.next();
            byte[] rid = result.getRow();
            try {
                byte[] value = result.getValue(columnFamily, studyIdBytes);
                vcfSlice = VcfSliceProtos.VcfSlice.parseFrom(value);
                vcfRecordIterator = vcfSlice.getRecordsList().iterator();

            } catch (InvalidProtocolBufferException e) {
                throw new RuntimeException(e);
            }
        }
        VcfSliceProtos.VcfRecord vcfRecord = vcfRecordIterator.next();
        return new Variant(vcfSlice.getChromosome(), vcfSlice.getPosition() + vcfRecord.getRelativeStart(),
                vcfRecord.getReference(), vcfRecord.getAlternate());

    }
}
