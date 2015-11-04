/**
 * 
 */
package org.opencb.opencga.storage.hadoop.mr;

import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;
import org.opencb.biodata.models.variant.protobuf.VcfSliceProtos.VcfMeta;
import org.opencb.biodata.models.variant.protobuf.VcfSliceProtos.VcfRecord;
import org.opencb.biodata.models.variant.protobuf.VcfSliceProtos.VcfSlice;
import org.opencb.biodata.models.variant.protobuf.VcfSliceProtos.VcfSlice.Builder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.protobuf.InvalidProtocolBufferException;

/**
 * @author Matthias Haimel mh719+git@cam.ac.uk
 *
 */
public class GenomeVariantHelper extends GenomeHelper {

    private final static Logger log = LoggerFactory.getLogger(GenomeVariantHelper.class);
    private final AtomicReference<VcfMeta> meta = new AtomicReference<>();
    private final byte[] column;


    private final VcfRecordComparator vcfComparator = new VcfRecordComparator();


    public static Logger getLog() {
        return log;
    }

    /**
     * @throws IOException
     */
    public GenomeVariantHelper(Configuration conf) throws IOException {
        super(conf);
        this.meta.set(loadMetaData(conf, (InputStream in) -> VcfMeta.parseFrom(in)));
        column = Bytes.toBytes(getMeta().getFileId());
    }

    public GenomeVariantHelper(GenomeHelper helper, byte[] meta) throws IOException {
        super(helper);
        this.meta.set(VcfMeta.parseFrom(meta));
        column = Bytes.toBytes(getMeta().getFileId());
    }

    public GenomeVariantHelper(GenomeHelper helper, VcfMeta meta) throws IOException {
        super(helper);
        this.meta.set(meta);
        column = Bytes.toBytes(getMeta().getFileId());
    }


    public static void setMetaProtoFile (Configuration conf, URI filePath) {
        conf.set(CONFIG_VCF_META_PROTO_FILE, filePath.toString());
    }
    

    public VcfMeta getMeta () {
        return meta.get();
    }

    public byte[] getColumn () {
        return column;
    }

    public VcfSlice join (byte[] key, Iterable<Put> input) throws InvalidProtocolBufferException {
        Builder sliceBuilder = VcfSlice.newBuilder();
        boolean isFirst = true;
        List<VcfRecord> vcfRecordLst = new ArrayList<VcfRecord>();
        for (Put p : input) {
            VcfSlice slice = extractSlice(p);

            byte[] skey = generateBlockIdAsBytes(slice.getChromosome(), slice.getPosition());
            // Consistency check
            if (!Bytes.equals(skey, key)) // Address doesn't match up -> should
                                          // never happen
                throw new IllegalStateException(String.format("Row keys don't match up!!! %s != %s", Bytes.toString(key),
                        Bytes.toString(skey)));

            if (isFirst) { // init new slice
                sliceBuilder.setChromosome(slice.getChromosome()).setPosition(slice.getPosition());
                isFirst = false;
            }
            vcfRecordLst.addAll(slice.getRecordsList());
        }

        // Sort records
        Collections.sort(vcfRecordLst, getVcfComparator());

        // Add all
        sliceBuilder.addAllRecords(vcfRecordLst);
        return sliceBuilder.build();
    }

    private VcfSlice extractSlice (Put put) throws InvalidProtocolBufferException {
        List<Cell> cList = put.get(getColumnFamily(), getColumn());
        if (cList.isEmpty()) {
            throw new IllegalStateException(String.format("No data available for row % in column %s in familiy %s!!!",
                    Bytes.toString(put.getRow()), Bytes.toString(getColumn()), Bytes.toString(getColumnFamily())));
        }
        if (cList.size() > 1) {
            throw new IllegalStateException(String.format("One entry instead of %s expected for row %s column %s in familiy %s!!!",
                    cList.size(), Bytes.toString(put.getRow()), Bytes.toString(getColumn()), Bytes.toString(getColumnFamily())));
        }
        Cell cell = cList.get(0);
        
        byte[] arr = Arrays.copyOfRange(cell.getValueArray(), cell.getValueOffset(), cell.getValueOffset() + cell.getValueLength());
        VcfSlice slice = VcfSlice.parseFrom(arr);
        return slice;
    }

    private VcfRecordComparator getVcfComparator () {
        return vcfComparator;
    }

    public byte[] wrap (VcfRecord record) {
        return record.toByteArray();
    }

    public Put wrap (VcfSlice slice) {
        byte[] rowId = generateBlockIdAsBytes(slice.getChromosome(), (long) slice.getPosition());
        return wrapAsPut(getColumn(), rowId, slice);
    }

    public Put getMetaAsPut(){
        return wrapMetaAsPut(getColumn(), getMeta());
    }

}
