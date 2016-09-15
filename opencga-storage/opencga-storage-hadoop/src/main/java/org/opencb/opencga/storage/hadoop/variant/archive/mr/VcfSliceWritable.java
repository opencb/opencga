package org.opencb.opencga.storage.hadoop.variant.archive.mr;

import org.apache.hadoop.io.Writable;
import org.opencb.biodata.models.variant.protobuf.VcfSliceProtos;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * Created on 15/02/16.
 *
 * @author Jacobo Coll &lt;jacobo167@gmail.com&gt;
 */
public class VcfSliceWritable implements Writable {

    public VcfSliceWritable() {
    }

    public VcfSliceWritable(VcfSliceProtos.VcfSlice obj) {
        this.obj = obj;
    }

    private VcfSliceProtos.VcfSlice obj;

    @Override
    public void write(DataOutput out) throws IOException {
        byte[] bytes = obj.toByteArray();
        out.writeInt(bytes.length);
        out.write(bytes);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        int length = in.readInt();
        byte[] bytes = new byte[length];
        in.readFully(bytes);
        obj = VcfSliceProtos.VcfSlice.parseFrom(bytes);
    }

    public VcfSliceProtos.VcfSlice get() {
        return obj;
    }

    public void setObj(VcfSliceProtos.VcfSlice obj) {
        this.obj = obj;
    }
}
