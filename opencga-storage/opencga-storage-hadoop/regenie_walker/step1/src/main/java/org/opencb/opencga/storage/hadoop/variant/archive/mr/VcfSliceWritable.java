/*
 * Copyright 2015-2017 OpenCB
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

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
