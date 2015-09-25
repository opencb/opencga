/*
 * Copyright 2015 OpenCB
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

package org.opencb.opencga.storage.hadoop.mr;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.avro.io.DatumWriter;
import org.apache.avro.mapred.AvroKey;
import org.apache.avro.specific.SpecificDatumWriter;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Mapper;
import org.opencb.biodata.models.variant.avro.Variant;
import org.opencb.biodata.models.variant.converter.VariantAvroToVcfRecord;
import org.opencb.biodata.models.variant.protobuf.VcfSliceProtos.VcfRecord;
import org.opencb.biodata.models.variant.protobuf.VcfSliceProtos.VcfSlice;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.protobuf.InvalidProtocolBufferException;

/**
 * @author Matthias Haimel mh719+git@cam.ac.uk
 *
 */
public class GenomeVariantConverter extends Mapper<AvroKey<Variant>, NullWritable, ImmutableBytesWritable, Put> {

    private final static Logger log = LoggerFactory.getLogger(GenomeVariantConverter.class);

    private final VariantAvroToVcfRecord converter = new VariantAvroToVcfRecord();
    private final AtomicReference<GenomeVariantHelper> helper = new AtomicReference<GenomeVariantHelper>();

    DatumWriter<Variant> variantDatumWriter = new SpecificDatumWriter<Variant>(Variant.class);

    public GenomeVariantConverter () {
    }

    @Override
    protected void setup (Mapper<AvroKey<Variant>, NullWritable, ImmutableBytesWritable, Put>.Context context) throws IOException,
            InterruptedException {
        this.helper.set(new GenomeVariantHelper(context.getConfiguration()));
        converter.updateVcfMeta(getHelper().getMeta());
        super.setup(context);
    }

    public GenomeVariantHelper getHelper () {
        return helper.get();
    }

    public int getChunkSize () {
        return getHelper().getChunkSize();
    }

    public byte[] getColumn () {
        return getHelper().getColumn();
    }

    @Override
    protected void map (AvroKey<Variant> key, NullWritable value,
            Mapper<AvroKey<Variant>, NullWritable, ImmutableBytesWritable, Put>.Context context) throws IOException, InterruptedException {
        Variant variant = key.datum();
        VcfSlice slice = convert(variant);
        Put put = getHelper().wrap(slice);
        ImmutableBytesWritable rowKey = new ImmutableBytesWritable(put.getRow());
        context.write(rowKey, put);
    }

    public VariantAvroToVcfRecord getConverter () {
        return converter;
    }

    /**
     * Convert a Variant to a {@link VcfSlice} converting the position into the slice position <br>
     * e.g. using chunk size 100 with position 1234 would result in slice position 1200
     * @param variant {@link Variant}
     * @return {@link VcfSlice}
     */
    public VcfSlice convert (Variant variant) {
        VcfRecord rec = getConverter().convert(variant, this.getChunkSize());
        long slicePosition = getSlicePosition(variant);
        slicePosition = slicePosition * this.getChunkSize(); // add missing 0s at the end e.g. 123 -> 12300 (for chunk size 100)
        VcfSlice slice = VcfSlice.newBuilder()
                .addRecords(rec)
                .setChromosome(extractChromosome(variant))
                .setPosition((int) slicePosition)
                .build();
        return slice;
    }

    private long getSlicePosition (Variant variant) {
        return getConverter().getSlicePosition(variant.getStart(), this.getChunkSize());
    }

    private String extractChromosome (Variant var) {
        return getHelper().standardChromosome(var.getChromosome().toString());
    }

    public List<String> getSamples () {
        return this.converter.getSamples();
    }

    public static Logger getLog () {
        return log;
    }

}
