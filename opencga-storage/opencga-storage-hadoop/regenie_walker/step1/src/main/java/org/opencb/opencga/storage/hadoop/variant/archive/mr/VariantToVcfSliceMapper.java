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

import org.apache.avro.io.DatumWriter;
import org.apache.avro.mapred.AvroKey;
import org.apache.avro.specific.SpecificDatumWriter;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Mapper;
import org.opencb.biodata.models.core.Region;
import org.opencb.biodata.models.variant.Variant;
import org.opencb.biodata.models.variant.avro.VariantAvro;
import org.opencb.biodata.models.variant.protobuf.VcfSliceProtos.VcfSlice;
import org.opencb.biodata.tools.variant.converters.proto.VariantToProtoVcfRecord;
import org.opencb.biodata.tools.variant.converters.proto.VariantToVcfSliceConverter;
import org.opencb.opencga.storage.hadoop.variant.archive.ArchiveTableHelper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Collections;
import java.util.concurrent.atomic.AtomicReference;

/**
 * @author Matthias Haimel mh719+git@cam.ac.uk.
 */
public class VariantToVcfSliceMapper extends Mapper<AvroKey<VariantAvro>, NullWritable, ImmutableBytesWritable, VcfSliceWritable> {

    private final Logger logger = LoggerFactory.getLogger(VariantToVcfSliceMapper.class);

//    private final VariantToProtoVcfRecord converter = new VariantToProtoVcfRecord();
    private final VariantToVcfSliceConverter converter = new VariantToVcfSliceConverter();
    private final AtomicReference<ArchiveTableHelper> helper = new AtomicReference<>();

    private DatumWriter<VariantAvro> variantDatumWriter = new SpecificDatumWriter<>(VariantAvro.class);

    public VariantToVcfSliceMapper() {
    }

    @Override
    protected void setup(Context context)
            throws IOException, InterruptedException {
        this.helper.set(new ArchiveTableHelper(context.getConfiguration()));
//        converter.updateVcfMeta(getHelper().getMeta());
        super.setup(context);
    }

    public ArchiveTableHelper getHelper() {
        return helper.get();
    }

    public byte[] getColumn() {
        return getHelper().getNonRefColumnName();
    }

    /**
     * Convert a Variant to a list of {@link VcfSlice} converting the position into the slice position <br>
     * e.g. using chunk size 100 with position 1234 would result in slice position 1200.
     */
    @Override
    protected void map(AvroKey<VariantAvro> key, NullWritable value, Context context) throws IOException,
            InterruptedException {
        VariantAvro varAvro = key.datum();
        Variant variant = new Variant(varAvro);
        context.getCounter("OPENCGA.HBASE", "VCF_MAP_COUNT").increment(1);

        long[] slicePositionArr = getCoveredSlicePositions(variant);
        for (long slicePos : slicePositionArr) {
            VcfSlice slice = converter.convert(Collections.singletonList(variant), (int) slicePos);
            ImmutableBytesWritable rowKey = new ImmutableBytesWritable(getHelper().getKeyFactory().
                    generateBlockIdAsBytes(getHelper().getFileId(), variant.getChromosome(), (int) slicePos));
            context.write(rowKey, new VcfSliceWritable(slice));
        }

    }


    private long[] getCoveredSlicePositions(Variant variant) {
        int chSize = getHelper().getChunkSize();
        long startChunk = VariantToProtoVcfRecord.getSlicePosition(variant.getStart(), chSize);
        long endChunk = VariantToProtoVcfRecord.getSlicePosition(variant.getEnd(), chSize);
        if (endChunk == startChunk) {
            return new long[]{startChunk};
        }
        int len = (int) ((endChunk - startChunk) / chSize) + 1;
        long[] ret = new long[len];
        for (int i = 0; i < len; ++i) {
            ret[i] = startChunk + (((long) i) * chSize);
        }
        return ret;
    }

    private String extractChromosome(Variant var) {
        return Region.normalizeChromosome(var.getChromosome());
    }

    public Logger getLogger() {
        return logger;
    }

}
