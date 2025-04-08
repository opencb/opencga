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

/**
 *
 */
package org.opencb.opencga.storage.hadoop.variant.archive.mr;

import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.mapreduce.Reducer;
import org.opencb.biodata.models.variant.Variant;
import org.opencb.biodata.models.variant.protobuf.VcfSliceProtos.VcfSlice;
import org.opencb.biodata.tools.variant.converters.proto.VariantToVcfSliceConverter;
import org.opencb.biodata.tools.variant.converters.proto.VcfSliceToVariantListConverter;
import org.opencb.opencga.storage.hadoop.variant.archive.ArchiveRowKeyFactory;
import org.opencb.opencga.storage.hadoop.variant.archive.ArchiveTableHelper;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * @author Matthias Haimel mh719+git@cam.ac.uk
 */
public class VcfSliceCombiner extends Reducer<ImmutableBytesWritable, VcfSliceWritable, ImmutableBytesWritable, VcfSliceWritable> {

    private VcfSliceToVariantListConverter converterFromSlice;
    private VariantToVcfSliceConverter converterToSlice;
    private ArchiveRowKeyFactory keyFactory;

    @Override
    protected void setup(Context context) throws IOException,
            InterruptedException {
        ArchiveTableHelper helper = new ArchiveTableHelper(context.getConfiguration());
        converterFromSlice = new VcfSliceToVariantListConverter(helper.getFileMetadata()
                .toVariantStudyMetadata(String.valueOf(helper.getStudyId())));
        converterToSlice = new VariantToVcfSliceConverter();
        keyFactory = helper.getKeyFactory();
    }

    @Override
    protected void reduce(ImmutableBytesWritable key, Iterable<VcfSliceWritable> input, Context cxt) throws IOException,
            InterruptedException {
        cxt.getCounter("OPENCGA.HBASE", "VCF_COMBINE_COUNT").increment(1);

        List<Variant> variants = new ArrayList<>();
        for (VcfSliceWritable vcfSlice : input) {
            variants.addAll(converterFromSlice.convert(vcfSlice.get()));
        }

        String keyStr = Bytes.toString(key.get());
        int position = (int) keyFactory.extractPositionFromBlockId(keyStr);
        VcfSlice slice = converterToSlice.convert(variants, position);
        cxt.getCounter("OPENCGA.HBASE", "VCF_SLICE_SIZE").increment(slice.getRecordsCount());

//        cxt.write(key, new ImmutableBytesWritable(slice.toByteArray()));
        cxt.write(key, new VcfSliceWritable(slice));
    }

}
