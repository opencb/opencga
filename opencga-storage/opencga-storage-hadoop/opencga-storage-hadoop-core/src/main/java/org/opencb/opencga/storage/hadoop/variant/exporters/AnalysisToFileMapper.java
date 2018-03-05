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

package org.opencb.opencga.storage.hadoop.variant.exporters;

import org.apache.avro.mapred.AvroKey;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.io.NullWritable;
import org.opencb.biodata.models.variant.Variant;
import org.opencb.opencga.storage.hadoop.variant.mr.AbstractHBaseVariantMapper;
import org.opencb.opencga.storage.hadoop.variant.mr.AnalysisTableMapReduceHelper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

import static org.opencb.opencga.storage.hadoop.variant.exporters.VariantTableExportDriver.CONFIG_VARIANT_TABLE_EXPORT_TYPE;

/**
 * Created by mh719 on 06/12/2016.
 * @author Matthias Haimel
 */
public class AnalysisToFileMapper extends AbstractHBaseVariantMapper<Object, Object> {

    private Logger logger = LoggerFactory.getLogger(AnalysisToFileMapper.class);
    private VariantTableExportDriver.ExportType type;

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        super.setup(context);

//        List<String> returnedSamples = Collections.emptyList(); // No GT data by default
//        boolean withGenotype = context.getConfiguration().getBoolean(VariantTableExportDriver
//                .CONFIG_VARIANT_TABLE_EXPORT_AVRO_GENOTYPE, false);
//        withGenotype = context.getConfiguration().getBoolean(VariantTableExportDriver
//                .CONFIG_VARIANT_TABLE_EXPORT_GENOTYPE, withGenotype);
//        if (withGenotype) {
//            returnedSamples = new ArrayList<>(this.getIndexedSamples().keySet());
//        }
//        logger.info("Export Genotype [{}] of {} samples ... ", withGenotype, returnedSamples.size());
        getHbaseToVariantConverter().setStudyNameAsStudyId(true);

        String typeString = context.getConfiguration()
                .get(CONFIG_VARIANT_TABLE_EXPORT_TYPE, VariantTableExportDriver.ExportType.AVRO.name());
        this.type = VariantTableExportDriver.ExportType.valueOf(typeString);
    }

    @Override
    protected void map(ImmutableBytesWritable key, Result value, Context context) throws IOException,
            InterruptedException {
        Variant variant = this.getHbaseToVariantConverter().convert(value);
        switch (this.type) {
            case AVRO:
                context.write(new AvroKey<>(variant.getImpl()), NullWritable.get());
                break;
            case VCF:
                context.write(variant, NullWritable.get());
                break;
            default:
                throw new IllegalStateException("Type not supported: " + this.type);
        }
        context.getCounter(AnalysisTableMapReduceHelper.COUNTER_GROUP_NAME, this.type.name()).increment(1);
    }
}
