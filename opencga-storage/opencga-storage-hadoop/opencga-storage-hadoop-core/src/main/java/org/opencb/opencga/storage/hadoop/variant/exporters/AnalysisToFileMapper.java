package org.opencb.opencga.storage.hadoop.variant.exporters;

import org.apache.avro.mapred.AvroKey;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.NullWritable;
import org.opencb.biodata.models.variant.Variant;
import org.opencb.opencga.storage.hadoop.variant.AbstractHBaseMapReduce;
import org.opencb.opencga.storage.hadoop.variant.GenomeHelper;
import org.opencb.opencga.storage.hadoop.variant.index.AbstractVariantTableMapReduce;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import static org.opencb.opencga.storage.hadoop.variant.exporters.VariantTableExportDriver
        .CONFIG_VARIANT_TABLE_EXPORT_TYPE;

/**
 * Created by mh719 on 06/12/2016.
 * @author Matthias Haimel
 */
public class AnalysisToFileMapper extends AbstractHBaseMapReduce<Object, Object> {

    private byte[] studiesRow;
    private VariantTableExportDriver.ExportType type;

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        super.setup(context);
        studiesRow = getHelper().generateVariantRowKey(GenomeHelper.DEFAULT_METADATA_ROW_KEY, 0);

        List<String> returnedSamples = Collections.emptyList(); // No GT data by default
        boolean withGenotype = context.getConfiguration().getBoolean(VariantTableExportDriver
                .CONFIG_VARIANT_TABLE_EXPORT_AVRO_GENOTYPE, false);
        withGenotype = context.getConfiguration().getBoolean(VariantTableExportDriver
                .CONFIG_VARIANT_TABLE_EXPORT_GENOTYPE, withGenotype);
        if (withGenotype) {
            returnedSamples = new ArrayList<>(this.getIndexedSamples().keySet());
        }
        getLog().info("Export Genotype [{}] of {} samples ... ", withGenotype, returnedSamples.size());
        getHbaseToVariantConverter().setReturnedSamples(returnedSamples);
        getHbaseToVariantConverter().setStudyNameAsStudyId(true);

        String typeString = context.getConfiguration()
                .get(CONFIG_VARIANT_TABLE_EXPORT_TYPE, VariantTableExportDriver.ExportType.AVRO.name());
        this.type = VariantTableExportDriver.ExportType.valueOf(typeString);
    }

    @Override
    protected void map(ImmutableBytesWritable key, Result value, Context context) throws IOException,
            InterruptedException {
        if (!Bytes.startsWith(value.getRow(), this.studiesRow)) { // ignore _METADATA row
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
            context.getCounter(AbstractVariantTableMapReduce.COUNTER_GROUP_NAME, this.type.name()).increment(1);
        }
    }
}
