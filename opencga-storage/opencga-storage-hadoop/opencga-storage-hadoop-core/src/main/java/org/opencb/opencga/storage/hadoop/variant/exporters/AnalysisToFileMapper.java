package org.opencb.opencga.storage.hadoop.variant.exporters;

import org.apache.avro.mapred.AvroKey;
import org.apache.hadoop.io.NullWritable;
import org.opencb.biodata.models.variant.Variant;
import org.opencb.opencga.storage.hadoop.variant.AbstractPhoenisMapReduce;
import org.opencb.opencga.storage.hadoop.variant.annotation.PhoenixVariantAnnotationWritable;
import org.opencb.opencga.storage.hadoop.variant.index.AbstractVariantTableMapReduce;

import java.io.IOException;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import static org.opencb.opencga.storage.hadoop.variant.exporters.VariantTableExportDriver
        .CONFIG_VARIANT_TABLE_EXPORT_TYPE;

/**
 * Created by mh719 on 06/12/2016.
 * @author Matthias Haimel
 */
public class AnalysisToFileMapper extends AbstractPhoenisMapReduce<PhoenixVariantAnnotationWritable, Object, Object> {

    private VariantTableExportDriver.ExportType type;

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        super.setup(context);

        List<String> returnedSamples = Collections.singletonList("."); // No GT data by default
        boolean withGenotype = context.getConfiguration().getBoolean(VariantTableExportDriver
                .CONFIG_VARIANT_TABLE_EXPORT_AVRO_GENOTYPE, false);
        withGenotype = context.getConfiguration().getBoolean(VariantTableExportDriver
                .CONFIG_VARIANT_TABLE_EXPORT_GENOTYPE, withGenotype);
        if (withGenotype) {
            returnedSamples = new ArrayList<>(this.getIndexedSamples().keySet());
        }
        getLog().info("Export Genotype [{}] of {} samples ... ", withGenotype, returnedSamples.size());
        this.getHbaseToVariantConverter().setReturnedSamples(returnedSamples);

        String typeString = context.getConfiguration()
                .get(CONFIG_VARIANT_TABLE_EXPORT_TYPE, VariantTableExportDriver.ExportType.AVRO.name());
        this.type = VariantTableExportDriver.ExportType.valueOf(typeString);
    }

    @Override
    protected void map(NullWritable key, PhoenixVariantAnnotationWritable value, Context context) throws IOException,
            InterruptedException {
        try {
            context.getCounter(AbstractVariantTableMapReduce.COUNTER_GROUP_NAME, this.type.name()).increment(1);
            Variant variant = this.getHbaseToVariantConverter().convert(value.getResultSet());
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
        } catch (SQLException e) {
            throw new IOException(e);
        }
    }
}
