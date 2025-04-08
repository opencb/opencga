package org.opencb.opencga.storage.hadoop.variant.search;

import org.apache.hadoop.hbase.client.Result;
import org.opencb.biodata.models.variant.Variant;
import org.opencb.commons.datastore.core.Query;
import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.opencga.storage.core.variant.adaptors.VariantField;
import org.opencb.opencga.storage.core.variant.query.projection.VariantQueryProjection;
import org.opencb.opencga.storage.core.variant.query.projection.VariantQueryProjectionParser;
import org.opencb.opencga.storage.hadoop.variant.adaptors.VariantHadoopDBAdaptor;
import org.opencb.opencga.storage.hadoop.variant.converters.HBaseToVariantConverter;
import org.opencb.opencga.storage.hadoop.variant.converters.HBaseVariantConverterConfiguration;
import org.opencb.opencga.storage.hadoop.variant.pending.PendingVariantsReader;

import java.util.Arrays;
import java.util.List;

public class SecondaryIndexPendingVariantsReader extends PendingVariantsReader {

    private final HBaseToVariantConverter<Result> converter;

    public SecondaryIndexPendingVariantsReader(Query query, VariantHadoopDBAdaptor dbAdaptor) {
        super(query, new SecondaryIndexPendingVariantsDescriptor(), dbAdaptor);
        QueryOptions qo = new QueryOptions(QueryOptions.EXCLUDE, Arrays.asList(VariantField.STUDIES_SAMPLES, VariantField.STUDIES_FILES));
        VariantQueryProjection projection =
                new VariantQueryProjectionParser(dbAdaptor.getMetadataManager()).parseVariantQueryProjection(query, qo);
        converter = HBaseToVariantConverter.fromResult(dbAdaptor.getMetadataManager())
                .configure(HBaseVariantConverterConfiguration.builder()
                        .setMutableSamplesPosition(false)
                        .setStudyNameAsStudyId(true)
                        .setProjection(projection)
                        .setIncludeIndexStatus(true)
                        .build());
    }

    @Override
    protected List<Variant> convert(List<Result> results) {
        return converter.apply(results);
    }
}
