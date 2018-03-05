package org.opencb.opencga.storage.hadoop.variant.converters.annotation;

import org.apache.hadoop.hbase.client.Put;
import org.opencb.biodata.models.variant.avro.VariantAnnotation;
import org.opencb.biodata.tools.Converter;
import org.opencb.commons.ProgressLogger;
import org.opencb.opencga.storage.hadoop.variant.GenomeHelper;
import org.opencb.opencga.storage.hadoop.variant.converters.AbstractPhoenixConverter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Created on 25/01/18.
 *
 * @author Jacobo Coll &lt;jacobo167@gmail.com&gt;
 */
public class VariantAnnotationToHBaseConverter extends AbstractPhoenixConverter implements Converter<VariantAnnotation, Put> {

    private final ProgressLogger progressLogger;
    private final VariantAnnotationToPhoenixConverter converter;
    protected static Logger logger = LoggerFactory.getLogger(VariantAnnotationToHBaseConverter.class);

    public VariantAnnotationToHBaseConverter(GenomeHelper genomeHelper, ProgressLogger progressLogger) {
        super(genomeHelper.getColumnFamily());
        this.progressLogger = progressLogger;
        this.converter = new VariantAnnotationToPhoenixConverter(genomeHelper.getColumnFamily());
    }

    @Override
    public Put convert(VariantAnnotation variantAnnotation) {
        Put put = converter.buildPut(variantAnnotation);
        if (progressLogger != null) {
            progressLogger.increment(1);
        }
        return put;
    }

}
