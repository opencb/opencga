package org.opencb.opencga.storage.hadoop.variant.converters.annotation;

import org.apache.hadoop.hbase.client.Put;
import org.opencb.biodata.models.variant.avro.VariantAnnotation;
import org.opencb.biodata.tools.commons.Converter;
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

    public VariantAnnotationToHBaseConverter(ProgressLogger progressLogger, int annotationId) {
        super(GenomeHelper.COLUMN_FAMILY_BYTES);
        this.progressLogger = progressLogger;
        this.converter = new VariantAnnotationToPhoenixConverter(GenomeHelper.COLUMN_FAMILY_BYTES, annotationId);
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
