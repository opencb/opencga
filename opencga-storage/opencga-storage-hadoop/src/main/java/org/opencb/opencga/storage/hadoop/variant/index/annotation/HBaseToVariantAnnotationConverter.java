package org.opencb.opencga.storage.hadoop.variant.index.annotation;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.hadoop.hbase.client.Result;
import org.opencb.biodata.models.variant.avro.VariantAnnotation;
import org.opencb.biodata.tools.variant.converter.Converter;
import org.opencb.opencga.storage.core.variant.io.json.VariantAnnotationMixin;
import org.opencb.opencga.storage.hadoop.variant.GenomeHelper;
import org.opencb.opencga.storage.hadoop.variant.index.phoenix.VariantPhoenixHelper;

import java.io.IOException;
import java.sql.ResultSet;
import java.sql.SQLException;

/**
 * Created on 03/12/15.
 *
 * @author Jacobo Coll &lt;jacobo167@gmail.com&gt;
 */
public class HBaseToVariantAnnotationConverter implements Converter<Result, VariantAnnotation> {

    private GenomeHelper genomeHelper;
    private ObjectMapper objectMapper;

    public HBaseToVariantAnnotationConverter(GenomeHelper genomeHelper) {
        this.genomeHelper = genomeHelper;
        objectMapper = new ObjectMapper();
        objectMapper.addMixIn(VariantAnnotation.class, VariantAnnotationMixin.class);
    }

    @Override
    public VariantAnnotation convert(Result result) {

        byte[] value = result.getValue(genomeHelper.getColumnFamily(), VariantPhoenixHelper.VariantColumn.FULL_ANNOTATION.bytes());
        if (value != null && value.length > 0) {
            try {
                return objectMapper.readValue(value, VariantAnnotation.class);
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
        return null;

    }

    public VariantAnnotation convert(ResultSet resultSet) {
        int column;
        try {
            column = resultSet.findColumn(VariantPhoenixHelper.VariantColumn.FULL_ANNOTATION.column());
        } catch (SQLException e) {
            //Column not found
            return null;
        }
        try {
            String value = resultSet.getString(column);
            if (value != null && !value.isEmpty()) {
                try {
                    return objectMapper.readValue(value, VariantAnnotation.class);
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
        return null;

    }
}
