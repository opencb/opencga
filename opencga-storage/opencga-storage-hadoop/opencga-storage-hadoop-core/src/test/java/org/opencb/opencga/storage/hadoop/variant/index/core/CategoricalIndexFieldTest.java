package org.opencb.opencga.storage.hadoop.variant.index.core;

import org.junit.Test;
import org.opencb.opencga.core.config.storage.IndexFieldConfiguration;
import org.opencb.opencga.core.config.storage.SampleIndexConfiguration;
import org.opencb.opencga.storage.core.io.bit.BitBuffer;
import org.opencb.opencga.storage.core.io.bit.BitOutputStream;
import org.opencb.opencga.storage.hadoop.variant.index.sample.SampleIndexSchema;

import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static org.junit.Assert.assertEquals;
import static org.opencb.opencga.core.config.storage.IndexFieldConfiguration.Source.SAMPLE;

public class CategoricalIndexFieldTest {

    @Test
    public void testLength() {
        boolean nullable = false;
        assertEquals(1, CategoricalIndexField.create(new IndexFieldConfiguration(SAMPLE, "K", IndexFieldConfiguration.Type.CATEGORICAL, "1").setNullable(nullable), 0).getBitLength());
        assertEquals(1, CategoricalIndexField.create(new IndexFieldConfiguration(SAMPLE, "K", IndexFieldConfiguration.Type.CATEGORICAL, "1", "2").setNullable(nullable), 0).getBitLength());
        assertEquals(2, CategoricalIndexField.create(new IndexFieldConfiguration(SAMPLE, "K", IndexFieldConfiguration.Type.CATEGORICAL, "1", "2", "3").setNullable(nullable), 0).getBitLength());
        assertEquals(2, CategoricalIndexField.create(new IndexFieldConfiguration(SAMPLE, "K", IndexFieldConfiguration.Type.CATEGORICAL, "1", "2", "3", "4").setNullable(nullable), 0).getBitLength());
        assertEquals(3, CategoricalIndexField.create(new IndexFieldConfiguration(SAMPLE, "K", IndexFieldConfiguration.Type.CATEGORICAL, "1", "2", "3", "4", "5").setNullable(nullable), 0).getBitLength());
        assertEquals(3, CategoricalIndexField.create(new IndexFieldConfiguration(SAMPLE, "K", IndexFieldConfiguration.Type.CATEGORICAL, "1", "2", "3", "4", "5", "6").setNullable(nullable), 0).getBitLength());

        nullable = true;
        assertEquals(1, CategoricalIndexField.create(new IndexFieldConfiguration(SAMPLE, "K", IndexFieldConfiguration.Type.CATEGORICAL, "1").setNullable(nullable), 0).getBitLength());
        assertEquals(2, CategoricalIndexField.create(new IndexFieldConfiguration(SAMPLE, "K", IndexFieldConfiguration.Type.CATEGORICAL, "1", "2").setNullable(nullable), 0).getBitLength());
        assertEquals(2, CategoricalIndexField.create(new IndexFieldConfiguration(SAMPLE, "K", IndexFieldConfiguration.Type.CATEGORICAL, "1", "2", "3").setNullable(nullable), 0).getBitLength());
        assertEquals(3, CategoricalIndexField.create(new IndexFieldConfiguration(SAMPLE, "K", IndexFieldConfiguration.Type.CATEGORICAL, "1", "2", "3", "4").setNullable(nullable), 0).getBitLength());
        assertEquals(3, CategoricalIndexField.create(new IndexFieldConfiguration(SAMPLE, "K", IndexFieldConfiguration.Type.CATEGORICAL, "1", "2", "3", "4", "5").setNullable(nullable), 0).getBitLength());
        assertEquals(3, CategoricalIndexField.create(new IndexFieldConfiguration(SAMPLE, "K", IndexFieldConfiguration.Type.CATEGORICAL, "1", "2", "3", "4", "5", "6").setNullable(nullable), 0).getBitLength());
    }

    @Test
    public void testEncodeDecode() {
        SampleIndexSchema indexSchema = SampleIndexSchema.defaultSampleIndexSchema();
        CategoricalMultiValuedIndexField<String> field = (CategoricalMultiValuedIndexField<String>) indexSchema.getCtIndex().getField();

        List<String> expected = Arrays.asList("synonymous_variant", "missense_variant");
        int encode = field.encode(expected);
        Set<String> actual = new HashSet<>(field.decode(encode));
        assertEquals(new HashSet<>(expected), actual);

        int values = Byte.SIZE * 5;
        BitOutputStream bos = new BitOutputStream(field.getBitLength() * values / Byte.SIZE);

        for (int i = 0; i < values; i++) {
            bos.write(indexSchema.getCtIndex().getField().write(expected));
        }

        BitBuffer bb = bos.toBitBuffer();

        for (int i = 0; i < values; i++) {
            actual = new HashSet<>(field.decode(indexSchema.getCtIndex().readFieldValue(bb, i)));
            assertEquals(new HashSet<>(expected), actual);
        }

    }
}