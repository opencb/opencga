package org.opencb.opencga.storage.hadoop.variant.index.core;

import org.apache.commons.lang3.tuple.Pair;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.opencb.biodata.models.variant.StudyEntry;
import org.opencb.opencga.core.config.storage.FieldConfiguration;
import org.opencb.opencga.core.config.storage.SampleIndexConfiguration;
import org.opencb.opencga.core.testclassification.duration.ShortTests;
import org.opencb.opencga.storage.core.io.bit.BitBuffer;
import org.opencb.opencga.storage.core.io.bit.BitOutputStream;
import org.opencb.opencga.storage.hadoop.variant.index.sample.SampleIndexSchema;

import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static org.junit.Assert.assertEquals;
import static org.opencb.opencga.core.config.storage.FieldConfiguration.Source.FILE;
import static org.opencb.opencga.core.config.storage.FieldConfiguration.Source.SAMPLE;

@Category(ShortTests.class)
public class CategoricalIndexFieldTest {

    @Test
    public void testLength() {
        boolean nullable = false;
        assertEquals(1, CategoricalIndexField.create(new FieldConfiguration(SAMPLE, "K", FieldConfiguration.Type.CATEGORICAL, "1").setNullable(nullable), 0).getBitLength());
        assertEquals(1, CategoricalIndexField.create(new FieldConfiguration(SAMPLE, "K", FieldConfiguration.Type.CATEGORICAL, "1", "2").setNullable(nullable), 0).getBitLength());
        assertEquals(2, CategoricalIndexField.create(new FieldConfiguration(SAMPLE, "K", FieldConfiguration.Type.CATEGORICAL, "1", "2", "3").setNullable(nullable), 0).getBitLength());
        assertEquals(2, CategoricalIndexField.create(new FieldConfiguration(SAMPLE, "K", FieldConfiguration.Type.CATEGORICAL, "1", "2", "3", "4").setNullable(nullable), 0).getBitLength());
        assertEquals(3, CategoricalIndexField.create(new FieldConfiguration(SAMPLE, "K", FieldConfiguration.Type.CATEGORICAL, "1", "2", "3", "4", "5").setNullable(nullable), 0).getBitLength());
        assertEquals(3, CategoricalIndexField.create(new FieldConfiguration(SAMPLE, "K", FieldConfiguration.Type.CATEGORICAL, "1", "2", "3", "4", "5", "6").setNullable(nullable), 0).getBitLength());

        nullable = true;
        assertEquals(1, CategoricalIndexField.create(new FieldConfiguration(SAMPLE, "K", FieldConfiguration.Type.CATEGORICAL, "1").setNullable(nullable), 0).getBitLength());
        assertEquals(2, CategoricalIndexField.create(new FieldConfiguration(SAMPLE, "K", FieldConfiguration.Type.CATEGORICAL, "1", "2").setNullable(nullable), 0).getBitLength());
        assertEquals(2, CategoricalIndexField.create(new FieldConfiguration(SAMPLE, "K", FieldConfiguration.Type.CATEGORICAL, "1", "2", "3").setNullable(nullable), 0).getBitLength());
        assertEquals(3, CategoricalIndexField.create(new FieldConfiguration(SAMPLE, "K", FieldConfiguration.Type.CATEGORICAL, "1", "2", "3", "4").setNullable(nullable), 0).getBitLength());
        assertEquals(3, CategoricalIndexField.create(new FieldConfiguration(SAMPLE, "K", FieldConfiguration.Type.CATEGORICAL, "1", "2", "3", "4", "5").setNullable(nullable), 0).getBitLength());
        assertEquals(3, CategoricalIndexField.create(new FieldConfiguration(SAMPLE, "K", FieldConfiguration.Type.CATEGORICAL, "1", "2", "3", "4", "5", "6").setNullable(nullable), 0).getBitLength());
    }

    @Test
    public void testEncodeDecodeQual() {
        SampleIndexSchema indexSchema = SampleIndexSchema.defaultSampleIndexSchema();
        IndexField<String> qualfield = indexSchema.getFileIndex().getCustomField(FILE, StudyEntry.QUAL);

        List<Pair<String, String>> pairs = Arrays.asList(
                Pair.of("45", "30.0"),
                Pair.of("25", "20.0"),
                Pair.of("30", "30.0"),
                Pair.of("10", "10.0"),
                Pair.of("0", Double.toString(Double.MIN_VALUE))
        );
        for (Pair<String, String> pair : pairs) {
            String qual = pair.getKey();
            String expectedQual = pair.getValue();
            int encode = qualfield.encode(qual);
            String actualQual = qualfield.decode(encode);
            assertEquals(expectedQual, actualQual);
        }
    }

    @Test
    public void testEncodeDecodeFilter() {
        SampleIndexConfiguration indexConfiguration = SampleIndexConfiguration.defaultConfiguration();
        indexConfiguration.getFileIndexConfiguration().getCustomField(FILE, StudyEntry.FILTER)
                .setType(FieldConfiguration.Type.CATEGORICAL_MULTI_VALUE)
                .setValues("PASS", "noPass");
        SampleIndexSchema indexSchema = new SampleIndexSchema(indexConfiguration, 0);
        IndexField<String> field = indexSchema.getFileIndex().getCustomField(FILE, StudyEntry.FILTER);

        List<Pair<String, String>> pairs = Arrays.asList(
                Pair.of("PASS", "PASS"),
                Pair.of("asdfasdf", null),
                Pair.of("noPass", "noPass"),
                Pair.of("PASS;noPass", "PASS;noPass"),
                Pair.of("PASS;noPass;other;another", "PASS;noPass;NA"),
                Pair.of(".", null)
        );
        for (Pair<String, String> pair : pairs) {
            String filter = pair.getKey();
            String expectedFilter = pair.getValue();
            int encode = field.encode(filter);
            String actualFilter = field.decode(encode);
            assertEquals(expectedFilter, actualFilter);
        }
    }

    @Test
    public void testEncodeDecode() {
        SampleIndexSchema indexSchema = SampleIndexSchema.defaultSampleIndexSchema();
        CategoricalMultiValuedIndexField<String> field = indexSchema.getCtIndex().getField();

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