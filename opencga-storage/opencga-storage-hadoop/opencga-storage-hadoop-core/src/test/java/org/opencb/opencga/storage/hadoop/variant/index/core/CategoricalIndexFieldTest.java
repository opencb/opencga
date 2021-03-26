package org.opencb.opencga.storage.hadoop.variant.index.core;

import org.junit.Test;
import org.opencb.opencga.core.config.storage.IndexFieldConfiguration;

import static org.junit.Assert.*;
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
}