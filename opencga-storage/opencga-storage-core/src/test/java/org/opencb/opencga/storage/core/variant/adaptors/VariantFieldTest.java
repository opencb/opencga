package org.opencb.opencga.storage.core.variant.adaptors;

import org.apache.avro.Schema;
import org.junit.Test;
import org.opencb.biodata.models.variant.avro.VariantAnnotation;

import java.util.Set;
import java.util.TreeSet;
import java.util.stream.Collectors;

import static org.junit.Assert.*;

/**
 * Created on 15/03/18.
 *
 * @author Jacobo Coll &lt;jacobo167@gmail.com&gt;
 */
public class VariantFieldTest {

    @Test
    public void checkAnnotationSubFields() throws Exception {

        Set<String> expectedFields = VariantAnnotation.getClassSchema().getFields().stream().map(Schema.Field::name)
                .collect(Collectors.toCollection(TreeSet::new));
        Set<String> actual = VariantField.ANNOTATION.getChildren().stream().map(f -> f.fieldName().split("\\.")[1])
                .collect(Collectors.toCollection(TreeSet::new));

        assertEquals(expectedFields, actual);
    }


}