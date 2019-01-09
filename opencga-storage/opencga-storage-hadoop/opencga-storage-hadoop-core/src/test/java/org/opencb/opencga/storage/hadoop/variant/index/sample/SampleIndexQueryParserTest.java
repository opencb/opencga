package org.opencb.opencga.storage.hadoop.variant.index.sample;

import org.junit.Test;
import org.opencb.biodata.models.variant.avro.VariantType;
import org.opencb.commons.datastore.core.Query;

import static org.junit.Assert.assertEquals;
import static org.opencb.opencga.storage.core.variant.adaptors.VariantQueryParam.*;
import static org.opencb.opencga.storage.hadoop.variant.index.annotation.AnnotationIndexConverter.*;
import static org.opencb.opencga.storage.hadoop.variant.index.sample.SampleIndexQueryParser.parseAnnotationMask;

/**
 * Created on 08/01/19.
 *
 * @author Jacobo Coll &lt;jacobo167@gmail.com&gt;
 */
public class SampleIndexQueryParserTest {

    @Test
    public void parseAnnotationMaskTest() {
        assertEquals(EMPTY_ANNOTATION_MASK, parseAnnotationMask(new Query()));
        assertEquals(EMPTY_ANNOTATION_MASK, parseAnnotationMask(new Query(TYPE.key(), VariantType.SNV)));
        assertEquals(EMPTY_ANNOTATION_MASK, parseAnnotationMask(new Query(TYPE.key(), VariantType.SNP)));
        assertEquals(NON_SNV_MASK, parseAnnotationMask(new Query(TYPE.key(), VariantType.INDEL)));
        assertEquals(NON_SNV_MASK, parseAnnotationMask(new Query(TYPE.key(), VariantType.INSERTION)));
        assertEquals(NON_SNV_MASK, parseAnnotationMask(new Query(TYPE.key(), VariantType.DELETION)));

        assertEquals(PROTEIN_CODING_MASK, parseAnnotationMask(new Query(ANNOT_BIOTYPE.key(), "protein_coding")));
        assertEquals(EMPTY_ANNOTATION_MASK, parseAnnotationMask(new Query(ANNOT_BIOTYPE.key(), "other_than_protein_coding")));
        assertEquals(MISSENSE_VARIANT_MASK, parseAnnotationMask(new Query(ANNOT_PROTEIN_SUBSTITUTION.key(), "sift<0.1")));
        assertEquals(EMPTY_ANNOTATION_MASK, parseAnnotationMask(new Query(ANNOT_PROTEIN_SUBSTITUTION.key(), "sift<<0.1")));
    }
}