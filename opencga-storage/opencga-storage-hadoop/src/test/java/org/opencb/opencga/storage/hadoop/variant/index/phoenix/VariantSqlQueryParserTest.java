package org.opencb.opencga.storage.hadoop.variant.index.phoenix;

import org.junit.Test;
import org.opencb.datastore.core.Query;

import java.util.Arrays;
import java.util.Collections;

import static org.junit.Assert.*;
import static org.opencb.opencga.storage.core.variant.adaptors.VariantDBAdaptor.VariantQueryParams.ANNOTATION_EXISTS;

/**
 * Created on 17/12/15
 *
 * @author Jacobo Coll &lt;jacobo167@gmail.com&gt;
 */
public class VariantSqlQueryParserTest {

    @Test
    public void testIsValid() {
        assertFalse(VariantSqlQueryParser.isValidParam(new Query(), ANNOTATION_EXISTS));
        assertFalse(VariantSqlQueryParser.isValidParam(new Query(ANNOTATION_EXISTS.key(), null), ANNOTATION_EXISTS));
        assertFalse(VariantSqlQueryParser.isValidParam(new Query(ANNOTATION_EXISTS.key(), ""), ANNOTATION_EXISTS));
        assertFalse(VariantSqlQueryParser.isValidParam(new Query(ANNOTATION_EXISTS.key(), Collections.emptyList()), ANNOTATION_EXISTS));
        assertFalse(VariantSqlQueryParser.isValidParam(new Query(ANNOTATION_EXISTS.key(), Arrays.asList()), ANNOTATION_EXISTS));

        assertTrue(VariantSqlQueryParser.isValidParam(new Query(ANNOTATION_EXISTS.key(), Arrays.asList(1,2,3)), ANNOTATION_EXISTS));
        assertTrue(VariantSqlQueryParser.isValidParam(new Query(ANNOTATION_EXISTS.key(), 5), ANNOTATION_EXISTS));
        assertTrue(VariantSqlQueryParser.isValidParam(new Query(ANNOTATION_EXISTS.key(), "sdfas"), ANNOTATION_EXISTS));
    }

}