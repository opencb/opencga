/*
 * Copyright 2015-2016 OpenCB
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.opencb.opencga.storage.hadoop.variant.index.phoenix;

import org.junit.Test;
import org.opencb.commons.datastore.core.Query;

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