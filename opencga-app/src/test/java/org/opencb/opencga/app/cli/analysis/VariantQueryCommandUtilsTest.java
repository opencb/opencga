/*
 * Copyright 2015-2017 OpenCB
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

package org.opencb.opencga.app.cli.analysis;

import org.junit.Test;
import org.opencb.commons.datastore.core.Query;
import org.opencb.opencga.app.cli.internal.InternalCliOptionsParser;
import org.opencb.opencga.app.cli.internal.executors.VariantQueryCommandUtils;
import org.opencb.opencga.app.cli.internal.options.VariantCommandOptions;
import org.opencb.opencga.storage.core.variant.adaptors.VariantQueryParam;

import java.util.Collections;
import java.util.Map;

import static org.junit.Assert.assertEquals;

/**
 * Created on 07/06/16
 *
 * @author Jacobo Coll &lt;jacobo167@gmail.com&gt;
 */
public class VariantQueryCommandUtilsTest {

    @Test
    public void parseQueryTest() throws Exception {

        InternalCliOptionsParser cliOptionsParser = new InternalCliOptionsParser();
        VariantCommandOptions.VariantQueryCommandOptions queryVariantsOptions = cliOptionsParser.getVariantCommandOptions().queryVariantCommandOptions;

        queryVariantsOptions.genericVariantQueryOptions.trait = "HP:0002812";
        queryVariantsOptions.genericVariantQueryOptions.includeStudy = "1";
        Map<Long, String> studyIds = Collections.singletonMap(1L, "study");

        Query query = VariantQueryCommandUtils.parseQuery(queryVariantsOptions, studyIds, null);

//        System.out.println("query = " + query.toJson());
        assertEquals("HP:0002812", query.get(VariantQueryParam.ANNOT_TRAIT.key()));
    }

}