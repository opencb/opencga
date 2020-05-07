/*
 * Copyright 2015-2020 OpenCB
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

package org.opencb.opencga.server.rest.analysis;

import org.junit.Test;
import org.opencb.commons.datastore.core.QueryParam;
import org.opencb.opencga.core.models.variant.VariantQueryParams;
import org.opencb.opencga.storage.core.variant.adaptors.VariantQueryParam;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.opencb.opencga.analysis.variant.manager.VariantCatalogQueryUtils.VARIANT_CATALOG_QUERY_PARAMS;

public class VariantWebServiceTest {

    @Test
    public void testVariantQueryParams() {
        Set<String> other = VARIANT_CATALOG_QUERY_PARAMS.stream().map(QueryParam::key).collect(Collectors.toSet());
        List<String> excluded = Arrays.asList("chromosome", "sort");

        Map<String, Class<?>> fields = new VariantQueryParams().fields();
        for (String field : fields.keySet()) {
            if (excluded.contains(field)) {
                continue;
            }
            VariantQueryParam queryParam = VariantQueryParam.valueOf(field);
            if (queryParam == null) {
                if (!other.contains(field)) {
                    fail("Extra field " + field);
                }
            }
        }

        for (VariantQueryParam value : VariantQueryParam.values()) {
            assertTrue(fields.containsKey(value.key()));
        }

        for (QueryParam value : VARIANT_CATALOG_QUERY_PARAMS) {
            assertTrue(fields.containsKey(value.key()));
        }
    }
}