package org.opencb.opencga.server.rest.analysis;

import org.junit.Test;
import org.opencb.commons.datastore.core.QueryParam;
import org.opencb.opencga.core.api.variant.VariantQueryParams;
import org.opencb.opencga.storage.core.variant.adaptors.VariantQueryParam;

import java.lang.reflect.Field;
import java.util.Arrays;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import static org.junit.Assert.fail;
import static org.opencb.opencga.analysis.variant.manager.VariantCatalogQueryUtils.VARIANT_CATALOG_QUERY_PARAMS;

public class VariantAnalysisWSServiceTest {

    @Test
    public void testVariantQueryParams() throws NoSuchFieldException {
        Set<String> other = VARIANT_CATALOG_QUERY_PARAMS.stream().map(QueryParam::key).collect(Collectors.toSet());
        List<String> excluded = Arrays.asList("chromosome", "maf", "mgf", "sort", "groupBy");

        for (Field field : VariantQueryParams.class.getFields()) {
            if (excluded.contains(field.getName())) {
                continue;
            }
            VariantQueryParam queryParam = VariantQueryParam.valueOf(field.getName());
//            VariantCatalogQueryUtils.FAMILY
            if (queryParam == null) {
                if (!other.contains(field.getName())) {
                    fail("Extra field " + field.getName());
                }
            }
        }

        for (VariantQueryParam value : VariantQueryParam.values()) {
            VariantQueryParams.class.getField(value.key());
        }

        for (QueryParam value : VARIANT_CATALOG_QUERY_PARAMS) {
            VariantQueryParams.class.getField(value.key());
        }
    }
}