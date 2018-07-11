package org.opencb.opencga.storage.hadoop.variant.adaptors;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.avro.generic.GenericRecord;
import org.junit.AssumptionViolatedException;
import org.junit.Test;
import org.opencb.biodata.models.variant.Variant;
import org.opencb.commons.datastore.core.Query;
import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.opencga.core.results.VariantQueryResult;
import org.opencb.opencga.storage.core.variant.adaptors.iterators.VariantDBIterator;
import org.opencb.opencga.storage.core.variant.adaptors.VariantQueryParam;
import org.opencb.opencga.storage.core.variant.io.json.mixin.GenericRecordAvroJsonMixin;

import static org.junit.Assume.assumeTrue;

/**
 * Created on 16/04/18.
 *
 * @author Jacobo Coll &lt;jacobo167@gmail.com&gt;
 */
public class HadoopVariantDBAdaptorNativeTest extends HadoopVariantDBAdaptorTest {

    @Override
    public VariantQueryResult<Variant> query(Query query, QueryOptions options) {
//        checkCanExecuteNativeQuery(query);
        String message = getMessage(query);
        try {
            assumeTrue(message, VariantHBaseQueryParser.isSupportedQuery(query));
        } catch (AssumptionViolatedException e) {
            System.out.println(message);
            // Either if the query is not supported, check if the query was throwing an exception, to match with the "thrown"
            super.query(query, options);
            throw e;
        }
        options = options == null ? new QueryOptions(VariantHadoopDBAdaptor.NATIVE, true) : options.append(VariantHadoopDBAdaptor.NATIVE, true);
        return super.query(query, options);
    }

    @Override
    public VariantDBIterator iterator(Query query, QueryOptions options) {
//        checkCanExecuteNativeQuery(query);
        String message = getMessage(query);
        try {
            assumeTrue(message, VariantHBaseQueryParser.isSupportedQuery(query));
        } catch (AssumptionViolatedException e) {
            System.out.println(message);
            // Either if the query is not supported, check if the query was throwing an exception, to match with the "thrown"
            super.iterator(query, options);
            throw e;
        }
        options = options == null ? new QueryOptions(VariantHadoopDBAdaptor.NATIVE, true) : options.append(VariantHadoopDBAdaptor.NATIVE, true);
        return super.iterator(query, options);
    }

    public String getMessage(Query query) {
        try {
            return "Query " + (query == null ? null : new ObjectMapper().addMixIn(GenericRecord.class, GenericRecordAvroJsonMixin.class).writeValueAsString(query)) + " is not fully supported";
        } catch (JsonProcessingException e) {
            e.printStackTrace();
            return "Query " + (query == null ? null : query.keySet()) + " is not fully supported";
        }
    }

    private void checkCanExecuteNativeQuery(Query query) {
        String message = "Query " + (query == null ? null : query.toJson()) + " is not fully supported";
        try {
            assumeTrue(message, VariantHBaseQueryParser.isSupportedQuery(query));
        } catch (AssumptionViolatedException e) {
            System.out.println(message);
            throw e;
        }
    }


    @Test
    public void testGetAllVariants_limit_skip_sorted_multi_regions() {
        limitSkip(new Query(VariantQueryParam.REGION.key(), "1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20"), new QueryOptions(QueryOptions.SORT, true));
    }

}
