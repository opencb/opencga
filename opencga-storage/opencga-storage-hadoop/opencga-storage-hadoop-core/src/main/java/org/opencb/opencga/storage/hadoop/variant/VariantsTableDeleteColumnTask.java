package org.opencb.opencga.storage.hadoop.variant;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.Mutation;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.opencb.opencga.storage.hadoop.utils.DeleteHBaseColumnDriver;
import org.opencb.opencga.storage.hadoop.variant.search.HadoopVariantSearchIndexUtils;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * Created on 23/04/18.
 *
 * @author Jacobo Coll &lt;jacobo167@gmail.com&gt;
 */
public class VariantsTableDeleteColumnTask extends DeleteHBaseColumnDriver.DeleteHBaseColumnTask {

    @Override
    protected void setup(Configuration configuration) throws IOException {
        super.setup(configuration);
        initCounter("PUT_not_sync_status");
    }

    @Override
    protected List<Mutation> map(Result result) {
        List<Mutation> mutations = super.map(result);
        if (mutations.isEmpty()) {
            return mutations;
        } else {
            Put put = new Put(result.getRow());
            HadoopVariantSearchIndexUtils.addNotSyncStatus(put, GenomeHelper.COLUMN_FAMILY_BYTES);

            List<Mutation> mutationsWithPut = new ArrayList<>(mutations.size() + 1);
            mutationsWithPut.addAll(mutations);
            mutationsWithPut.add(put);
            count("PUT_not_sync_status");
            return mutationsWithPut;
        }
    }
}
