package org.opencb.opencga.storage.hadoop.variant.mr;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.mapreduce.TableInputFormat;

/**
 * Created on 18/02/19.
 *
 * @author Jacobo Coll &lt;jacobo167@gmail.com&gt;
 */
public class PartialResultTableInputFormat extends TableInputFormat {

    @Override
    public void setConf(Configuration configuration) {
        super.setConf(configuration);
        getScan().setAllowPartialResults(true);
    }
}
