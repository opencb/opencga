package org.opencb.opencga.storage.hadoop.variant.mr;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.mapreduce.MultiTableInputFormat;

/**
 * Created on 17/07/18.
 *
 * @author Jacobo Coll &lt;jacobo167@gmail.com&gt;
 */
public class MultiRegionSampleIndexTableInputFormat extends MultiTableInputFormat {

    @Override
    public void setConf(Configuration configuration) {
        super.setConf(configuration);
        setTableRecordReader(new SampleIndexTableRecordReader(configuration));
    }

}
