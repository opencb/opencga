package org.opencb.opencga.storage.hadoop.variant.mr;

import org.apache.hadoop.hbase.mapreduce.TableInputFormat;
import org.apache.hadoop.mapreduce.JobContext;

import java.io.IOException;

/**
 * Created on 10/07/18.
 *
 * @author Jacobo Coll &lt;jacobo167@gmail.com&gt;
 */
public class SampleIndexTableInputFormat extends TableInputFormat {

    @Override
    protected void initialize(JobContext context) throws IOException {
        super.initialize(context);
        setTableRecordReader(new SampleIndexTableRecordReader(context.getConfiguration()));
    }

}
