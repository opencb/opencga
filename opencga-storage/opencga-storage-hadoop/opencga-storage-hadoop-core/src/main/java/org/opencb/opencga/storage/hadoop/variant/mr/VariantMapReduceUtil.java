package org.opencb.opencga.storage.hadoop.variant.mr;


import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.mapreduce.TableInputFormat;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.mapreduce.Job;

import java.io.IOException;

/**
 * Created on 27/10/17.
 *
 * @author Jacobo Coll &lt;jacobo167@gmail.com&gt;
 */
public class VariantMapReduceUtil {

    public static void setInputHBase(Job job, String variantTableName) throws IOException {
        TableMapReduceUtil.initTableMapperJob(
                variantTableName,      // input table
                new Scan(),             // Scan instance to control CF and attribute selection
                TableMapper.class,      // mapper class
                null,             // mapper output key
                null,             // mapper output value
                job,
                false);

        job.getConfiguration().set(TableInputFormat.INPUT_TABLE, variantTableName);
        job.setInputFormatClass(HBaseVariantTableInputFormat.class);
    }

}
