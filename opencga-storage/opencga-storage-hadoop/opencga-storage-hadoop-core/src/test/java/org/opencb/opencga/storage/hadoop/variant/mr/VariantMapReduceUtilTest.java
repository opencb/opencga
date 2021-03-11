package org.opencb.opencga.storage.hadoop.variant.mr;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.Job;
import org.junit.Test;
import org.opencb.opencga.storage.core.config.SampleIndexConfiguration;

import java.io.IOException;

import static org.junit.Assert.*;

public class VariantMapReduceUtilTest {

    @Test
    public void testSerializeConfiguration() throws IOException {
        Job job = Job.getInstance();
        SampleIndexConfiguration configuration = SampleIndexConfiguration.defaultConfiguration();
        VariantMapReduceUtil.setSampleIndexConfiguration(job, configuration);
        SampleIndexConfiguration actualConfiguration = VariantMapReduceUtil.getSampleIndexConfiguration(job.getConfiguration());
//        System.out.println("configuration = " + configuration);
        assertEquals(configuration, actualConfiguration);
    }

}