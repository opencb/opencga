package org.opencb.opencga.storage.hadoop.variant.mr;

import org.apache.hadoop.mapreduce.Job;
import org.junit.Test;
import org.opencb.opencga.core.config.storage.SampleIndexConfiguration;

import java.io.IOException;

import static org.junit.Assert.*;

public class VariantMapReduceUtilTest {

    @Test
    public void testSerializeConfiguration() throws IOException {
        Job job = Job.getInstance();
        SampleIndexConfiguration configuration = SampleIndexConfiguration.defaultConfiguration();
        int version = 156;
        VariantMapReduceUtil.setSampleIndexConfiguration(job, configuration, version);
        SampleIndexConfiguration actualConfiguration = VariantMapReduceUtil.getSampleIndexConfiguration(job.getConfiguration());
        int actualVersion = VariantMapReduceUtil.getSampleIndexConfigurationVersion(job.getConfiguration());
//        System.out.println("configuration = " + configuration);
        assertEquals(configuration, actualConfiguration);
        assertEquals(version, actualVersion);
    }

}