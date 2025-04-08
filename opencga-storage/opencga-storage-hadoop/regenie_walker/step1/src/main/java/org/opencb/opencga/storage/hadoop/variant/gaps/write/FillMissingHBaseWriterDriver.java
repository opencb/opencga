package org.opencb.opencga.storage.hadoop.variant.gaps.write;

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.opencb.commons.datastore.core.ObjectMap;
import org.opencb.opencga.storage.core.exceptions.StorageEngineException;
import org.opencb.opencga.storage.core.variant.adaptors.VariantQueryParam;
import org.opencb.opencga.storage.hadoop.utils.HBaseManager;
import org.opencb.opencga.storage.hadoop.variant.AbstractVariantsTableDriver;
import org.opencb.opencga.storage.hadoop.variant.gaps.FillGapsDriver;
import org.opencb.opencga.storage.hadoop.variant.gaps.PrepareFillMissingMapper;
import org.opencb.opencga.storage.hadoop.variant.mr.VariantMapReduceUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileNotFoundException;
import java.io.IOException;

import static org.apache.hadoop.mapreduce.MRJobConfig.JOB_RUNNING_MAP_LIMIT;
import static org.opencb.opencga.storage.hadoop.variant.HadoopVariantStorageOptions.FILL_MISSING_WRITE_MAPPERS_LIMIT_FACTOR;

/**
 * Created on 09/03/18.
 *
 * @author Jacobo Coll &lt;jacobo167@gmail.com&gt;
 */
public class FillMissingHBaseWriterDriver extends AbstractVariantsTableDriver {

    private static final Logger LOG = LoggerFactory.getLogger(FillMissingHBaseWriterDriver.class);
    private String inputPath;
    private FileSystem fs;
    private final Logger logger = LoggerFactory.getLogger(AbstractVariantsTableDriver.class);

    public FillMissingHBaseWriterDriver() {
    }

    public FillMissingHBaseWriterDriver(Configuration conf) {
        super(conf);
    }

    @Override
    protected void parseAndValidateParameters() throws IOException {
        super.parseAndValidateParameters();
        inputPath = getConf().get(FillGapsDriver.FILL_MISSING_INTERMEDIATE_FILE);
        Path path = new Path(inputPath);
        fs = path.getFileSystem(getConf());
        if (!fs.exists(path)) {
            throw new FileNotFoundException("Intermediate file not found: " + inputPath);
        }
    }

    @Override
    protected Class<PrepareFillMissingMapper> getMapperClass() {
        return PrepareFillMissingMapper.class;
    }

    @Override
    protected Job setupJob(Job job, String archiveTableName, String variantTableName) throws IOException {
        ObjectMap options = new ObjectMap();
        getConf().iterator().forEachRemaining(entry -> options.put(entry.getKey(), entry.getValue()));
        int serversSize;
        try (HBaseManager hBaseManager = new HBaseManager(getConf())) {
            serversSize = hBaseManager.act(variantTableName, (table, admin) -> admin.getClusterStatus().getServersSize());
        }
        float factor = getConf().getFloat(FILL_MISSING_WRITE_MAPPERS_LIMIT_FACTOR.key(),
                FILL_MISSING_WRITE_MAPPERS_LIMIT_FACTOR.defaultValue());
        if (factor <= 0) {
            throw new IllegalArgumentException(FILL_MISSING_WRITE_MAPPERS_LIMIT_FACTOR + " must be positive!");
        }
        int mapsLimit = Math.round(serversSize * factor);
        if (mapsLimit == 0) {
            mapsLimit = 40;
        }
        job.getConfiguration().setInt(JOB_RUNNING_MAP_LIMIT, mapsLimit);
        logger.info("Set job running map limit to " + mapsLimit + ". ServersSize: " + serversSize + ", mappersFactor: " + factor);

        // input
        FileInputFormat.setInputPaths(job, inputPath);
        job.setInputFormatClass(org.apache.hadoop.mapreduce.lib.input.SequenceFileAsBinaryInputFormat.class);

        // mapper
        job.setMapperClass(FillMissingHBaseWriterMapper.class);

        job.setSpeculativeExecution(false);

        // output
        VariantMapReduceUtil.setMultiTableOutput(job);

        VariantMapReduceUtil.setNoneReduce(job);

        return job;
    }

    @Override
    protected void postExecution(boolean succeed) throws IOException, StorageEngineException {
        super.postExecution(succeed);
        if (succeed) {
            fs.delete(new Path(inputPath), true);
        }
    }

    @Override
    protected String getJobOperationName() {
        String regionStr = getConf().get(VariantQueryParam.REGION.key());
        return "write_fill_missing" + (StringUtils.isNotEmpty(regionStr) ? "_" + regionStr : "");
    }

    public static void main(String[] args) {
        try {
            System.exit(new FillMissingHBaseWriterDriver().privateMain(args));
        } catch (Exception e) {
            LOG.error("Error executing " + FillMissingHBaseWriterDriver.class, e);
            System.exit(1);
        }
    }
}
