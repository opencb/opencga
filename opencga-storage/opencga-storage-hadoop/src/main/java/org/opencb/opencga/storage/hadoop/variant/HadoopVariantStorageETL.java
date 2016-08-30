package org.opencb.opencga.storage.hadoop.variant;

import org.apache.hadoop.conf.Configuration;
import org.opencb.commons.datastore.core.ObjectMap;
import org.opencb.opencga.storage.core.config.StorageConfiguration;
import org.opencb.opencga.storage.core.exceptions.StorageManagerException;
import org.opencb.opencga.storage.core.variant.io.VariantReaderUtils;
import org.opencb.opencga.storage.hadoop.auth.HBaseCredentials;
import org.opencb.opencga.storage.hadoop.variant.adaptors.VariantHadoopDBAdaptor;
import org.opencb.opencga.storage.hadoop.variant.archive.ArchiveDriver;
import org.opencb.opencga.storage.hadoop.variant.executors.MRExecutor;
import org.slf4j.LoggerFactory;

import java.net.URI;

import static org.opencb.opencga.storage.hadoop.variant.HadoopVariantStorageManager.HADOOP_BIN;
import static org.opencb.opencga.storage.hadoop.variant.HadoopVariantStorageManager.Options;

/**
 * Created on 31/03/16.
 *
 * @author Jacobo Coll &lt;jacobo167@gmail.com&gt;
 */
public class HadoopVariantStorageETL extends AbstractHadoopVariantStorageETL {


    public HadoopVariantStorageETL(
            StorageConfiguration configuration, String storageEngineId,
            VariantHadoopDBAdaptor dbAdaptor, MRExecutor mrExecutor,
            Configuration conf, HBaseCredentials archiveCredentials,
            VariantReaderUtils variantReaderUtils, ObjectMap options) {
        super(configuration, storageEngineId, LoggerFactory.getLogger(HadoopVariantStorageETL.class), dbAdaptor, variantReaderUtils,
                options, archiveCredentials, mrExecutor, conf);
    }


    protected void loadArch(URI input) throws StorageManagerException {
        int studyId = getStudyId();
        URI vcfMeta = URI.create(VariantReaderUtils.getMetaFromTransformedFile(input.toString()));
        int fileId = options.getInt(Options.FILE_ID.key());

        String hadoopRoute = options.getString(HADOOP_BIN, "hadoop");
        String jar = getJarWithDependencies();

        Class execClass = ArchiveDriver.class;
        String executable = hadoopRoute + " jar " + jar + " " + execClass.getName();
        String args = ArchiveDriver.buildCommandLineArgs(input, vcfMeta,
                archiveTableCredentials.getHostUri().toString(), archiveTableCredentials.getTable(), studyId,
                fileId, options);

        long startTime = System.currentTimeMillis();
        logger.info("------------------------------------------------------");
        logger.info("Loading file {} into archive table '{}'", fileId, archiveTableCredentials.getTable());
        logger.debug(executable + " " + args);
        logger.info("------------------------------------------------------");
        int exitValue = mrExecutor.run(executable, args);
        logger.info("------------------------------------------------------");
        logger.info("Exit value: {}", exitValue);
        logger.info("Total time: {}s", (System.currentTimeMillis() - startTime) / 1000.0);
        if (exitValue != 0) {
            throw new StorageManagerException("Error loading file " + input + " into archive table \""
                    + archiveTableCredentials.getTable() + "\"");
        }
    }


    @Override
    protected boolean needLoadFromHdfs() {
        return true;
    }
}
