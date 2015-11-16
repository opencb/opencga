/**
 * 
 */
package org.opencb.opencga.storage.hadoop.mr;

import java.io.IOException;
import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.util.ToolRunner;
import org.opencb.opencga.storage.hadoop.auth.HadoopCredentials;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author Matthias Haimel mh719+git@cam.ac.uk
 *
 */
public class GenomeVariantLoadDriver {
    private static final Logger LOG = LoggerFactory.getLogger(GenomeVariantLoadDriver.class);

    public GenomeVariantLoadDriver () {/* nothing */ }


    public static int load(HadoopCredentials db, URI vcfFile, URI vcfMetaFile) throws IOException{

        /*  SERVER details  */
        String master = String.join(":", db.getHost(), Integer.toString(db.getHbasePort()));

        Configuration conf = new Configuration();
        conf.set(HConstants.ZOOKEEPER_QUORUM, db.getHost());
        conf.set(GenomeVariantDriver.HBASE_MASTER, master);
        conf.set(GenomeVariantDriver.OPT_TABLE_NAME,db.getTable());
        conf.set(GenomeVariantDriver.OPT_VCF_FILE,vcfFile.toString());
        conf.set(GenomeVariantDriver.OPT_VCF_META_FILE,vcfMetaFile.toString());
        LOG.info(String.format(" %s %s ", GenomeVariantDriver.HBASE_MASTER,master));
        LOG.info(String.format(" %s %s ", HConstants.ZOOKEEPER_QUORUM,db.getHost()));
        LOG.info(String.format(" %s %s ", GenomeVariantDriver.OPT_TABLE_NAME, db.getTable()));
        LOG.info(String.format(" %s %s ", GenomeVariantDriver.OPT_VCF_FILE,vcfFile.toString()));
        LOG.info(String.format(" %s %s ", GenomeVariantDriver.OPT_VCF_META_FILE,vcfMetaFile.toString()));

        int exitCode;
        try {
            exitCode = ToolRunner.run(conf,new GenomeVariantDriver(), new String[]{});
        } catch (Exception e) {
           LOG.error("Problems running Hadoop job ...", e);
           throw new IOException(e);
        }
        return exitCode;
    }

}
