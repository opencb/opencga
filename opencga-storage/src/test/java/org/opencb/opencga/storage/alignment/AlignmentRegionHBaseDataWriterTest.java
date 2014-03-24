package org.opencb.opencga.storage.alignment;

import org.apache.hadoop.hbase.HBaseConfiguration;
import org.junit.Test;
import org.opencb.commons.bioformats.alignment.AlignmentRegion;
import org.opencb.commons.bioformats.alignment.io.readers.AlignmentDataReader;
import org.opencb.commons.bioformats.alignment.io.readers.AlignmentRegionDataReader;
import org.opencb.commons.bioformats.alignment.sam.io.AlignmentSamDataReader;
import org.opencb.commons.test.GenericTest;
import org.opencb.opencga.lib.auth.IllegalOpenCGACredentialsException;
import org.opencb.opencga.lib.auth.MonbaseCredentials;

/**
 * Created with IntelliJ IDEA.
 * User: josemi
 * Date: 3/24/14
 * Time: 7:57 PM
 * To change this template use File | Settings | File Templates.
 */
public class AlignmentRegionHBaseDataWriterTest extends GenericTest {

    String smallSam = getClass().getResource("/small.sam").getFile();
    String usedFile = smallSam;

    @Test
    public void samToHbaseTest () {
        String tableName = "alignmentRegion_test_jj";
        MonbaseCredentials credentials = null;
        org.apache.hadoop.conf.Configuration config;

        // Credentials for the query builder
        try {
            credentials = new MonbaseCredentials("172.24.79.30", 60010, "172.24.79.30", 2181, "localhost", 9999, tableName, "cgonzalez", "cgonzalez");
        } catch (IllegalOpenCGACredentialsException e) {
            e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
        }

        // HBase configuration with the active credentials
        config = HBaseConfiguration.create();
        config.set("hbase.master", credentials.getHbaseMasterHost() + ":" + credentials.getHbaseMasterPort());
        config.set("hbase.zookeeper.quorum", credentials.getHbaseZookeeperQuorum());
        config.set("hbase.zookeeper.property.clientPort", String.valueOf(credentials.getHbaseZookeeperClientPort()));

        //Reader
        AlignmentDataReader alignmentDataReader = new AlignmentSamDataReader(usedFile);
        AlignmentRegionDataReader alignmentRegionDataReader = new AlignmentRegionDataReader(alignmentDataReader,100);
        //Writer
        AlignmentRegionHBaseDataWriter alignmentRegionHBaseDataWriter =
                new AlignmentRegionHBaseDataWriter(credentials, tableName);

        // end setup. start stuff
        alignmentRegionDataReader.open();
        alignmentRegionDataReader.pre();

        alignmentRegionHBaseDataWriter.open();
        alignmentRegionHBaseDataWriter.pre();

        AlignmentRegion alignmentRegion = alignmentRegionDataReader.read();
        int i = 0;
        if (alignmentRegion != null) {
            alignmentRegionHBaseDataWriter.write(alignmentRegion);
            i++;
        }
        System.out.println("writed " + i + " AlignmentRegions in HBase");

        alignmentRegionDataReader.post();
        alignmentRegionDataReader.close();

        alignmentRegionHBaseDataWriter.post();
        alignmentRegionHBaseDataWriter.close();
    }
}
