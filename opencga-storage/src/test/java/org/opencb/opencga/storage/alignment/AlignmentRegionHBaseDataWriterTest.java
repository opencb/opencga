package org.opencb.opencga.storage.alignment;

import net.sf.samtools.SAMFileHeader;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.junit.Test;
import org.opencb.commons.bioformats.alignment.Alignment;
import org.opencb.commons.bioformats.alignment.AlignmentRegion;
import org.opencb.commons.bioformats.alignment.io.readers.AlignmentDataReader;
import org.opencb.commons.bioformats.alignment.io.readers.AlignmentRegionDataReader;
import org.opencb.commons.bioformats.alignment.sam.io.AlignmentBamDataReader;
import org.opencb.commons.bioformats.alignment.sam.io.AlignmentSamDataReader;
import org.opencb.commons.bioformats.alignment.sam.io.AlignmentSamDataWriter;
import org.opencb.commons.bioformats.feature.Region;
import org.opencb.commons.containers.QueryResult;
import org.opencb.commons.containers.map.QueryOptions;
import org.opencb.commons.test.GenericTest;
import org.opencb.opencga.lib.auth.IllegalOpenCGACredentialsException;
import org.opencb.opencga.lib.auth.MonbaseCredentials;

import java.util.List;

/**
 * Created with IntelliJ IDEA.
 * User: josemi
 * Date: 3/24/14
 * Time: 7:57 PM
 * To change this template use File | Settings | File Templates.
 */
public class AlignmentRegionHBaseDataWriterTest extends GenericTest {

    String smallSam = getClass().getResource("/small.sam").getFile();
  //  String chrom20Sam = getClass().getResource("/chrom20.bam").getFile();
    String usedFile = smallSam;

    @Test
    public void samToHbaseTest () {
        String tableName = "alignmentRegion_test_snappy_jj";
        MonbaseCredentials credentials = null;
        Configuration config;

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
        AlignmentRegionDataReader alignmentRegionDataReader = new AlignmentRegionDataReader(alignmentDataReader,8000,100000);
        //Writer
        AlignmentRegionHBaseDataWriter alignmentRegionHBaseDataWriter =
                new AlignmentRegionHBaseDataWriter(credentials, tableName);

        // end setup. start stuff
        alignmentRegionDataReader.open();
        alignmentRegionDataReader.pre();

        SAMFileHeader header = (SAMFileHeader) alignmentDataReader.getHeader();

        alignmentRegionHBaseDataWriter.open();
        alignmentRegionHBaseDataWriter.pre();

        AlignmentRegion alignmentRegion = alignmentRegionDataReader.read();
        int i = 0;
        while (alignmentRegion != null) {
            alignmentRegionHBaseDataWriter.write(alignmentRegion);
            i++;
            alignmentRegion = alignmentRegionDataReader.read();
        }
        System.out.println("written " + i + " AlignmentRegions in HBase");

        alignmentRegionDataReader.post();
        alignmentRegionDataReader.close();

        alignmentRegionHBaseDataWriter.post();
        alignmentRegionHBaseDataWriter.close();












        // HBase configuration with the active credentials

        AlignmentHBaseQueryBuilder alignmentHBaseQueryBuilder = new AlignmentHBaseQueryBuilder(config, tableName);

        //alignmentHBaseQueryBuilder.setColumnFamilyName("Family1");


        Region region = new Region("20", 60000, 76177);
        QueryOptions queryOptions = new QueryOptions();
        QueryResult queryResult = alignmentHBaseQueryBuilder.getAllAlignmentsByRegion(region, queryOptions);

        System.out.println("QueryResult size: " + queryResult.getResult().size());
        for(Object object : queryResult.getResult()){
            Alignment alignment =  ((Alignment) object);
            System.out.println(alignment.getName() + " : " + alignment.getStart() + " " + alignment.getEnd());
        }
        List<Alignment> alignments = queryResult.getResult();


        AlignmentSamDataWriter alignmentSamDataWriter = new AlignmentSamDataWriter("/tmp/salida.sam", header);

        alignmentSamDataWriter.open();
        alignmentSamDataWriter.pre();

        alignmentSamDataWriter.write(alignments);

        alignmentSamDataWriter.post();
        alignmentSamDataWriter.close();


        System.out.println("Tadaaaaaaa");




    }

    @Test
    public void compareFromSamAndFromHbase() {
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

        AlignmentHBaseQueryBuilder alignmentHBaseQueryBuilder = new AlignmentHBaseQueryBuilder(config, tableName);

        QueryResult region = alignmentHBaseQueryBuilder.getAllAlignmentsByRegion(new Region("20", 61000, 61100), new QueryOptions());

        for (Object o:region.getResult()) {
            Alignment alignment = (Alignment) o;
            System.out.println("received alignment starting at " + alignment.getStart());
            System.out.println("    end: " + alignment.getEnd());
            System.out.println("    length: " + alignment.getLength());
            System.out.println("    name: " + alignment.getName());
            System.out.println("    inferred insert size: " + alignment.getInferredInsertSize());
            System.out.println("    pnext: " + alignment.getMateAlignmentStart());
        }

    }

    @Test
    public void removeBQ (){
        usedFile = getClass().getResource("/chrom20.bam").getFile();
        String tableName = "alignmentRegion_with_BQ_jj";
        MonbaseCredentials credentials = null;
        Configuration config;

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
        AlignmentDataReader alignmentDataReader = new AlignmentBamDataReader(usedFile);
        AlignmentRegionDataReader alignmentRegionDataReader = new AlignmentRegionDataReader(alignmentDataReader,8000,100000);
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
        while (alignmentRegion != null) {
            alignmentRegionHBaseDataWriter.write(alignmentRegion);
            i++;
            alignmentRegion = alignmentRegionDataReader.read();
        }
        System.out.println("written " + i + " AlignmentRegions in HBase");

        alignmentRegionDataReader.post();
        alignmentRegionDataReader.close();

        alignmentRegionHBaseDataWriter.post();
        alignmentRegionHBaseDataWriter.close();
    }

}
