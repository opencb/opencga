package org.opencb.opencga.storage.alignment;

import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.MasterNotRunningException;
import org.apache.hadoop.hbase.ZooKeeperConnectionException;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.junit.Ignore;
import org.junit.Test;
import org.opencb.commons.bioformats.alignment.AlignmentRegion;
import org.opencb.commons.bioformats.alignment.io.readers.AlignmentDataReader;
import org.opencb.commons.bioformats.alignment.io.readers.AlignmentRegionDataReader;
import org.opencb.commons.bioformats.alignment.sam.io.AlignmentSamDataReader;
import org.opencb.commons.bioformats.alignment.stats.AlignmentCoverageCalculatorTask;
import org.opencb.commons.io.DataWriter;
import org.opencb.commons.run.Runner;
import org.opencb.commons.run.Task;
import org.opencb.commons.test.GenericTest;
import org.opencb.opencga.lib.auth.IllegalOpenCGACredentialsException;
import org.opencb.opencga.lib.auth.MonbaseCredentials;

import java.io.IOException;
import java.util.LinkedList;
import java.util.List;

/**
 * Created with IntelliJ IDEA.
 * User: jcoll
 * Date: 2/20/14
 * Time: 7:12 PM
 * To change this template use File | Settings | File Templates.
 */
public class AlignmentRegionCoverageHBaseDataWriterTest extends GenericTest {

    String shortSam = getClass().getResource("/alignments_small.sam").getFile();


    @Test
    public void openClose_Test(){
        String tableName = "coverage_test_jj";
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



        AlignmentRegionCoverageHBaseDataWriter alignmentRegionCoverageHBaseDataWriter =
                new AlignmentRegionCoverageHBaseDataWriter(config);

        alignmentRegionCoverageHBaseDataWriter.open();
        alignmentRegionCoverageHBaseDataWriter.pre();

        alignmentRegionCoverageHBaseDataWriter.post();
        alignmentRegionCoverageHBaseDataWriter.close();



    }

    @Test
    public void AlignmentRegionCoverageHBaseDataWriterTest_1(){

        //Reader
        AlignmentDataReader alignmentDataReader = new AlignmentSamDataReader(shortSam);
        AlignmentRegionDataReader alignmentRegionDataReader = new AlignmentRegionDataReader(alignmentDataReader);

        //Task
        List<Task<AlignmentRegion>> tasks = new LinkedList<>();
        AlignmentCoverageCalculatorTask alignmentCoverageCalculatorTask =
                new AlignmentCoverageCalculatorTask();
        alignmentCoverageCalculatorTask.addMeanCoverageCalculator(10   ,"010");
        alignmentCoverageCalculatorTask.addMeanCoverageCalculator(100  ,"100");
        alignmentCoverageCalculatorTask.addMeanCoverageCalculator(1000 ,"01K");
        alignmentCoverageCalculatorTask.addMeanCoverageCalculator(10000,"10K");
        tasks.add(alignmentCoverageCalculatorTask);
        //Writer
        List<DataWriter<AlignmentRegion>> writers = new LinkedList<>();
        // HBase configuration with the active credentials
        String tableName = "coverage_test_jj";
        MonbaseCredentials credentials = null;
        org.apache.hadoop.conf.Configuration config;


        // Credentials for the query builder
        try {
            credentials = new MonbaseCredentials("172.24.79.30", 60010, "172.24.79.30", 2181, "localhost", 9999, tableName, "jcoll", "jcoll");
        } catch (IllegalOpenCGACredentialsException e) {
            e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
        }

        // HBase configuration with the active credentials
        config = HBaseConfiguration.create();
        config.set("hbase.master", credentials.getHbaseMasterHost() + ":" + credentials.getHbaseMasterPort());
        config.set("hbase.zookeeper.quorum", credentials.getHbaseZookeeperQuorum());
        config.set("hbase.zookeeper.property.clientPort", String.valueOf(credentials.getHbaseZookeeperClientPort()));



        AlignmentRegionCoverageHBaseDataWriter alignmentRegionCoverageHBaseDataWriter =
                new AlignmentRegionCoverageHBaseDataWriter(config);
        alignmentRegionCoverageHBaseDataWriter.setTableName(tableName);
        alignmentRegionCoverageHBaseDataWriter.setSample("Sample1");
        alignmentRegionCoverageHBaseDataWriter.setColumnFamilyName("Family1");


        writers.add(alignmentRegionCoverageHBaseDataWriter);

        Runner<AlignmentRegion> runner = new Runner<>(alignmentRegionDataReader,writers,tasks);

        try {
            runner.run();
        } catch (IOException e) {
            e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
        }
    }
}
