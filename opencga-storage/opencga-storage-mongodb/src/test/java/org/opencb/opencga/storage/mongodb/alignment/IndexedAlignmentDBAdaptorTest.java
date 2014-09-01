package org.opencb.opencga.storage.mongodb.alignment;

import org.junit.Before;
import org.junit.Test;
import org.opencb.biodata.models.alignment.stats.MeanCoverage;
import org.opencb.biodata.models.alignment.stats.RegionCoverage;
import org.opencb.biodata.models.feature.Region;
import org.opencb.commons.test.GenericTest;
import org.opencb.datastore.core.QueryOptions;
import org.opencb.datastore.core.QueryResult;
import org.opencb.opencga.storage.core.alignment.adaptors.AlignmentQueryBuilder;

import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;

public class IndexedAlignmentDBAdaptorTest  extends GenericTest{


    private AlignmentQueryBuilder dbAdaptor;
    private MongoDBAlignmentStorageManager manager;
    private AlignmentMetaDataDBAdaptor metadata;

    @Before
    public void before(){
        manager = new MongoDBAlignmentStorageManager(Paths.get("/media/jacobo/Nusado/opencga", "opencga.properties"));
        metadata = manager.getMetadata();

        Path adaptorPath = null;
        adaptorPath = Paths.get("/media/jacobo/Nusado/opencga/sequence", "human_g1k_v37.fasta.gz.sqlite.db");
        dbAdaptor = manager.getDBAdaptor(adaptorPath);
    }

    @Test
    public void testGetAllAlignmentsByRegion(){


        QueryOptions qo = new QueryOptions();
        qo.put(IndexedAlignmentDBAdaptor.QO_BAM_PATH, metadata.getBamFromIndex("1").toString());
        qo.put(IndexedAlignmentDBAdaptor.QO_BAI_PATH, metadata.getBaiFromIndex("1").toString());  //NOT NECESSARY
        //qo.put("view_as_pairs", true);

        //qo.put("bam_path", "/media/jacobo/Nusado/opencga/alignment/HG00096.chrom20.ILLUMINA.bwa.GBR.low_coverage.20120522.bam");
        //qo.put("bai_path", "/media/jacobo/Nusado/opencga/alignment/HG00096.chrom20.ILLUMINA.bwa.GBR.low_coverage.20120522.bam.bai");
        qo.put(IndexedAlignmentDBAdaptor.QO_PROCESS_DIFFERENCES, false);

        //Region region = new Region("20", 20000000, 20000100);
        Region region = new Region("20", 29829000, 29830000);

        QueryResult alignmentsByRegion = dbAdaptor.getAllAlignmentsByRegion(region, qo);
        System.out.println(alignmentsByRegion);
        System.out.println(alignmentsByRegion.getTime());

    }


    @Test
    public void testGetCoverageByRegion(){

        QueryOptions qo = new QueryOptions();
        qo.put(IndexedAlignmentDBAdaptor.QO_FILE_ID, "HG04239");

        //Region region = new Region("20", 20000000, 20000100);

        printQueryResult(dbAdaptor.getCoverageByRegion(new Region("20", 29829000, 29830000), qo));
        printQueryResult(dbAdaptor.getCoverageByRegion(new Region("20", 29829000, 29850000), qo));
        qo.put(IndexedAlignmentDBAdaptor.QO_COVERAGE, false);
        printQueryResult(dbAdaptor.getCoverageByRegion(new Region("20", 29829000, 29830000), qo));
        qo.put(IndexedAlignmentDBAdaptor.QO_BATCH_SIZE, 1000000);
        printQueryResult(dbAdaptor.getCoverageByRegion(new Region("20", 1, 65000000), qo));

//        System.out.println(coverageByRegion);
//        System.out.println(coverageByRegion.getTime());
    }


    private void printQueryResult(QueryResult cr){
        String s = cr.getResultType();
        System.out.println("cr.getDbTime() = " + cr.getDbTime());
        if (s.equals(MeanCoverage.class.getCanonicalName())) {
            List<MeanCoverage> meanCoverageList = cr.getResult();
            for(MeanCoverage mc : meanCoverageList){
                System.out.println(mc.getRegion().toString()+" : " + mc.getCoverage());
            }
        } else if (s.equals(RegionCoverage.class.getCanonicalName())) {
            List<RegionCoverage> regionCoverageList = cr.getResult();
            for(RegionCoverage rc : regionCoverageList){
                System.out.print(new Region(rc.getChromosome(), (int) rc.getStart(), (int) rc.getEnd()).toString()+ " (");
                for (int i = 0; i < rc.getAll().length; i++) {
                    System.out.print(rc.getAll()[i] + ",");
                }
                System.out.println(");");

            }
        }
    }

}