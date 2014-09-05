package org.opencb.opencga.storage.mongodb.alignment;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.Before;
import org.junit.Test;
import org.opencb.biodata.models.alignment.Alignment;
import org.opencb.biodata.models.alignment.stats.MeanCoverage;
import org.opencb.biodata.models.alignment.stats.RegionCoverage;
import org.opencb.biodata.models.feature.Region;
import org.opencb.commons.test.GenericTest;
import org.opencb.datastore.core.QueryOptions;
import org.opencb.datastore.core.QueryResult;
import org.opencb.opencga.storage.core.alignment.adaptors.AlignmentQueryBuilder;
import org.opencb.opencga.storage.core.alignment.json.AlignmentDifferenceJsonMixin;

import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
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
        metadata = new AlignmentMetaDataDBAdaptor(manager.getProperties().getProperty("files-index", "/tmp/files-index.properties"));
        //manager.getMetadata();

        Path adaptorPath = null;
        adaptorPath = Paths.get("/media/jacobo/Nusado/opencga/sequence", "human_g1k_v37.fasta.gz.sqlite.db");
        dbAdaptor = manager.getDBAdaptor(adaptorPath);
    }

    @Test
    public void testGetAllAlignmentsByRegion() throws IOException {


        QueryOptions qo = new QueryOptions();
//        qo.put(IndexedAlignmentDBAdaptor.QO_BAM_PATH, metadata.getBamFromIndex("1").toString());
//        qo.put(IndexedAlignmentDBAdaptor.QO_BAI_PATH, metadata.getBaiFromIndex("1").toString());  //NOT NECESSARY
        //qo.put("view_as_pairs", true);

        qo.put("bam_path", "/media/jacobo/Nusado/opencga/alignment/HG04239.chrom20.ILLUMINA.bwa.ITU.low_coverage.20130415.bam");
        qo.put("bai_path", "/media/jacobo/Nusado/opencga/alignment/HG04239.chrom20.ILLUMINA.bwa.ITU.low_coverage.20130415.bam.bai");
        qo.put(IndexedAlignmentDBAdaptor.QO_PROCESS_DIFFERENCES, false);

        //Region region = new Region("20", 20000000, 20000100);
        Region region = new Region("20", 29829000, 29830000);

        QueryResult alignmentsByRegion = dbAdaptor.getAllAlignmentsByRegion(region, qo);
        printQueryResult(alignmentsByRegion);
        jsonQueryResult("HG04239",alignmentsByRegion);

        qo.put(IndexedAlignmentDBAdaptor.QO_PROCESS_DIFFERENCES, true);
        alignmentsByRegion = dbAdaptor.getAllAlignmentsByRegion(new Region("20", 29829000, 29829500), qo);
        printQueryResult(alignmentsByRegion);
        jsonQueryResult("HG04239",alignmentsByRegion);

        alignmentsByRegion = dbAdaptor.getAllAlignmentsByRegion(new Region("20", 29829500, 29830000), qo);
        printQueryResult(alignmentsByRegion);
        jsonQueryResult("HG04239",alignmentsByRegion);

    }

    @Test
    public void testGetCoverageByRegion2() throws IOException {

        QueryOptions qo = new QueryOptions();
        qo.put(IndexedAlignmentDBAdaptor.QO_FILE_ID, "HG04239");
        jsonQueryResult("HG04239.coverage",dbAdaptor.getCoverageByRegion(new Region("20", 29829001, 29830000), qo));
        jsonQueryResult("HG04239.coverage",dbAdaptor.getCoverageByRegion(new Region("20", 29830001, 29833000), qo));
        qo.put(IndexedAlignmentDBAdaptor.QO_COVERAGE, false);
        jsonQueryResult("HG04239.mean-coverage.1k",dbAdaptor.getCoverageByRegion(new Region("20", 29800000, 29900000), qo));

    }


    @Test
    public void testGetCoverageByRegion() throws IOException {

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

    private void jsonQueryResult(String name, QueryResult qr) throws IOException {
        JsonFactory factory = new JsonFactory();
        ObjectMapper jsonObjectMapper = new ObjectMapper(factory);
        jsonObjectMapper.addMixInAnnotations(Alignment.AlignmentDifference.class, AlignmentDifferenceJsonMixin.class);
        JsonGenerator generator = factory.createGenerator(new FileOutputStream("/tmp/"+name+"."+qr.getId()+".json"));

        generator.writeObject(qr.getResult());

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