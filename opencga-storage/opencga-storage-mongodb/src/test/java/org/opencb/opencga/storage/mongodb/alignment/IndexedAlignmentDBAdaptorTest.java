package org.opencb.opencga.storage.mongodb.alignment;

import org.junit.Test;
import org.opencb.biodata.models.feature.Region;
import org.opencb.commons.containers.QueryResult;
import org.opencb.commons.containers.map.QueryOptions;
import org.opencb.commons.test.GenericTest;
import org.opencb.opencga.storage.core.alignment.adaptors.AlignmentQueryBuilder;

import java.nio.file.Path;
import java.nio.file.Paths;

import static org.junit.Assert.*;

public class IndexedAlignmentDBAdaptorTest  extends GenericTest{

    @Test
    public void testGetAllAlignmentsByRegion(){
        MongoDBAlignmentStorageManager manager = new MongoDBAlignmentStorageManager(Paths.get("/home/jacobo/Documentos/bioinfo/opencga", "opencga.properties"));
        AlignmentMetaDataDBAdaptor metadata = manager.getMetadata();

        Path adaptorPath = null;
        adaptorPath = Paths.get("/home/jacobo/Documentos/bioinfo/opencga/sequence", "human_g1k_v37.fasta.gz.sqlite.db");
        AlignmentQueryBuilder dbAdaptor = manager.getDBAdaptor(adaptorPath);

        QueryOptions qo = new QueryOptions();
        qo.put("bam_path", metadata.getBamFromIndex("0").toString());
        qo.put("bai_path", metadata.getBaiFromIndex("0").toString());  //NOT NECESSARY
        //qo.put("view_as_pairs", true);

        //qo.put("bam_path", "/home/jacobo/Documentos/bioinfo/opencga/alignment/HG00096.chrom20.ILLUMINA.bwa.GBR.low_coverage.20120522.bam");
        //qo.put("bai_path", "/home/jacobo/Documentos/bioinfo/opencga/alignment/HG00096.chrom20.ILLUMINA.bwa.GBR.low_coverage.20120522.bam.bai");
        qo.put("process_differences", true);

        Region region = new Region("20", 20000000, 20010000);
        QueryResult alignmentsByRegion = dbAdaptor.getAllAlignmentsByRegion(region, qo);
        System.out.println(alignmentsByRegion);
    }


}