package org.opencb.opencga.storage.mongodb.alignment;

import org.junit.Test;

import java.nio.file.Paths;


public class AlignmentMetaDataDBAdaptorTest {

    @Test
    public void generalTest(){
        AlignmentMetaDataDBAdaptor adaptor = new AlignmentMetaDataDBAdaptor("/home/jacobo/appl/files-index.properties");
        System.out.println(adaptor.registerPath(Paths.get("/tmp/noexiste.bam")));
        System.out.println(adaptor.registerPath(Paths.get("/home/jacobo/Documentos/bioinfo/HG00096.chrom20.ILLUMINA.bwa.GBR.low_coverage.20120522.bam")));
        System.out.println(adaptor.registerPath(Paths.get("/tmp/noexiste3.bam")));
        System.out.println(adaptor.registerPath(Paths.get("/tmp/noexiste4.bam")));
        System.out.println(adaptor.registerPath(Paths.get("/tmp/noexiste2.bam")));
        System.out.println(adaptor.getBamFromIndex("1"));
        System.out.println(adaptor.getBaiFromIndex("1"));
    }

}