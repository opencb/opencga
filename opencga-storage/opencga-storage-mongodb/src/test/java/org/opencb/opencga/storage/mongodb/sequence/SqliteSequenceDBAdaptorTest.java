package org.opencb.opencga.storage.mongodb.sequence;

import org.junit.Test;
import org.opencb.biodata.formats.io.FileFormatException;
import org.opencb.biodata.formats.sequence.fasta.dbadaptor.SequenceDBAdaptor;
import org.opencb.biodata.models.feature.Region;
import org.opencb.commons.test.GenericTest;
import org.opencb.opencga.storage.mongodb.sequence.SqliteSequenceDBAdaptor;

import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.sql.SQLException;

public class SqliteSequenceDBAdaptorTest extends GenericTest {

    Path fasta = Paths.get("/home/jacobo/Documentos/bioinfo/human_g1k_v37.fasta");
    Path gz = Paths.get("/home/jacobo/Documentos/bioinfo/human_g1k_v37.fasta.gz");

    @Test
    public void queryDB() throws IOException, SQLException {
        SequenceDBAdaptor sql = new SqliteSequenceDBAdaptor(Paths.get("/home/jacobo/Documentos/bioinfo/human_g1k_v37.fasta.gz.sqlite.db"));
        //SequenceDBAdaptor sql = new CellBaseSequenceDBAdaptor();


        sql.open();

        long start = System.currentTimeMillis();
        System.out.println(sql.getSequence(new Region("2", 1010000, 1030000)).length() + " Elements");
        long end = System.currentTimeMillis();
        System.out.println(end-start + " ms");
        sql.close();

    }

    @Test
    public void crehtoateDB() throws IOException, SQLException, FileFormatException {
        SqliteSequenceDBAdaptor sql = new SqliteSequenceDBAdaptor();
        sql.createDB(Paths.get("/home/jacobo/Documentos/bioinfo/human_g1k_v37.fasta.gz"));
    }

}