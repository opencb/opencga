package org.opencb.opencga.storage.mongodb.sequence;

import org.junit.Test;
import org.opencb.biodata.formats.io.FileFormatException;
import org.opencb.biodata.formats.sequence.fasta.dbadaptor.CellBaseSequenceDBAdaptor;
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
        SequenceDBAdaptor sql = new SqliteSequenceDBAdaptor(Paths.get("/home/jacobo/Documentos/bioinfo/opencga/sequence/human_g1k_v37.fasta.gz.sqlite.db"));
        SequenceDBAdaptor cellbase = new CellBaseSequenceDBAdaptor();
        long start;
        long end;
        Region region = new Region("2", 1000000, 1000015);


        sql.open();
        start = System.currentTimeMillis();
        System.out.println(sql.getSequence(region));
        end = System.currentTimeMillis();
        sql.close();
        System.out.println(end - start + " ms");


        cellbase.open();
        start = System.currentTimeMillis();
        System.out.println(cellbase.getSequence(region));
        end = System.currentTimeMillis();
        cellbase.close();
        System.out.println(end - start + " ms");

    }

    @Test
    public void createDB() throws IOException, SQLException, FileFormatException {
        SqliteSequenceDBAdaptor sql = new SqliteSequenceDBAdaptor();
        sql.createDB(Paths.get("/home/jacobo/Documentos/bioinfo/human_g1k_v37.fasta.gz"));
    }

}