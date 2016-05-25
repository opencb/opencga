/*
 * Copyright 2015 OpenCB
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.opencb.opencga.storage.core.sequence;

import org.junit.Test;
import org.opencb.biodata.formats.io.FileFormatException;
import org.opencb.biodata.formats.sequence.fasta.dbadaptor.CellBaseSequenceDBAdaptor;
import org.opencb.biodata.formats.sequence.fasta.dbadaptor.SequenceDBAdaptor;
import org.opencb.biodata.models.core.Region;
import org.opencb.commons.test.GenericTest;

import java.io.File;
import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.sql.SQLException;

public class SqliteSequenceDBAdaptorTest extends GenericTest {

    Path fasta = Paths.get("/home/jacobo/Documentos/bioinfo/human_g1k_v37.fasta");
    Path gz = Paths.get("/home/jacobo/Documentos/bioinfo/human_g1k_v37.fasta.gz");

    @Test
    public void queryDB() throws IOException, SQLException {
        Path input = Paths.get("/home/jacobo/Documentos/bioinfo/opencga/sequence/human_g1k_v37.fasta.gz.sqlite.db");
        if (!input.toFile().exists()) {
            return;
        }
        SequenceDBAdaptor sql = new SqliteSequenceDBAdaptor(input);
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
    public void indexDB() throws IOException, SQLException, FileFormatException {
        SqliteSequenceDBAdaptor sql = new SqliteSequenceDBAdaptor();
        File file = Paths.get("/home/jacobo/Documentos/bioinfo/human_g1k_v37.fasta.gz").toFile();
        if (!file.exists()) {
            return;
        }
        sql.index(file, null);
    }

}