package org.opencb.opencga.storage.core.alignment.local;

import htsjdk.samtools.SAMFileHeader;
import htsjdk.samtools.SAMSequenceRecord;
import org.opencb.biodata.formats.io.FileFormatException;
import org.opencb.biodata.tools.alignment.AlignmentManager;
import org.opencb.biodata.tools.alignment.AlignmentUtils;
import org.opencb.commons.utils.FileUtils;
import org.opencb.opencga.storage.core.alignment.AlignmentDBAdaptor;
import org.opencb.opencga.storage.core.alignment.AlignmentStorageETL;
import org.opencb.opencga.storage.core.exceptions.StorageManagerException;

import java.io.FileInputStream;
import java.io.IOException;
import java.net.URI;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.Statement;
import java.util.List;

/**
 * Created by pfurio on 31/10/16.
 */
public class DefaultAlignmentStorageETL extends AlignmentStorageETL {

    private static final int MINOR_CHUNK_SIZE = 1000;
    private static final int MAJOR_CHUNK_SIZE = 10000;

    private Path workspace;

    @Deprecated
    public DefaultAlignmentStorageETL(AlignmentDBAdaptor dbAdaptor) {
        super(dbAdaptor);
    }

    public DefaultAlignmentStorageETL(AlignmentDBAdaptor dbAdaptor, Path workspace) {
        super(dbAdaptor);
        this.workspace = workspace;
    }

    @Override
    public URI extract(URI input, URI ouput) throws StorageManagerException {
        return input;
    }

    @Override
    public URI preTransform(URI input) throws IOException, FileFormatException, StorageManagerException {
        // Check if a BAM file is passed and it is sorted.
        // Only binaries and sorted BAM files are accepted at this point.
        Path inputPath = Paths.get(input.getRawPath());
        AlignmentUtils.checkBamOrCramFile(new FileInputStream(inputPath.toFile()), inputPath.getFileName().toString(), true);
        return input;
    }

    @Override
    public URI transform(URI input, URI pedigree, URI output) throws Exception {
        Path path = Paths.get(input.getRawPath());
        FileUtils.checkFile(path);

        // Check if the bai exists
        if (!path.getParent().resolve(path.getFileName().toString() + ".bai").toFile().exists()) {
            AlignmentManager alignmentManager = new AlignmentManager(path);
            alignmentManager.createIndex();
        }

        return input;
    }

    @Override
    public URI postTransform(URI input) throws Exception {
        Path path = Paths.get(input.getRawPath());
        FileUtils.checkFile(path);

        AlignmentManager alignmentManager = new AlignmentManager(path);

        // 2. Calculate stats and store in di
//        AlignmentGlobalStats stats = dbAdaptor.stats(input.getRawPath());
//        // TODO: Store in SQLite
//        ObjectMapper objectMapper = new ObjectMapper();
//        ObjectWriter objectWriter = objectMapper.typedWriter(AlignmentGlobalStats.class);
//        Path statsPath = path.getParent().resolve(path.getFileName() + ".stats");
//        objectWriter.writeValue(statsPath.toFile(), stats);

        // 3. Calculate coverage and store in SQLite

        SAMFileHeader fileHeader = AlignmentUtils.getFileHeader(path);
        initDatabase(fileHeader.getSequenceDictionary().getSequences());
//        Iterator<SAMSequenceRecord> iterator = fileHeader.getSequenceDictionary().getSequences().iterator();
//        while (iterator.hasNext()) {
//            SAMSequenceRecord next = iterator.next();
//            for (int i = 0; i < next.getSequenceLength(); i += MAJOR_CHUNK_SIZE) {
//                Region region = new Region(next.getSequenceName(), i, i + MAJOR_CHUNK_SIZE);
//                RegionDepth depth = alignmentManager.depth(region, null, null);
//            }
//          System.out.println(iterator.next().);
//        }

        return input;
    }

    @Override
    public URI preLoad(URI input, URI output) throws IOException, StorageManagerException {
        return null;
    }

    @Override
    public URI load(URI input) throws IOException, StorageManagerException {
        return null;
    }

    @Override
    public URI postLoad(URI input, URI output) throws IOException, StorageManagerException {
        return null;
    }

    private void initDatabase(List<SAMSequenceRecord> sequenceRecordList) {
        Path coverageDBPath = workspace.toAbsolutePath().resolve("coverage.db");
        if (!coverageDBPath.toFile().exists()) {
            Connection c = null;
            Statement stmt = null;
            try {
                Class.forName("org.sqlite.JDBC");
                c = DriverManager.getConnection("jdbc:sqlite:" + coverageDBPath);

                // Create tables
                stmt = c.createStatement();
                String sql = "CREATE TABLE chunk "
                        + "(id INTEGER PRIMARY KEY AUTOINCREMENT,"
                        + "chunk_id VARCHAR NOT NULL,"
                        + "chromosome VARCHAR NOT NULL, "
                        + "start INT NOT NULL, "
                        + "end INT NOT NULL); "
                        + "CREATE UNIQUE INDEX chunk_id_idx ON chunk (chunk_id);";
                stmt.executeUpdate(sql);

                sql = "CREATE TABLE file "
                        + "(id INTEGER PRIMARY KEY AUTOINCREMENT,"
                        + "path VARCHAR NOT NULL,"
                        + "name VARCHAR NOT NULL);"
                        + "CREATE UNIQUE INDEX path_idx ON file (path);";
                stmt.executeUpdate(sql);

                sql = "CREATE TABLE mean_coverage "
                        + "(chunk_id INTEGER,"
                        + "file_id INTEGER,"
                        + "v1 INTEGER, "
                        + "v2 INTEGER, "
                        + "v3 INTEGER, "
                        + "v4 INTEGER, "
                        + "v5 INTEGER, "
                        + "v6 INTEGER, "
                        + "v7 INTEGER, "
                        + "v8 INTEGER,"
                        + "PRIMARY KEY(chunk_id, file_id));";
                stmt.executeUpdate(sql);

                // Insert all the chunks

                int multiple = MAJOR_CHUNK_SIZE / MINOR_CHUNK_SIZE;
                String minorChunkSuffix = (MINOR_CHUNK_SIZE / 1000) * 64 + "k";
                String majorChunkSuffix = (MAJOR_CHUNK_SIZE / 1000) * 64 + "k";

                PreparedStatement insertChunk = c.prepareStatement("insert into chunk (chunk_id, chromosome, start, end) "
                        + "values (?, ?, ?, ?)");

                for (SAMSequenceRecord samSequenceRecord : sequenceRecordList) {
                    String chromosome = samSequenceRecord.getSequenceName();
                    int sequenceLength = samSequenceRecord.getSequenceLength();

                    int cont = 0;
                    for (int i = 0; i < sequenceLength; i += 64 * MINOR_CHUNK_SIZE) {
                        String chunkId = chromosome + "_" + cont + "_" + minorChunkSuffix;
                        insertChunk.setString(1, chunkId);
                        insertChunk.setString(2, chromosome);
                        insertChunk.setInt(3, i + 1);
                        insertChunk.setInt(4, i + 64 * MINOR_CHUNK_SIZE);
                        insertChunk.execute();

                        if (cont % multiple == 0) {
                            chunkId = chromosome + "_" + cont / multiple + "_" + majorChunkSuffix;
                            insertChunk.setString(1, chunkId);
                            insertChunk.setString(2, chromosome);
                            insertChunk.setInt(3, i + 1);
                            insertChunk.setInt(4, i + 64 * MAJOR_CHUNK_SIZE);
                            insertChunk.execute();
                        }

                        cont++;
                    }

                }

                stmt.close();
                c.close();

            } catch (Exception e) {
                System.err.println(e.getClass().getName() + ": " + e.getMessage());
                System.exit(0);
            }
            System.out.println("Opened database successfully");
        }
    }

}
