package org.opencb.opencga.storage.core.alignment.local;

import htsjdk.samtools.SAMFileHeader;
import htsjdk.samtools.SAMSequenceRecord;
import org.opencb.biodata.formats.io.FileFormatException;
import org.opencb.biodata.models.core.Region;
import org.opencb.biodata.tools.alignment.AlignmentManager;
import org.opencb.biodata.tools.alignment.AlignmentUtils;
import org.opencb.commons.utils.FileUtils;
import org.opencb.opencga.storage.core.alignment.AlignmentDBAdaptor;
import org.opencb.opencga.storage.core.alignment.AlignmentStorageETL;
import org.opencb.opencga.storage.core.exceptions.StorageManagerException;
import scala.collection.mutable.StringBuilder;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.PrintWriter;
import java.net.URI;
import java.nio.ByteBuffer;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.sql.*;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

/**
 * Created by pfurio on 31/10/16.
 */
public class DefaultAlignmentStorageETL extends AlignmentStorageETL {

    private Path workspace;

    private static final int MINOR_CHUNK_SIZE = 1000;
    private static final int MAJOR_CHUNK_SIZE = MINOR_CHUNK_SIZE * 10;
    private static final String COVERAGE_SUFFIX = ".coverage";

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

//        int fileId = insertFileDB(path.getFileName());

        Path coveragePath = workspace.toAbsolutePath().resolve(path.getFileName() + COVERAGE_SUFFIX);
        //Path coveragePath = path.getParent().resolve(path.getFileName() + "." + MINOR_CHUNK_SIZE + COVERAGE_SUFFIX);
        System.out.println("coveragePath = " + coveragePath);

        Iterator<SAMSequenceRecord> iterator = fileHeader.getSequenceDictionary().getSequences().iterator();
        PrintWriter writer = new PrintWriter(coveragePath.toFile());
        StringBuilder line;
        while (iterator.hasNext()) {
            SAMSequenceRecord next = iterator.next();
            for (int i = 0; i < next.getSequenceLength(); i += MINOR_CHUNK_SIZE) {
                Region region = new Region(next.getSequenceName(), i + 1,
                        Math.min(i + MINOR_CHUNK_SIZE, next.getSequenceLength()));
                //RegionDepth regionDepth = alignmentManager.depth(region, null, null);
                int meanDepth = 30; //regionDepth.meanDepth();

                // File columns: chunk   chromosome start   end coverage
                // chunk format: chrom_id_suffix, where:
                //      id: int value starting at 0
                //      suffix: chunkSize + k
                // eg. 3_4_1k

                line = new StringBuilder();
                line.append(region.getChromosome()).append("_");
                line.append(i / MINOR_CHUNK_SIZE).append("_").append(MINOR_CHUNK_SIZE / 1000).append("k");
                line.append("\t").append(region.getChromosome());
                line.append("\t").append(region.getStart());
                line.append("\t").append(region.getEnd());
                line.append("\t").append(meanDepth);
//                System.out.println(line.toString());
                writer.println(line.toString());
            }
//          System.out.println(iterator.next().);
        }
        writer.close();

        // save file to db
        insertCoverageDB(path);

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
            Statement stmt;
            try {
                Class.forName("org.sqlite.JDBC");
                Connection connection = DriverManager.getConnection("jdbc:sqlite:" + coverageDBPath);

                // Create tables
                stmt = connection.createStatement();
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

//                int multiple = MAJOR_CHUNK_SIZE / MINOR_CHUNK_SIZE;
                String minorChunkSuffix = (MINOR_CHUNK_SIZE / 1000) * 64 + "k";
//                String majorChunkSuffix = (MAJOR_CHUNK_SIZE / 1000) * 64 + "k";

                PreparedStatement insertChunk = connection.prepareStatement("insert into chunk (chunk_id, chromosome, start, end) "
                        + "values (?, ?, ?, ?)");
                connection.setAutoCommit(false);

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
                        insertChunk.addBatch();
//                        insertChunk.execute();

//                        if (cont % multiple == 0) {
//                            chunkId = chromosome + "_" + cont / multiple + "_" + majorChunkSuffix;
//                            insertChunk.setString(1, chunkId);
//                            insertChunk.setString(2, chromosome);
//                            insertChunk.setInt(3, i + 1);
//                            insertChunk.setInt(4, i + 64 * MAJOR_CHUNK_SIZE);
////                            insertChunk.execute();
//                            insertChunk.addBatch();
//                        }
                        cont++;
                    }
                    insertChunk.executeBatch();
                }

                connection.commit();
                stmt.close();
                connection.close();
            } catch (Exception e) {
                System.err.println(e.getClass().getName() + ": " + e.getMessage());
                System.exit(0);
            }
            System.out.println("Opened database successfully");
        }
    }

    private void insertCoverageDB(Path bamPath) throws IOException {
        FileUtils.checkFile(bamPath);
        String absoluteBamPath = bamPath.toFile().getAbsolutePath();
//        Path coveragePath = Paths.get(absoluteBamPath + COVERAGE_SUFFIX);
//        FileUtils.checkFile(coveragePath);
        Path coveragePath = workspace.toAbsolutePath().resolve(bamPath.getFileName() + COVERAGE_SUFFIX);

        String fileName = bamPath.toFile().getName();

        Path coverageDBPath = workspace.toAbsolutePath().resolve("coverage.db");
        try {
            // Insert into file table
            Class.forName("org.sqlite.JDBC");
            Connection connection = DriverManager.getConnection("jdbc:sqlite:" + coverageDBPath);
            Statement stmt = connection.createStatement();
            String insertFileSql = "insert into file (path, name) values ('" + absoluteBamPath + "', '" + fileName + "');";
            stmt.executeUpdate(insertFileSql);
            stmt.close();

            ResultSet rs = stmt.executeQuery("SELECT id FROM file where path = '" + absoluteBamPath + "';");
            int fileId = -1;
            while (rs.next()) {
                fileId = rs.getInt("id");
            }

            // Iterate file
            System.out.println("fileId = " + fileId);
            if (fileId != -1) {
                BufferedReader bufferedReader = FileUtils.newBufferedReader(coveragePath);
                // Checkstyle plugin is not happy with assignations inside while/for
                int chunkId = -1;

                byte[] meanCoverages = new byte[8]; // contains 8 coverages
                long[] packedCoverages = new long[8]; // contains 8 x 8 coverages
                int counter1 = 0; // counter for 8-byte mean coverages array
                int counter2 = 0; // counter for 8-long packed coverages array

                String line = bufferedReader.readLine();
                while (line != null) {
                    String[] fields = line.split("\t");

                    if (chunkId == -1) {
                        rs = stmt.executeQuery("SELECT id FROM chunk where path = '" + absoluteBamPath + "';");
                        while (rs.next()) {
                            chunkId = rs.getInt("id");
                        }
                    }
                    meanCoverages[counter1] = Byte.parseByte(fields[4]);
                    if (++counter1 == 8) {
                        // packed mean coverages and save into the packed coverages array
                        packedCoverages[counter2] = bytesToLong(meanCoverages);
                        if (++counter2 == 8) {
                            // write packed coverages array to DB

                            // init packed coverages array and counter2
                            Arrays.fill(packedCoverages, 0);
                            counter2 = 0;
                            chunkId = -1;
                        }
                        // init current packed coverage and counter1
                        counter1 = 0;
                        Arrays.fill(meanCoverages, (byte) 0);
                    }

                    line = bufferedReader.readLine();
                }
                bufferedReader.close();
            }

            connection.close();
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    private long bytesToLong(byte[] bytes) {
        ByteBuffer buffer = ByteBuffer.allocate(Long.BYTES);
        buffer.put(bytes);
        buffer.flip(); // need flip
        return buffer.getLong();
    }

    public byte[] longToBytes(long x) {
        ByteBuffer buffer = ByteBuffer.allocate(Long.BYTES);
        buffer.putLong(x);
        return buffer.array();
    }
}
