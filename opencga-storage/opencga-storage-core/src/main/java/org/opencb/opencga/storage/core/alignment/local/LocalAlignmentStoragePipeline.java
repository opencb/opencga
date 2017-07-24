/*
 * Copyright 2015-2017 OpenCB
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

package org.opencb.opencga.storage.core.alignment.local;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectWriter;
import htsjdk.samtools.SAMFileHeader;
import htsjdk.samtools.SAMSequenceRecord;
import org.opencb.biodata.formats.io.FileFormatException;
import org.opencb.biodata.models.alignment.RegionCoverage;
import org.opencb.biodata.models.core.Region;
import org.opencb.biodata.tools.alignment.AlignmentOptions;
import org.opencb.biodata.tools.alignment.BamManager;
import org.opencb.biodata.tools.alignment.BamUtils;
import org.opencb.biodata.tools.alignment.stats.AlignmentGlobalStats;
import org.opencb.commons.utils.FileUtils;
import org.opencb.opencga.storage.core.StoragePipeline;
import org.opencb.opencga.storage.core.exceptions.StorageEngineException;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.PrintWriter;
import java.net.URI;
import java.nio.ByteBuffer;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.sql.*;
import java.util.*;

/**
 * Created by pfurio on 31/10/16.
 */
public class LocalAlignmentStoragePipeline implements StoragePipeline {

    private static final String COVERAGE_SUFFIX = ".coverage";
    private static final String COVERAGE_DATABASE_NAME = "coverage.db";

    private static final int MINOR_CHUNK_SIZE = 1000;

    public LocalAlignmentStoragePipeline() {
        super();
    }

    @Override
    public URI extract(URI input, URI ouput) throws StorageEngineException {
        return input;
    }

    @Override
    public URI preTransform(URI input) throws IOException, FileFormatException, StorageEngineException {
        // Check if a BAM file is passed and it is sorted.
        // Only binaries and sorted BAM files are accepted at this point.
        Path inputPath = Paths.get(input.getRawPath());
        BamUtils.checkBamOrCramFile(new FileInputStream(inputPath.toFile()), inputPath.getFileName().toString(), true);
        return input;
    }

    @Override
    public URI transform(URI input, URI pedigree, URI output) throws Exception {
        Path path = Paths.get(input.getRawPath());
        FileUtils.checkFile(path);

        Path workspace = Paths.get(output.getRawPath());
        FileUtils.checkDirectory(workspace);

        // 1. Check if the bai does not exist and create it
        BamManager bamManager = new BamManager(path);
        if (!path.getParent().resolve(path.getFileName().toString() + ".bai").toFile().exists()) {
            bamManager.createIndex();
        }

        // 2. Calculate stats and store in a file
        Path statsPath = workspace.resolve(path.getFileName() + ".stats");
        if (!statsPath.toFile().exists()) {
            AlignmentGlobalStats stats = bamManager.stats();
            ObjectMapper objectMapper = new ObjectMapper();
            ObjectWriter objectWriter = objectMapper.writerFor(AlignmentGlobalStats.class);
            objectWriter.writeValue(statsPath.toFile(), stats);
        }

        // 3. Calculate coverage and store in SQLite
        SAMFileHeader fileHeader = BamUtils.getFileHeader(path);
//        long start = System.currentTimeMillis();
        initDatabase(fileHeader.getSequenceDictionary().getSequences(), workspace);
//        System.out.println("SQLite database initialization, in " + ((System.currentTimeMillis() - start) / 1000.0f)
//                + " s.");

        Path coveragePath = workspace.toAbsolutePath().resolve(path.getFileName() + COVERAGE_SUFFIX);

        AlignmentOptions options = new AlignmentOptions();
        options.setContained(false);

        Iterator<SAMSequenceRecord> iterator = fileHeader.getSequenceDictionary().getSequences().iterator();
        PrintWriter writer = new PrintWriter(coveragePath.toFile());
        StringBuilder line;
//        start = System.currentTimeMillis();
        while (iterator.hasNext()) {
            SAMSequenceRecord next = iterator.next();
            for (int i = 0; i < next.getSequenceLength(); i += MINOR_CHUNK_SIZE) {
                Region region = new Region(next.getSequenceName(), i + 1,
                        Math.min(i + MINOR_CHUNK_SIZE, next.getSequenceLength()));
                RegionCoverage regionCoverage = bamManager.coverage(region, null, options);
                int meanDepth = Math.min(regionCoverage.meanCoverage(), 255);

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
                writer.println(line.toString());
            }
        }
        writer.close();
//        System.out.println("Mean coverage file creation, in " + ((System.currentTimeMillis() - start) / 1000.0f) + " s.");

        // save file to db
//        start = System.currentTimeMillis();
        insertCoverageDB(path, workspace);
//        System.out.println("SQLite database population, in " + ((System.currentTimeMillis() - start) / 1000.0f) + " s.");

        return input;
    }

    @Override
    public URI postTransform(URI input) throws Exception {
        return input;
    }

    @Override
    public URI preLoad(URI input, URI output) throws IOException, StorageEngineException {
        return null;
    }

    @Override
    public URI load(URI input) throws IOException, StorageEngineException {
        return null;
    }

    @Override
    public URI postLoad(URI input, URI output) throws IOException, StorageEngineException {
        return null;
    }

    private void initDatabase(List<SAMSequenceRecord> sequenceRecordList, Path workspace) {
        Path coverageDBPath = workspace.toAbsolutePath().resolve(COVERAGE_DATABASE_NAME);
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
                        + "CREATE UNIQUE INDEX chunk_id_idx ON chunk (chunk_id);"
                        + "CREATE INDEX chrom_start_end_idx ON chunk (chromosome, start, end);";
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
                String minorChunkSuffix = (MINOR_CHUNK_SIZE / 1000) * 64 + "k";

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

    private void insertCoverageDB(Path bamPath, Path workspace) throws IOException {
        FileUtils.checkFile(bamPath);
        String absoluteBamPath = bamPath.toFile().getAbsolutePath();
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

            if (fileId != -1) {
                Map chunkIdMap = new HashMap<String, Integer>();
                String sql = "SELECT id, chromosome, start FROM chunk";
//                        System.out.println(sql);
                rs = stmt.executeQuery(sql);
                while (rs.next()) {
                    chunkIdMap.put(rs.getString("chromosome") + "_" + rs.getInt("start"), rs.getInt("id"));
                }

                // Iterate file
                PreparedStatement insertCoverage = connection.prepareStatement("insert into mean_coverage (chunk_id, "
                        + " file_id, v1, v2, v3, v4, v5, v6, v7, v8) values (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)");
                connection.setAutoCommit(false);

                BufferedReader bufferedReader = FileUtils.newBufferedReader(coveragePath);
                // Checkstyle plugin is not happy with assignations inside while/for
                int chunkId = -1;

                byte[] meanCoverages = new byte[8]; // contains 8 coverages
                long[] packedCoverages = new long[8]; // contains 8 x 8 coverages
                int counter1 = 0; // counter for 8-byte mean coverages array
                int counter2 = 0; // counter for 8-long packed coverages array
                String prevChromosome = null;

                String line = bufferedReader.readLine();
                while (line != null) {
                    String[] fields = line.split("\t");

                    if (prevChromosome == null) {
                        prevChromosome = fields[1];
                        System.out.println("Processing chromosome " + prevChromosome + "...");
                    } else if (!prevChromosome.equals(fields[1])) {
                        // we have to write the current results into the DB
                        if (counter1 > 0 || counter2 > 0) {
                            packedCoverages[counter2] = bytesToLong(meanCoverages);
                            insertPackedCoverages(insertCoverage, chunkId, fileId, packedCoverages);
                        }
                        prevChromosome = fields[1];
                        System.out.println("Processing chromosome " + prevChromosome + "...");

                        // reset arrays, counters,...
                        Arrays.fill(meanCoverages, (byte) 0);
                        Arrays.fill(packedCoverages, 0);
                        counter2 = 0;
                        counter1 = 0;
                        chunkId = -1;
                    }
                    if (chunkId == -1) {
                        String key = fields[1] + "_" + fields[2];
                        if (chunkIdMap.containsKey(key)) {
                            chunkId = (int) chunkIdMap.get(key);
                        } else {
                            throw new SQLException("Internal error: coverage chunk " + fields[1]
                                    + ":" + fields[2] + "-, not found in database");
                        }
                    }
                    meanCoverages[counter1] = (byte) Integer.parseInt(fields[4]);
                    if (++counter1 == 8) {
                        // packed mean coverages and save into the packed coverages array
                        packedCoverages[counter2] = bytesToLong(meanCoverages);
                        if (++counter2 == 8) {
                            // write packed coverages array to DB
                            insertPackedCoverages(insertCoverage, chunkId, fileId, packedCoverages);

                            // reset packed coverages array and counter2
                            Arrays.fill(packedCoverages, 0);
                            counter2 = 0;
                            chunkId = -1;
                        }
                        // reset mean coverages array and counter1
                        counter1 = 0;
                        Arrays.fill(meanCoverages, (byte) 0);
                    }

                    line = bufferedReader.readLine();
                }
                bufferedReader.close();

                if (counter1 > 0 || counter2 > 0) {
                    packedCoverages[counter2] = bytesToLong(meanCoverages);
                    insertPackedCoverages(insertCoverage, chunkId, fileId, packedCoverages);
                }

                // insert batch to the DB
                insertCoverage.executeBatch();
            }
            connection.commit();
            stmt.close();
            connection.close();
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    private void insertPackedCoverages(PreparedStatement insertCoverage, int chunkId, int fileId,
                                       long[] packedCoverages) throws SQLException {
        assert(chunkId != -1);

        insertCoverage.setInt(1, chunkId);
        insertCoverage.setInt(2, fileId);
        for (int i = 0; i < 8; i++) {
            insertCoverage.setLong(i + 3, packedCoverages[i]);
        }
        insertCoverage.addBatch();
    }

    private long bytesToLong(byte[] bytes) {
        ByteBuffer buffer = ByteBuffer.allocate(Long.BYTES);
        buffer.put(bytes);
        buffer.flip(); // need flip
        return buffer.getLong();
    }
}
