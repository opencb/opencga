package org.opencb.opencga.storage.core.alignment.local;

import htsjdk.samtools.SAMSequenceRecord;
import org.opencb.commons.utils.FileUtils;

import java.io.BufferedReader;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Path;
import java.sql.*;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Created by jtarraga on 01/12/16.
 */
public class LocalAlignmentUtils {

    public static final String COVERAGE_WIG_SUFFIX = ".coverage.wig";
    public static final String COVERAGE_BIGWIG_SUFFIX = ".coverage.bw";
    public static final String COVERAGE_DATABASE_NAME = "coverage.db";

    public static final int CHUNK_SIZE = 1000;
    public static final int WINDOW_SIZE = 1000000;
    public static final int COVERAGE_REGION_SIZE = 100000;

    public static void initDatabase(List<SAMSequenceRecord> sequenceRecordList, Path workspace) {
        Path coverageDBPath = workspace.toAbsolutePath().resolve(LocalAlignmentUtils.COVERAGE_DATABASE_NAME);
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
                String minorChunkSuffix = (LocalAlignmentUtils.CHUNK_SIZE / 1000) * 64 + "k";

                PreparedStatement insertChunk = connection.prepareStatement("insert into chunk (chunk_id, chromosome, "
                        + "start, end) values (?, ?, ?, ?)");
                connection.setAutoCommit(false);

                for (SAMSequenceRecord samSequenceRecord : sequenceRecordList) {
                    String chromosome = samSequenceRecord.getSequenceName();
                    int sequenceLength = samSequenceRecord.getSequenceLength();

                    int cont = 0;
                    for (int i = 0; i < sequenceLength; i += 64 * LocalAlignmentUtils.CHUNK_SIZE) {
                        String chunkId = chromosome + "_" + cont + "_" + minorChunkSuffix;
                        insertChunk.setString(1, chunkId);
                        insertChunk.setString(2, chromosome);
                        insertChunk.setInt(3, i + 1);
                        insertChunk.setInt(4, i + 64 * LocalAlignmentUtils.CHUNK_SIZE);
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

    public static void insertCoverageDBFromWig(Path bamPath, Path workspace) throws IOException {
        FileUtils.checkFile(bamPath);
        String absoluteBamPath = bamPath.toFile().getAbsolutePath();
        Path coveragePath = workspace.toAbsolutePath().resolve(bamPath.getFileName()
                + LocalAlignmentUtils.COVERAGE_WIG_SUFFIX);

        String fileName = bamPath.toFile().getName();

        Path coverageDBPath = workspace.toAbsolutePath().resolve(LocalAlignmentUtils.COVERAGE_DATABASE_NAME);
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

                // Prepare statement to iterate file
                PreparedStatement insertCoverage = connection.prepareStatement("insert into mean_coverage (chunk_id, "
                        + " file_id, v1, v2, v3, v4, v5, v6, v7, v8) values (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)");
                connection.setAutoCommit(false);

                // main loop
                BufferedReader bufferedReader = FileUtils.newBufferedReader(coveragePath);
                String headerLine = bufferedReader.readLine();
                while (headerLine != null) {
                    checkHeaderLine(headerLine);
                    headerLine = processWigData(headerLine, fileId, chunkIdMap, insertCoverage, bufferedReader);
                }
                bufferedReader.close();
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

    private static String processWigData(String headerLine, int fileId, Map chunkIdMap,
                                         PreparedStatement insertCoverage, BufferedReader bufferedReader) {

        int counter1 = 0; // counter for 8-byte mean coverages array
        int counter2 = 0; // counter for 8-long packed coverages array
        byte[] meanCoverages = new byte[8]; // contains 8 coverages
        long[] packedCoverages = new long[8]; // contains 8 x 8 coverages

        String line = null;

        try {
            String chromosome = getHeaderInfo("chrom", headerLine);
            int start = Integer.parseInt(getHeaderInfo("start", headerLine));
            int span;
            try {
                span = Integer.parseInt(getHeaderInfo("span", headerLine));
            } catch (Exception e) {
                span = 1;
            }

            int chunk64 = (64 * LocalAlignmentUtils.CHUNK_SIZE);
            int chunkId, chunk = start / chunk64;

            line = bufferedReader.readLine();
            if (span <= LocalAlignmentUtils.CHUNK_SIZE) {
                int sum = 0;
                int counter = 0;

                // span is smaller than CHUNK_SIZE, we have to read n = CHUNK_SIZE/span lines
                // to get a coverage for the chunk
                int n = LocalAlignmentUtils.CHUNK_SIZE / span;
                while (line != null) {
                    if (isHeaderLine(line)) {
                        // we assume that's a chromosome change
                        // exit from the loop, and continue processing next...
                        break;
                    } else {
                        // accumulate the current value, and check if we have a complete value
                        sum += Integer.parseInt(line);
                        counter++;
                        if (counter == n) {
                            meanCoverages[counter1] = (byte) Math.min(sum / counter, 255);
                            if (++counter1 == 8) {
                                // packed mean coverages and save into the packed coverages array
                                packedCoverages[counter2] = bytesToLong(meanCoverages);
                                if (++counter2 == 8) {
                                    // write packed coverages array to DB

                                    chunkId = (int) chunkIdMap.get(chromosome + "_" + (chunk * chunk64 + 1));
                                    insertPackedCoverages(insertCoverage, chunkId, fileId, packedCoverages);

                                    // reset packed coverages array and counter2
                                    Arrays.fill(packedCoverages, 0);
                                    counter2 = 0;
                                    chunk++;
                                }
                                // reset mean coverages array and counter1
                                counter1 = 0;
                                Arrays.fill(meanCoverages, (byte) 0);
                            }
                            counter = 0;
                        }
                    }
                    line = bufferedReader.readLine();
                }
            } else {
                // span is bigger than CHUNK_SIZE,
                // for each line, we have n = span/CHUNK_SIZE coverages for the corresponding n chunks
                int n = span / LocalAlignmentUtils.CHUNK_SIZE;
                while (line != null) {
                    if (isHeaderLine(line)) {
                        // we assume that's a chromosome change
                        // exit from the loop, and continue processing next...
                        break;
                    } else {
                        for (int i = 0; i < n; i++) {
                            meanCoverages[counter1] = (byte) Math.min(Integer.parseInt(line), 255);
                            if (++counter1 == 8) {
                                // packed mean coverages and save into the packed coverages array
                                packedCoverages[counter2] = bytesToLong(meanCoverages);
                                if (++counter2 == 8) {
                                    // write packed coverages array to DB
                                    chunkId = (int) chunkIdMap.get(chromosome + "_" + (chunk * chunk64 + 1));
                                    insertPackedCoverages(insertCoverage, chunkId, fileId, packedCoverages);

                                    // reset packed coverages array and counter2
                                    Arrays.fill(packedCoverages, 0);
                                    counter2 = 0;
                                    chunk++;
                                }
                                // reset mean coverages array and counter1
                                counter1 = 0;
                                Arrays.fill(meanCoverages, (byte) 0);
                            }
                        }
                    }
                }
            }
            // populate DB with outstanding results
            if (counter1 > 0 || counter2 > 0) {
                packedCoverages[counter2] = bytesToLong(meanCoverages);
                chunkId = (int) chunkIdMap.get(chromosome + "_" + (chunk * chunk64 + 1));
                insertPackedCoverages(insertCoverage, chunkId, fileId, packedCoverages);
            }

            // insert batch to the DB
            insertCoverage.executeBatch();
        } catch (Exception e) {
            e.printStackTrace();
        }

        return line;
    }

    public static long bytesToLong(byte[] bytes) {
        ByteBuffer buffer = ByteBuffer.allocate(Long.BYTES);
        buffer.put(bytes);
        buffer.flip(); // need flip
        return buffer.getLong();
    }

    private static void insertPackedCoverages(PreparedStatement insertCoverage, int chunkId, int fileId,
                                              long[] packedCoverages) throws SQLException {
        assert(chunkId != -1);

        insertCoverage.setInt(1, chunkId);
        insertCoverage.setInt(2, fileId);
        for (int i = 0; i < 8; i++) {
            insertCoverage.setLong(i + 3, packedCoverages[i]);
        }
        insertCoverage.addBatch();
    }

    private static void checkHeaderLine(String headerLine) throws IOException {
        if (isHeaderLine(headerLine)) {
            if (isVariableStep(headerLine)) {
                throw new UnsupportedOperationException("VariableStep for coverage is not supported yet.");
            }
        } else {
            throw new IOException("Coverage wigfile does not start with a valid header line.");
        }
    }

    private static boolean isHeaderLine(String headerLine) {
        return (headerLine.startsWith("fixedStep") || headerLine.startsWith("variableStep"));
    }

    private static boolean isVariableStep(String headerLine) {
        return (headerLine.startsWith("variableStep"));
    }

    private static String getHeaderInfo(String name, String headerLine) {
        String[] fields = headerLine.split("[\t ]");
        for (String field : fields) {
            if (field.startsWith(name + "=")) {
                String[] subfields = field.split("=");
                return subfields[1];
            }
        }
        return null;
    }

}
