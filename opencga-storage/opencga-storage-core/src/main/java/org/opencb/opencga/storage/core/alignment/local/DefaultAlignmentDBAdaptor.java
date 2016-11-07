package org.opencb.opencga.storage.core.alignment.local;

import ga4gh.Reads;
import htsjdk.samtools.SAMFileHeader;
import htsjdk.samtools.SAMRecord;
import htsjdk.samtools.SAMSequenceRecord;
import org.apache.commons.lang3.time.StopWatch;
import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.map.ObjectWriter;
import org.ga4gh.models.ReadAlignment;
import org.opencb.biodata.models.alignment.RegionCoverage;
import org.opencb.biodata.models.core.Region;
import org.opencb.biodata.tools.alignment.AlignmentFilters;
import org.opencb.biodata.tools.alignment.AlignmentManager;
import org.opencb.biodata.tools.alignment.AlignmentOptions;
import org.opencb.biodata.tools.alignment.AlignmentUtils;
import org.opencb.biodata.tools.alignment.stats.AlignmentGlobalStats;
import org.opencb.commons.datastore.core.Query;
import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.commons.datastore.core.QueryResult;
import org.opencb.commons.utils.FileUtils;
import org.opencb.opencga.storage.core.alignment.AlignmentDBAdaptor;
import org.opencb.opencga.storage.core.alignment.iterators.AlignmentIterator;
import org.opencb.opencga.storage.core.alignment.iterators.ProtoAlignmentIterator;
import org.opencb.opencga.storage.core.alignment.iterators.SamRecordAlignmentIterator;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.PrintWriter;
import java.nio.ByteBuffer;
import java.nio.file.Path;
import java.sql.*;
import java.util.*;

/**
 * Created by pfurio on 26/10/16.
 */
public class DefaultAlignmentDBAdaptor implements AlignmentDBAdaptor {

    private static final String COVERAGE_SUFFIX = ".coverage";
    private static final String COVERAGE_DATABASE_NAME = "coverage.db";

    private static final int MINOR_CHUNK_SIZE = 1000;
    private static final int DEFAULT_CHUNK_SIZE = 1000;
    private static final int DEFAULT_WINDOW_SIZE = 1000000;

    private int chunkSize = DEFAULT_CHUNK_SIZE;

    public DefaultAlignmentDBAdaptor() {
        this(DEFAULT_CHUNK_SIZE);
    }

    public DefaultAlignmentDBAdaptor(int chunkSize) {
        this.chunkSize = chunkSize;
    }

    @Override
    public QueryResult getAllAlignmentsByRegion(List<Region> regions, QueryOptions options) {
        throw new UnsupportedOperationException();
    }

    @Override
    public QueryResult getAllAlignmentsByGene(String gene, QueryOptions options) {
        throw new UnsupportedOperationException();
    }

    @Override
    public QueryResult getCoverageByRegion(Region region, QueryOptions options) {
        throw new UnsupportedOperationException();
    }

    @Override
    public QueryResult getAlignmentsHistogramByRegion(Region region, boolean histogramLogarithm, int histogramMax) {
        throw new UnsupportedOperationException();
    }

    @Override
    public QueryResult getAllIntervalFrequencies(Region region, QueryOptions options) {
        throw new UnsupportedOperationException();
    }

    @Override
    public QueryResult getAlignmentRegionInfo(Region region, QueryOptions options) {
        throw new UnsupportedOperationException();
    }

    @Override
    public QueryResult<ReadAlignment> get(Path path, Query query, QueryOptions options) {
        try {
            StopWatch watch = new StopWatch();
            watch.start();

            FileUtils.checkFile(path);
            AlignmentManager alignmentManager = new AlignmentManager(path);

            AlignmentOptions alignmentOptions = parseQueryOptions(options);
            AlignmentFilters alignmentFilters = parseQuery(query);
            Region region = parseRegion(query);

            List<ReadAlignment> readAlignmentList;
            if (region != null) {
                readAlignmentList = alignmentManager.query(region, alignmentOptions, alignmentFilters, ReadAlignment.class);
            } else {
                readAlignmentList = alignmentManager.query(alignmentOptions, alignmentFilters, ReadAlignment.class);
            }
//            List<String> stringFormatList = new ArrayList<>(readAlignmentList.size());
//            for (Reads.ReadAlignment readAlignment : readAlignmentList) {
//                stringFormatList.add(readAlignment());
//            }
//            List<JsonFormat> list = alignmentManager.query(region, alignmentOptions, alignmentFilters, Reads.ReadAlignment.class);
            watch.stop();
            return new QueryResult("Get alignments", ((int) watch.getTime()), readAlignmentList.size(), readAlignmentList.size(),
                    null, null, readAlignmentList);
        } catch (Exception e) {
            e.printStackTrace();
            return new QueryResult<>();
        }
    }

    @Override
    public ProtoAlignmentIterator iterator(Path path) {
        return iterator(path, new Query(), new QueryOptions());
    }

    @Override
    public ProtoAlignmentIterator iterator(Path path, Query query, QueryOptions options) {
        return (ProtoAlignmentIterator) iterator(path, query, options, Reads.ReadAlignment.class);
    }

    @Override
    public <T> AlignmentIterator<T> iterator(Path path, Query query, QueryOptions options, Class<T> clazz) {
        try {
            FileUtils.checkFile(path);

            if (query == null) {
                query = new Query();
            }

            if (options == null) {
                options = new QueryOptions();
            }

            AlignmentManager alignmentManager = new AlignmentManager(path);
            AlignmentFilters alignmentFilters = parseQuery(query);
            AlignmentOptions alignmentOptions = parseQueryOptions(options);

            Region region = parseRegion(query);
            if (region != null) {
                if (Reads.ReadAlignment.class == clazz) {
                    return (AlignmentIterator<T>) new ProtoAlignmentIterator(alignmentManager.iterator(region, alignmentOptions,
                            alignmentFilters, Reads.ReadAlignment.class));
                } else if (SAMRecord.class == clazz) {
                    return (AlignmentIterator<T>) new SamRecordAlignmentIterator(alignmentManager.iterator(region, alignmentOptions,
                            alignmentFilters, SAMRecord.class));
                }
            } else {
                if (Reads.ReadAlignment.class == clazz) {
                    return (AlignmentIterator<T>) new ProtoAlignmentIterator(alignmentManager.iterator(alignmentOptions, alignmentFilters,
                            Reads.ReadAlignment.class));
                } else if (SAMRecord.class == clazz) {
                    return (AlignmentIterator<T>) new SamRecordAlignmentIterator(alignmentManager.iterator(alignmentOptions,
                            alignmentFilters, SAMRecord.class));
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
        return null;
    }

    @Override
    public QueryResult<Long> count(Path path, Query query, QueryOptions options) {
        StopWatch watch = new StopWatch();
        watch.start();

        ProtoAlignmentIterator iterator = iterator(path, query, options);
        long cont = 0;
        while (iterator.hasNext()) {
            iterator.next();
            cont++;
        }

        watch.stop();
        return new QueryResult<>("Get count", (int) watch.getTime(), 1, 1, "", "", Arrays.asList(cont));
    }

    @Override
    public QueryResult<AlignmentGlobalStats> stats(Path path, Path workspace) throws Exception {
        StopWatch watch = new StopWatch();
        watch.start();

        FileUtils.checkFile(path);
        FileUtils.checkDirectory(workspace);

        Path statsPath = workspace.resolve(path.getFileName() + ".stats");
        AlignmentGlobalStats alignmentGlobalStats;

        if (statsPath.toFile().exists()) {
            // Read the file of stats
            ObjectMapper objectMapper = new ObjectMapper();
            alignmentGlobalStats = objectMapper.readValue(statsPath.toFile(), AlignmentGlobalStats.class);
        } else {
            AlignmentManager alignmentManager = new AlignmentManager(path);
            alignmentGlobalStats = alignmentManager.stats();
            ObjectMapper objectMapper = new ObjectMapper();
            ObjectWriter objectWriter = objectMapper.typedWriter(AlignmentGlobalStats.class);
            objectWriter.writeValue(statsPath.toFile(), alignmentGlobalStats);
        }

        watch.stop();
        return new QueryResult<>("Get stats", (int) watch.getTime(), 1, 1, "", "", Arrays.asList(alignmentGlobalStats));
    }

    @Override
    public QueryResult<AlignmentGlobalStats> stats(Path path, Path workspace, Query query, QueryOptions options) throws Exception {
        StopWatch watch = new StopWatch();
        watch.start();

        if (options == null) {
            options = new QueryOptions();
        }
        if (query == null) {
            query = new Query();
        }

        if (options.size() == 0 && query.size() == 0) {
            return stats(path, workspace);
        }

        FileUtils.checkFile(path);

        AlignmentOptions alignmentOptions = parseQueryOptions(options);
        AlignmentFilters alignmentFilters = parseQuery(query);
        Region region = parseRegion(query);

        AlignmentManager alignmentManager = new AlignmentManager(path);
        AlignmentGlobalStats alignmentGlobalStats = alignmentManager.stats(region, alignmentOptions, alignmentFilters);

        watch.stop();
        return new QueryResult<>("Get stats", (int) watch.getTime(), 1, 1, "", "", Arrays.asList(alignmentGlobalStats));
    }

    @Override
    public QueryResult<RegionCoverage> coverage(Path path, Path workspace) throws Exception {
        QueryOptions options = new QueryOptions();
        options.put("windowSize", DEFAULT_WINDOW_SIZE);
        options.put("contained", false);
        return coverage(path, workspace, new Query(), options);
    }

    @Override
    public QueryResult<RegionCoverage> coverage(Path path, Path workspace, Query query, QueryOptions options) throws Exception {
        StopWatch watch = new StopWatch();
        watch.start();

        FileUtils.checkFile(path);

        if (query == null) {
            query = new Query();
        }

        if (options == null) {
            options = new QueryOptions();
        }

        AlignmentOptions alignmentOptions = parseQueryOptions(options);
        AlignmentFilters alignmentFilters = parseQuery(query);
        Region region = parseRegion(query);

        int windowSize;
        RegionCoverage coverage;
        if (region != null) {
            if (region.getEnd() - region.getStart() > 50 * MINOR_CHUNK_SIZE) {
                // if region is too big then we calculate the mean. We need to protect this code!
                // and query SQLite database
                windowSize = options.getInt("windowSize", DEFAULT_WINDOW_SIZE);
                coverage = meanCoverage(path, workspace, region, windowSize);
            } else {
                // if region is small enough we calculate all coverage for all positions dynamically
                // calling the biodata alignment manager
                AlignmentManager alignmentManager = new AlignmentManager(path);
                coverage = alignmentManager.coverage(region, alignmentOptions, alignmentFilters);
            }
        } else {
            // if no region is given we set up the windowSize to default value,
            // we should return a few thousands mean values
            // and query SQLite database
            windowSize = DEFAULT_WINDOW_SIZE;
            SAMFileHeader fileHeader = AlignmentUtils.getFileHeader(path);
            SAMSequenceRecord seq = fileHeader.getSequenceDictionary().getSequences().get(0);
            int arraySize = Math.min(50 * MINOR_CHUNK_SIZE, seq.getSequenceLength());
//            System.out.println("size = " + size);
//            System.out.println("seq = " + seq);
//            System.exit(0);

            region = new Region(seq.getSequenceName(), 1, arraySize * MINOR_CHUNK_SIZE);

            Path coverageDBPath = workspace.toAbsolutePath().resolve(COVERAGE_DATABASE_NAME);
            if (!coverageDBPath.toFile().exists()) {
                // Create coverage database
                calculateCoverage(path, workspace, fileHeader.getSequenceDictionary().getSequences());
            }
            coverage = meanCoverage(path, workspace, region, windowSize);
        }

        watch.stop();
        return new QueryResult("Region coverage", ((int) watch.getTime()), 1, 1, null, null, Arrays.asList(coverage));
    }


    private Region parseRegion(Query query) {
        Region region = null;
        String regionString = query.getString(QueryParams.REGION.key());
        if (regionString != null && !regionString.isEmpty()) {
            region = new Region(regionString);
        }
        return region;
    }

    private AlignmentFilters parseQuery(Query query) {
        AlignmentFilters alignmentFilters = AlignmentFilters.create();
        int minMapQ = query.getInt(QueryParams.MIN_MAPQ.key());
        if (minMapQ > 0) {
            alignmentFilters.addMappingQualityFilter(minMapQ);
        }
        return  alignmentFilters;
    }

    private AlignmentOptions parseQueryOptions(QueryOptions options) {
        AlignmentOptions alignmentOptions = new AlignmentOptions()
                .setContained(options.getBoolean(QueryParams.CONTAINED.key()));
        int limit = options.getInt(QueryParams.LIMIT.key());
        if (limit > 0) {
            alignmentOptions.setLimit(limit);
        }
        return alignmentOptions;
    }

    public int getChunkSize() {
        return chunkSize;
    }

    public DefaultAlignmentDBAdaptor setChunkSize(int chunkSize) {
        this.chunkSize = chunkSize;
        return this;
    }

    private RegionCoverage meanCoverage(Path bamPath, Path workspace, Region region, int windowSize) {
        windowSize = Math.max(windowSize / MINOR_CHUNK_SIZE * MINOR_CHUNK_SIZE, MINOR_CHUNK_SIZE);
        int size = ((region.getEnd() - region.getStart() + 1) / windowSize) + 1;
        short[] values = new short[size];

        String absoluteBamPath = bamPath.toFile().getAbsolutePath();
        Path coverageDBPath = workspace.toAbsolutePath().resolve(COVERAGE_DATABASE_NAME);

        try {
            Class.forName("org.sqlite.JDBC");
            Connection connection = DriverManager.getConnection("jdbc:sqlite:" + coverageDBPath);
            Statement stmt = connection.createStatement();

            String sql = "SELECT id FROM file where path = '" + absoluteBamPath + "';";
            ResultSet rs = stmt.executeQuery(sql);
            int fileId = -1;
            while (rs.next()) {
                fileId = rs.getInt("id");
                break;
            }

            // sanity check
            if (fileId == -1) {
                throw new SQLException("Internal error: file " + absoluteBamPath + " not found in the coverage DB");
            }

            sql = "SELECT c.start, c.end, mc.v1, mc.v2, mc.v3, mc.v4, mc.v5, mc.v6, mc.v7, mc.v8"
                    + " FROM chunk c, mean_coverage mc"
                    + " WHERE c.id = mc.chunk_id AND mc.file_id = " + fileId
                    + " AND c.chromosome = '" + region.getChromosome() + "' AND c.start <= " + region.getEnd()
                    + " AND c.end > " + region.getStart() + " ORDER by c.start ASC;";
            rs = stmt.executeQuery(sql);

            int chunksPerWindow = windowSize / MINOR_CHUNK_SIZE;
            int chunkCounter = 0;
            int coverageAccumulator = 0;
            int arrayPos = 0;

            int start = 0;
            long packedCoverages;
            byte[] meanCoverages;

            boolean first = true;
            while (rs.next()) {
                if (first) {
                    start = rs.getInt("start");
                    first = false;
                }
                for (int i = 0; i < 8; i++) {
                    packedCoverages = rs.getInt("v" + (i + 1));
                    meanCoverages = longToBytes(packedCoverages);
                    for (int j = 0; j < 8; j++) {
                        if (start <= region.getEnd() && (start + MINOR_CHUNK_SIZE) >= region.getStart()) {
                            // A byte is always signed in Java,
                            // we can get its unsigned value by binary-anding it with 0xFF
                            coverageAccumulator += (meanCoverages[j] & 0xFF);
                            if (++chunkCounter >= chunksPerWindow) {
                                values[arrayPos++] = (short) Math.round(1.0f * coverageAccumulator / chunkCounter);
                                coverageAccumulator = 0;
                                chunkCounter = 0;
                            }
                        }
                        start += MINOR_CHUNK_SIZE;
                    }
                }
            }
            if (chunkCounter > 0) {
                values[arrayPos++] = (short) Math.round(1.0f * coverageAccumulator / chunkCounter);
            }
        } catch (Exception e) {
            e.printStackTrace();
        }

        return new RegionCoverage(region, windowSize, values);
    }

    private byte[] longToBytes(long x) {
        ByteBuffer buffer = ByteBuffer.allocate(Long.BYTES);
        buffer.putLong(x);
        return buffer.array();
    }

    private void calculateCoverage(Path filePath, Path workspace, List<SAMSequenceRecord> sequenceRecordList) throws IOException {
        // 3. Calculate coverage and store in SQLite
        initDatabase(sequenceRecordList, workspace);
        //        System.out.println("SQLite database initialization, in " + ((System.currentTimeMillis() - start) / 1000.0f)
        //                + " s.");

        Path coveragePath = workspace.toAbsolutePath().resolve(filePath.getFileName() + COVERAGE_SUFFIX);

        AlignmentOptions options = new AlignmentOptions();
        options.setContained(false);

        AlignmentManager alignmentManager = new AlignmentManager(filePath);
        Iterator<SAMSequenceRecord> iterator = sequenceRecordList.iterator();
        PrintWriter writer = new PrintWriter(coveragePath.toFile());
        StringBuilder line;
        //        start = System.currentTimeMillis();
        while (iterator.hasNext()) {
            SAMSequenceRecord next = iterator.next();
            for (int i = 0; i < next.getSequenceLength(); i += MINOR_CHUNK_SIZE) {
                Region region = new Region(next.getSequenceName(), i + 1,
                        Math.min(i + MINOR_CHUNK_SIZE, next.getSequenceLength()));
                RegionCoverage regionCoverage = alignmentManager.coverage(region, options, null);
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
        insertCoverageDB(filePath, workspace);
        //        System.out.println("SQLite database population, in " + ((System.currentTimeMillis() - start) / 1000.0f) + " s.");
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

        Path coverageDBPath = workspace.toAbsolutePath().resolve(COVERAGE_DATABASE_NAME);
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

    private void insertPackedCoverages(PreparedStatement insertCoverage, int chunkId, int fileId, long[] packedCoverages)
            throws SQLException {
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
