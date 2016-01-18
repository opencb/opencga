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

package org.opencb.opencga.storage.mongodb.alignment;

import com.mongodb.BasicDBObject;
import com.mongodb.BasicDBObjectBuilder;
import com.mongodb.DBObject;
import htsjdk.samtools.SAMFileReader;
import htsjdk.samtools.SAMRecord;
import htsjdk.samtools.SAMRecordIterator;
import org.opencb.biodata.formats.alignment.AlignmentConverter;
import org.opencb.biodata.formats.sequence.fasta.dbadaptor.CellBaseSequenceDBAdaptor;
import org.opencb.biodata.formats.sequence.fasta.dbadaptor.SequenceDBAdaptor;
import org.opencb.biodata.models.alignment.Alignment;
import org.opencb.biodata.models.alignment.AlignmentRegion;
import org.opencb.biodata.models.alignment.stats.MeanCoverage;
import org.opencb.biodata.models.alignment.stats.RegionCoverage;
import org.opencb.biodata.models.core.Region;
import org.opencb.datastore.core.ComplexTypeConverter;
import org.opencb.datastore.core.QueryOptions;
import org.opencb.datastore.core.QueryResult;
import org.opencb.datastore.mongodb.MongoDBCollection;
import org.opencb.datastore.mongodb.MongoDataStore;
import org.opencb.datastore.mongodb.MongoDataStoreManager;
import org.opencb.opencga.core.common.IOUtils;
import org.opencb.opencga.storage.core.alignment.adaptors.AlignmentDBAdaptor;
import org.opencb.opencga.storage.core.alignment.tasks.AlignmentRegionCoverageCalculatorTask;
import org.opencb.opencga.storage.mongodb.utils.MongoCredentials;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.*;

/**
 * Date 15/08/14.
 *
 * @author Jacobo Coll Moragon <jcoll@ebi.ac.uk>
 */
public class IndexedAlignmentDBAdaptor implements AlignmentDBAdaptor {

    public static final String FEATURES_COUNT = "features_count";
    protected static org.slf4j.Logger logger = LoggerFactory.getLogger(IndexedAlignmentDBAdaptor.class);

    private AlignmentConverter converter;
    private static MongoDataStoreManager mongoManager = null;
    private MongoDataStore mongoDataStore;
    private MongoCredentials credentials;


    public IndexedAlignmentDBAdaptor(SequenceDBAdaptor adaptor, MongoCredentials credentials) {
        try {
            this.converter = new AlignmentConverter(adaptor);
        } catch (IOException e) {
            e.printStackTrace();
        }

        this.credentials = credentials;
        if (mongoManager == null) {
            mongoManager = new MongoDataStoreManager(credentials.getDataStoreServerAddresses());
        }
        mongoDataStore = mongoManager.get(credentials.getMongoDbName(), credentials.getMongoDBConfiguration());
    }

    public IndexedAlignmentDBAdaptor(MongoCredentials credentials) {
        this(new CellBaseSequenceDBAdaptor(), credentials);
    }

    /**
     * @param regions Query Region
     * @param options Query Options: Expected bam_path. Optionally bai_path and view_as_pairs
     * @return
     */
    @Override
    public QueryResult getAllAlignmentsByRegion(List<Region> regions, QueryOptions options) {


        String fileId = options.getString(QO_FILE_ID, "");
        String bam = options.getString(QO_BAM_PATH, "");
        String bai = options.getString(QO_BAI_PATH, "");
        boolean includeCoverage = options.getBoolean(QO_INCLUDE_COVERAGE, true);
        if (bai.equals("")) {
            bai = getIndexFromBam(bam);
        }
        Path bamFile = Paths.get(bam);
        Path baiFile = Paths.get(bai);

        QueryResult<AlignmentRegion> queryResult = new QueryResult<>(fileId);

        long startTime = System.currentTimeMillis();


        if (!bam.endsWith("")) {
            queryResult.setErrorMsg("Expected parameter \"" + QO_BAM_PATH + "=*.bam\"");
            logger.warn("Expected parameter \"" + QO_BAM_PATH + "=*.bam\"");
        }
        if (!bam.endsWith(".bam")) {
            queryResult.setErrorMsg("Unsupported extension for \"" + QO_BAM_PATH + "=" + bam + "\"");
            logger.warn("Unsupported extension for \"" + QO_BAM_PATH + "=" + bam + "\"");
        } else if (!bamFile.toFile().exists()) {
            queryResult.setErrorMsg("BAM file '" + bam + "' not found");
            logger.warn("BAM file " + bamFile + " not found");
        } else if (!bai.endsWith(".bai")) {
            queryResult.setErrorMsg("Can't find BAM index file. Expected parameter \"" + QO_BAI_PATH + "=*.bai\"");
            logger.warn("Can't find BAM index file. Expected parameter \"" + QO_BAI_PATH + "=*.bai\".");
        } else if (!baiFile.toFile().exists()) {
            queryResult.setErrorMsg("BAM index file (.bai) not found");
            logger.warn("BAM index file (.bai) " + baiFile + " for file " + bamFile + " not found");
        } else {
            List<AlignmentRegion> results = new LinkedList<>();
            for (Region region : regions) {

                AlignmentRegion filteredAlignmentRegion;

                List<SAMRecord> recordList = getSamRecordsByRegion(bamFile, baiFile, region);
                List<Alignment> alignmentList = getAlignmentsFromSamRecords(recordList, options);
                List<Alignment> alignmentsInRegion = getAlignmentsInRegion(alignmentList, region);

                RegionCoverage regionCoverage = null;
                if (includeCoverage) {
                    AlignmentRegion alignmentRegion;
                    if (!alignmentList.isEmpty()) {
                        alignmentRegion = new AlignmentRegion(alignmentList, null);
                        alignmentRegion.setEnd(region.getEnd());
                    } else {
                        alignmentRegion = new AlignmentRegion(region.getChromosome(), region.getStart(), region.getEnd());
                        alignmentRegion.setAlignments(new LinkedList<>());
                    }
                    regionCoverage = calculateCoverageByRegion(alignmentRegion, region);
                }

                filteredAlignmentRegion = new AlignmentRegion(
                        region.getChromosome(), region.getStart(), region.getEnd(), alignmentsInRegion, regionCoverage, null);

                results.add(filteredAlignmentRegion);
            }

            queryResult.setResult(results);
        }

        queryResult.setTime((int) (System.currentTimeMillis() - startTime));
        return queryResult;
    }

    @Override
    public QueryResult getAllAlignmentsByGene(String gene, QueryOptions options) {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public QueryResult getCoverageByRegion(Region region, QueryOptions options) {
        QueryResult<RegionCoverage> queryResult = new QueryResult<>(
                String.format("%s:%d-%d", region.getChromosome(), region.getStart(), region.getEnd()));
        long startTime = System.currentTimeMillis();

        options.put(QO_PROCESS_DIFFERENCES, false);
        options.put(QO_INCLUDE_COVERAGE, true);

        QueryResult alignmentsResult = this.getAllAlignmentsByRegion(Arrays.asList(region), options);
        if (alignmentsResult.getResultType().equals(AlignmentRegion.class.getCanonicalName())) {
//            AlignmentRegion alignmentRegion = new AlignmentRegion(alignmentsResult.getResult(), null);
//
//            RegionCoverage regionCoverage = calculateCoverageByRegion(alignmentRegion, region);

            RegionCoverage regionCoverage = ((AlignmentRegion) alignmentsResult.getResult().get(0)).getCoverage();

            queryResult.setResult(Arrays.asList(regionCoverage));
            queryResult.setNumResults(1);
        } else {
            queryResult.setErrorMsg(alignmentsResult.getErrorMsg());    //ERROR
            logger.warn(alignmentsResult.getErrorMsg());
        }
        queryResult.setTime((int) (System.currentTimeMillis() - startTime));
        return queryResult;
    }

    @Override
    public QueryResult getAllIntervalFrequencies(Region region, QueryOptions options) {
        int size = options.getInt(QO_INTERVAL_SIZE, 2000);
        String fileId = options.getString(QO_FILE_ID);
        int chunkSize = options.getInt(QO_COVERAGE_CHUNK_SIZE, 200);

        if (size % chunkSize != 0) {
            size -= size % chunkSize;
        }

        List<DBObject> operations = new LinkedList<>();
        operations.add(new BasicDBObject(
                "$match",
                new BasicDBObject(
                        "$and",
                        Arrays.asList(
                                new BasicDBObject(CoverageMongoDBWriter.START_FIELD, new BasicDBObject("$gt", region.getStart())),
                                new BasicDBObject(CoverageMongoDBWriter.START_FIELD, new BasicDBObject("$lt", region.getEnd())),
                                new BasicDBObject(CoverageMongoDBWriter.CHR_FIELD, region.getChromosome()),
                                new BasicDBObject(CoverageMongoDBWriter.SIZE_FIELD, chunkSize)
                        )
                )
        ));
        operations.add(new BasicDBObject(
                "$unwind",
                "$" + CoverageMongoDBWriter.FILES_FIELD
        ));
        operations.add(new BasicDBObject(
                "$match",
                new BasicDBObject(CoverageMongoDBWriter.FILES_FIELD + "." + CoverageMongoDBWriter.FILE_ID_FIELD, fileId)
        ));
        String startField = "$" + CoverageMongoDBWriter.START_FIELD;
        String averageField = "$" + CoverageMongoDBWriter.FILES_FIELD + "." + CoverageMongoDBWriter.AVERAGE_FIELD;
        operations.add(new BasicDBObject(
                "$group", BasicDBObjectBuilder.start(
                "_id", new BasicDBObject(
                        "$divide", Arrays.asList(
                        new BasicDBObject(
                                "$subtract", Arrays.asList(
                                startField,
                                new BasicDBObject(
                                        "$mod", Arrays.asList(startField, size)
                                )
                        )
                        ),
                        size
                )
                )
        )
                .append(
                        FEATURES_COUNT, new BasicDBObject(
                                "$sum",
                                averageField
//                                new BasicDBObject(
//                                        "$divide",
//                                        Arrays.asList(
//                                                averageField,
//                                                size / chunkSize
//                                        )
//                                )
                        )
                ).get()
        ));
        operations.add(new BasicDBObject(
                "$sort",
                new BasicDBObject("_id", 1)
        ));
        StringBuilder mongoAggregate = new StringBuilder("db.").append(CoverageMongoDBWriter.COVERAGE_COLLECTION_NAME + ".aggregate( [");
        for (DBObject operation : operations) {
            mongoAggregate.append(operation.toString()).append(" , ");
        }
        mongoAggregate.append("])");
        System.out.println("use " + mongoDataStore.getDatabaseName());
        System.out.println(mongoAggregate.toString());


        /*************/     //TODO: This should work, but doesn't
        System.out.println("dbName" + mongoDataStore.getDb().getName().toString());
        MongoDBCollection collection = mongoDataStore.getCollection(CoverageMongoDBWriter.COVERAGE_COLLECTION_NAME);
        QueryResult<DBObject> aggregate = collection.aggregate(operations, null);
        /*************/

        /*************/     //TODO: What's going on?
//        long startTime = System.currentTimeMillis();
//        AggregationOutput aggregationOutput = mongoDataStore.getDb().getCollection(CoverageMongoDBWriter.COVERAGE_COLLECTION_NAME)
// .aggregate(operations);
//
//        List<DBObject> results = new LinkedList<>();
//        for (DBObject object : aggregationOutput.results()) {
//            results.add(object);
//        }
//
//        long endTime = System.currentTimeMillis();
//        QueryResult<DBObject> aggregate = new QueryResult<>(fileId, ((int) (endTime - startTime)), results.size(), results.size(), "",
// "", results);
        /*************/

//        System.out.println(collection.find(new BasicDBObject(), new QueryOptions("limit", 2), null));
//        System.out.println(collection.find(new BasicDBObject("files.id", "34"), new QueryOptions("limit", 2), null));
//        System.out.println(collection.find(new BasicDBObject("files.id", "7"), new QueryOptions("limit", 2), null));
//        System.out.println(collection.find(new BasicDBObject("files.id", "4"), new QueryOptions("limit", 2), null));

        for (DBObject object : aggregate.getResult()) {
            int id = getInt(object, "_id");
            int start = id * size + 1;
            int end = id * size + size;
            object.put("chromosome", region.getChromosome());
            object.put("start", start);
            object.put("end", end);
            double featuresCount = getDouble(object, FEATURES_COUNT);
//            object.put("features_count_old", featureCount);
            featuresCount /= 1 + (end - 1) / chunkSize - (start + chunkSize - 2) / chunkSize;
            object.put(FEATURES_COUNT, featuresCount);
//            object.put("div1", end/chunkSize - start/chunkSize);
//            object.put("div2", end/chunkSize - (start+chunkSize)/chunkSize);
//            object.put("div3", (end-1)/chunkSize - (start+chunkSize-2)/chunkSize);
        }

        aggregate.setId(fileId);

        return aggregate;
    }

    private int getInt(DBObject object, String key) {
        int i;
        Object oi = object.get(key);
        if (oi instanceof Double) {
            i = (int) (double) oi;
        } else if (oi instanceof Float) {
            i = (int) (float) oi;
        } else {
            i = Integer.parseInt(oi.toString());
        }
        return i;
    }

    private double getDouble(DBObject object, String key) {
        double d;
        Object od = object.get(key);
        if (od instanceof Double) {
            d = (double) od;
        } else if (od instanceof Float) {
            d = (float) od;
        } else {
            d = Double.parseDouble(od.toString());
        }
        return d;
    }


    private QueryResult privateGetAllIntervalFrequencies(Region region, QueryOptions options) {
        long startTime = System.currentTimeMillis();
        MongoDBCollection collection = mongoDataStore.getCollection(CoverageMongoDBWriter.COVERAGE_COLLECTION_NAME);

        int size = region.getEnd() - region.getStart();
        String fileId = options.getString(QO_FILE_ID);
//        boolean histogram = options.getBoolean(QO_HISTOGRAM, size < 2000);
        boolean histogram = true;
        int batchSize = histogram ? options.getInt(QO_INTERVAL_SIZE, 1000) : 1000;
        String batchName = MeanCoverage.sizeToNameConvert(batchSize);

        String coverageType;
        ComplexTypeConverter complexTypeConverter;
        if (histogram) {
            complexTypeConverter = new DBObjectToMeanCoverageConverter();
            coverageType = CoverageMongoDBWriter.AVERAGE_FIELD;
        } else {
            complexTypeConverter = new DBObjectToRegionCoverageConverter();
            coverageType = CoverageMongoDBWriter.COVERAGE_FIELD;
        }
        List<String> regions = new LinkedList<>();

        for (int i = region.getStart() / batchSize; i <= (region.getEnd() - 1) / batchSize; i++) {
            regions.add(region.getChromosome() + "_" + i + "_" + batchName);
        }


        //db.alignment.find( { _id:{$in:regions}}, { files: {$elemMatch : {id:fileId} }, "files.cov":0 , "files.id":0 } )}
        DBObject query = new BasicDBObject(CoverageMongoDBWriter.ID_FIELD, new BasicDBObject("$in", regions));
        DBObject projection = BasicDBObjectBuilder
                .start()
//                .append(CoverageMongoWriter.FILES_FIELD+"."+CoverageMongoWriter.FILE_ID_FIELD,fileId)
                .append(CoverageMongoDBWriter.FILES_FIELD, new BasicDBObject("$elemMatch", new BasicDBObject(CoverageMongoDBWriter
                        .FILE_ID_FIELD, fileId)))
                .append(CoverageMongoDBWriter.FILES_FIELD + "." + coverageType, true)

                //.append(CoverageMongoWriter.FILES_FIELD+"."+DBObjectToRegionCoverageConverter.COVERAGE_FIELD, true)
                //.append(CoverageMongoWriter.FILES_FIELD+"."+DBObjectToMeanCoverageConverter.AVERAGE_FIELD, true)
                //.append(CoverageMongoWriter.FILES_FIELD+"."+CoverageMongoWriter.FILE_ID_FIELD, true)
                .get();

        System.out.println("db." + CoverageMongoDBWriter.COVERAGE_COLLECTION_NAME + ".find(" + query.toString() + ", " + projection
                .toString() + ")");

        QueryResult queryResult = collection.find(query, projection, complexTypeConverter, null);
        queryResult.setId(region.toString());
        queryResult.setTime((int) (System.currentTimeMillis() - startTime));
        return queryResult;
    }

    @Override
    public QueryResult getAlignmentsHistogramByRegion(Region region, boolean histogramLogarithm, int histogramMax) {
        return null;
    }

    @Override
    public QueryResult getAlignmentRegionInfo(Region region, QueryOptions options) {
        return null;
    }

    /* ******************************************
     *              Auxiliary queries           *
     * ******************************************/
    private List<SAMRecord> getSamRecordsByRegion(Path bamPath, Path baiPath, Region region) {
        List<SAMRecord> records = new ArrayList<>();

        SAMFileReader inputSam = new SAMFileReader(bamPath.toFile(), baiPath.toFile());
        inputSam.setValidationStringency(SAMFileReader.getDefaultValidationStringency().valueOf("LENIENT"));
        SAMRecordIterator recordsRegion = inputSam.query(region.getChromosome(), (int) region.getStart(), (int) region.getEnd(), false);

        SAMRecord record;
        while (recordsRegion.hasNext()) {
            record = recordsRegion.next();
            records.add(record);
        }

        return records;

    }

    private List<Alignment> getAlignmentsFromSamRecords(List<SAMRecord> records, QueryOptions params) {
        List<Alignment> alignments = new ArrayList<>();

        if (params.getBoolean(QO_VIEW_AS_PAIRS, false)) {
            // If must be shown as pairs, create new comparator by read name
            Collections.sort(records, new Comparator<SAMRecord>() {
                @Override
                public int compare(SAMRecord o1, SAMRecord o2) {
                    if (o1 != null && o1.getReadName() != null && o2 != null) {
                        return o1.getReadName().compareTo(o2.getReadName());
                    }
                    return -1;
                }
            });
        }

        // Create an Alignment per SAMRecord object
        for (SAMRecord record : records) {
            // Build alignment object, including differences calculation
            Alignment alignment = converter.buildAlignment(record, params.getBoolean(QO_PROCESS_DIFFERENCES, true));
            alignments.add(alignment);
        }

        return alignments;
    }

    private List<Alignment> getAlignmentsInRegion(List<Alignment> alignments, Region region) {
        LinkedList<Alignment> filteredAlignments = new LinkedList<>();
        for (Alignment alignment : alignments) {
            if (alignment.getStart() >= region.getStart() /*&& alignment.getEnd() <= region.getEnd()*/) {
                filteredAlignments.addLast(alignment);
            }
        }
        return filteredAlignments;
    }

    private static String getIndexFromBam(String bam) {
        String bai = bam + ".bai";
        if (Paths.get(bai).toFile().exists()) {
            return bai;
        } else {
            bai = IOUtils.removeExtension(bam) + ".bai";
            if (Paths.get(bai).toFile().exists()) {
                return bai;
            } else {
                return "";
            }
        }
    }

    private static RegionCoverage calculateCoverageByRegion(AlignmentRegion alignmentRegion, Region region) {
        AlignmentRegionCoverageCalculatorTask task = new AlignmentRegionCoverageCalculatorTask();
        try {
            task.apply(Arrays.asList(alignmentRegion));
        } catch (IOException e) {
            e.printStackTrace();
            logger.warn(e.getMessage());
            return null;
        }
        RegionCoverage coverage = alignmentRegion.getCoverage();
        int from = (int) (region.getStart() - coverage.getStart());
        int to = (int) (region.getEnd() - coverage.getStart() + 1);
        RegionCoverage coverageAdjust = new RegionCoverage(to - from);
        coverageAdjust.setChromosome(region.getChromosome());
        coverageAdjust.setStart(region.getStart());
        coverageAdjust.setEnd(region.getEnd());

//            int scrPos  = from>0 ? from : 0;
//            int destPos = from>0 ? 0 : from;
//            int length = Math.min(coverageAdjust.getAll().length,coverageAdjust.getAll().length);
//            System.arraycopy(coverage.getAll(), scrPos, coverageAdjust.getAll(), destPos, length);
//            System.arraycopy(coverage.getA()  , scrPos, coverageAdjust.getA()  , destPos, length);
//            System.arraycopy(coverage.getC()  , scrPos, coverageAdjust.getC()  , destPos, length);
//            System.arraycopy(coverage.getG()  , scrPos, coverageAdjust.getG()  , destPos, length);
//            System.arraycopy(coverage.getT()  , scrPos, coverageAdjust.getT()  , destPos, length);

        coverageAdjust.setAll(copyOfRange(coverage.getAll(), from, to));
        coverageAdjust.setA(copyOfRange(coverage.getA(), from, to));
        coverageAdjust.setC(copyOfRange(coverage.getC(), from, to));
        coverageAdjust.setG(copyOfRange(coverage.getG(), from, to));
        coverageAdjust.setT(copyOfRange(coverage.getT(), from, to));

        alignmentRegion.setCoverage(null);

        return coverageAdjust;
    }

    private static short[] copyOfRange(short[] array, int from, int to) {
        if (from < 0) {
            short[] shorts = new short[to - from];
//            for(int i = 0; i < Math.min(to, array.length); i++){
//                shorts[i-from] = array[i];
//            }
            System.arraycopy(array, 0, shorts, 0 - from, Math.min(to, array.length));


//            for(int i = 0; i < Math.min(to, array.length); i++){
//                shorts[i-Math.min(from, 0)] = array[i-Math.max(from, 0)];
//            }
            return shorts;
        } else {
            return Arrays.copyOfRange(array, from, to);
        }
    }

}
