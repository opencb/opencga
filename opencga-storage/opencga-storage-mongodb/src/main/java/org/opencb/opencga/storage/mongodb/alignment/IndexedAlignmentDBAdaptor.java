package org.opencb.opencga.storage.mongodb.alignment;

import com.mongodb.BasicDBObject;
import com.mongodb.BasicDBObjectBuilder;
import com.mongodb.DBObject;
import net.sf.samtools.SAMFileReader;
import net.sf.samtools.SAMRecord;
import net.sf.samtools.SAMRecordIterator;
import org.opencb.biodata.formats.alignment.AlignmentConverter;
import org.opencb.biodata.formats.sequence.fasta.dbadaptor.CellBaseSequenceDBAdaptor;
import org.opencb.biodata.formats.sequence.fasta.dbadaptor.SequenceDBAdaptor;
import org.opencb.biodata.models.alignment.Alignment;
import org.opencb.biodata.models.alignment.AlignmentRegion;
import org.opencb.biodata.models.alignment.stats.MeanCoverage;
import org.opencb.biodata.models.alignment.stats.RegionCoverage;
import org.opencb.biodata.models.feature.Region;
import org.opencb.datastore.core.ComplexTypeConverter;
import org.opencb.datastore.core.QueryOptions;
import org.opencb.datastore.core.QueryResult;
import org.opencb.datastore.mongodb.MongoDBCollection;
import org.opencb.datastore.mongodb.MongoDataStore;
import org.opencb.datastore.mongodb.MongoDataStoreManager;
import org.opencb.opencga.lib.common.IOUtils;
import org.opencb.opencga.storage.core.alignment.adaptors.AlignmentQueryBuilder;
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
public class IndexedAlignmentDBAdaptor implements AlignmentQueryBuilder {

    //Query Options
    public static final String QO_BAM_PATH = "bam_path";
    public static final String QO_BAI_PATH = "bai_path";
    public static final String QO_VIEW_AS_PAIRS = "view_as_pairs";
    public static final String QO_PROCESS_DIFFERENCES = "process_differences";
    public static final String QO_FILE_ID = "file_id";
    public static final String QO_HISTOGRAM = "coverage";
    //public static final String QO_AVERAGE = "average";
    public static final String QO_BATCH_SIZE = "batch_size";


    protected static org.slf4j.Logger logger = LoggerFactory.getLogger(IndexedAlignmentDBAdaptor.class);

    private AlignmentConverter converter;
    private MongoDataStoreManager mongoManager;
    private MongoDataStore mongoDataStore;
    private MongoDBCollection collection;


    public IndexedAlignmentDBAdaptor(SequenceDBAdaptor adaptor, MongoCredentials credentials) {
        try {
            this.converter = new AlignmentConverter(adaptor);
        } catch (IOException e) {
            e.printStackTrace();
        }

        mongoManager = new MongoDataStoreManager(credentials.getMongoHost(), credentials.getMongoPort());
        mongoDataStore = mongoManager.get(credentials.getMongoDbName());
        collection = mongoDataStore.getCollection(CoverageMongoWriter.COVERAGE_COLLECTION_NAME);

    }
    public IndexedAlignmentDBAdaptor(MongoCredentials credentials) {
        this(new CellBaseSequenceDBAdaptor(), credentials);
    }

    /**
     *
     * @param region Query Region
     * @param options Query Options: Expected bam_path. Optionally bai_path and view_as_pairs
     * @return
     */
    @Override
    public QueryResult getAllAlignmentsByRegion(Region region, QueryOptions options) {


        String bam = options.getString(QO_BAM_PATH, "");
        String bai = options.getString(QO_BAI_PATH, "");
        if(bai.equals("")) {
            bai = getIndexFromBam(bam);
        }
        Path bamFile = Paths.get(bam);
        Path baiFile = Paths.get(bai);

        QueryResult<Alignment> queryResult = new QueryResult<>(
                String.format("%s:%d-%d", region.getChromosome(), region.getStart(), region.getEnd()));

        long startTime = System.currentTimeMillis();


        if(!bam.endsWith("")){
            queryResult.setErrorMsg("Expected parameter \"" + QO_BAM_PATH + "=*.bam\"");
            logger.warn("Expected parameter \"" + QO_BAM_PATH + "=*.bam\"");
        } if(!bam.endsWith(".bam")){
            queryResult.setErrorMsg("Unsupported extension for \"" + QO_BAM_PATH + "=" + bam + "\"");
            logger.warn("Unsupported extension for \"" + QO_BAM_PATH + "=" + bam + "\"");
        } else if(!bamFile.toFile().exists()) {
            queryResult.setErrorMsg("BAM file not found");
            logger.warn("BAM file " + bamFile + " not found");
        } else if(!bai.endsWith(".bai")){
            queryResult.setErrorMsg("Can't find BAM index file. Expected parameter \"" + QO_BAI_PATH + "=*.bai\"");
            logger.warn("Can't find BAM index file. Expected parameter \"" + QO_BAI_PATH + "=*.bai\".");
        } else if (!baiFile.toFile().exists()){
            queryResult.setErrorMsg("BAM index file (.bai) not found");
            logger.warn("BAM index file (.bai) " + baiFile + " for file " + bamFile + " not found");
        } else {
            List<SAMRecord> recordList = getSamRecordsByRegion(bamFile, baiFile, region);
            List<Alignment> alignmentList = getAlignmentsFromSamRecords(recordList, options);
            queryResult.setResult(alignmentList);
            queryResult.setNumResults(alignmentList.size());
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

        QueryResult alignmentsResult = this.getAllAlignmentsByRegion(region, options);
        if(alignmentsResult.getResultType() == Alignment.class.getCanonicalName()) {
            AlignmentRegionCoverageCalculatorTask task = new AlignmentRegionCoverageCalculatorTask();
            AlignmentRegion alignmentRegion = new AlignmentRegion(alignmentsResult.getResult(), null);
//            alignmentRegion.setAlignments(alignmentsResult.getResult());
            try {
                task.apply(Arrays.asList(alignmentRegion));
            } catch (IOException e) {
                e.printStackTrace();
                queryResult.setErrorMsg(e.getMessage());    //ERROR
                logger.warn(e.getMessage());
            }
            RegionCoverage coverage = alignmentRegion.getCoverage();

            int from = (int) (region.getStart() - coverage.getStart());
            int to = (int) (region.getEnd() - coverage.getStart());
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
            if(from < 0){
                from = 0;
                System.out.println("[ERROR] from < 0");
            } else {
                coverageAdjust.setAll(copyOfRange(coverage.getAll(), from, to));
                coverageAdjust.setA  (copyOfRange(coverage.getA()  , from, to));
                coverageAdjust.setC  (copyOfRange(coverage.getC()  , from, to));
                coverageAdjust.setG  (copyOfRange(coverage.getG()  , from, to));
                coverageAdjust.setT  (copyOfRange(coverage.getT()  , from, to));
            }
            queryResult.setResult(Arrays.asList(coverageAdjust));
            queryResult.setNumResults(1);
        } else {
            queryResult.setErrorMsg(alignmentsResult.getErrorMsg());    //ERROR
            logger.warn(alignmentsResult.getErrorMsg());
        }
        queryResult.setTime((int) (System.currentTimeMillis() - startTime));
        return queryResult;
    }


    public QueryResult getAllIntervalFrequencies(Region region, QueryOptions options) {
        long startTime = System.currentTimeMillis();

        int size = region.getEnd()-region.getStart();
        String fileId = options.getString(QO_FILE_ID);
//        boolean histogram = options.getBoolean(QO_HISTOGRAM, size < 2000);
        boolean histogram = true;
        int batchSize = histogram? options.getInt(QO_BATCH_SIZE, 1000) : 1000;
        String batchName = MeanCoverage.sizeToNameConvert(batchSize);

        String coverageType;
        ComplexTypeConverter complexTypeConverter;
        if(histogram) {
            complexTypeConverter = new DBObjectToMeanCoverageConverter();
            coverageType = CoverageMongoWriter.AVERAGE_FIELD;
        } else {
            complexTypeConverter = new DBObjectToRegionCoverageConverter();
            coverageType = CoverageMongoWriter.COVERAGE_FIELD;
        }
        List<String> regions = new LinkedList<>();

        for(int i = region.getStart()/batchSize;  i <= (region.getEnd()-1)/batchSize; i++){
            regions.add(region.getChromosome()+"_"+i+"_"+batchName);
        }


        //db.alignment.find( { _id:{$in:regions}}, { files: {$elemMatch : {id:fileId} }, "files.cov":0 , "files.id":0 } )}
        DBObject query = new BasicDBObject(CoverageMongoWriter.ID_FIELD, new BasicDBObject("$in", regions));
        DBObject projection = BasicDBObjectBuilder
                .start()
//                .append(CoverageMongoWriter.FILES_FIELD+"."+CoverageMongoWriter.FILE_ID_FIELD,fileId)
                .append(CoverageMongoWriter.FILES_FIELD, new BasicDBObject("$elemMatch", new BasicDBObject(CoverageMongoWriter.FILE_ID_FIELD,fileId)))
                .append(CoverageMongoWriter.FILES_FIELD+"."+coverageType, true)

                //.append(CoverageMongoWriter.FILES_FIELD+"."+DBObjectToRegionCoverageConverter.COVERAGE_FIELD, true)
                //.append(CoverageMongoWriter.FILES_FIELD+"."+DBObjectToMeanCoverageConverter.AVERAGE_FIELD, true)
                //.append(CoverageMongoWriter.FILES_FIELD+"."+CoverageMongoWriter.FILE_ID_FIELD, true)
                .get();

        System.out.println("db."+CoverageMongoWriter.COVERAGE_COLLECTION_NAME+".find("+query.toString()+", "+projection.toString()+")");

        QueryResult queryResult = collection.find(query, null, complexTypeConverter, projection);
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
    private List<SAMRecord> getSamRecordsByRegion(Path bamPath, Path baiPath, Region region){
        List<SAMRecord> records = new ArrayList<>();

        SAMFileReader inputSam = new SAMFileReader(bamPath.toFile(), baiPath.toFile());
        inputSam.setValidationStringency(SAMFileReader.ValidationStringency.valueOf("LENIENT"));
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


    private String getIndexFromBam(String bam) {
        String bai;
        if(Paths.get((bai = bam+".bai")).toFile().exists()){
            return bai;
        } else if(Paths.get(bai = (IOUtils.removeExtension(bam) + ".bai")).toFile().exists()){
            return bai;
        } else {
            return "";
        }
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
