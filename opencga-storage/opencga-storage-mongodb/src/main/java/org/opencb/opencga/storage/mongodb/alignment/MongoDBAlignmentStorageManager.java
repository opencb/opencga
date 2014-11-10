package org.opencb.opencga.storage.mongodb.alignment;

import net.sf.samtools.BAMFileWriter;
import net.sf.samtools.SAMFileReader;
import net.sf.samtools.SAMRecordIterator;
import org.opencb.biodata.formats.alignment.io.AlignmentDataReader;
import org.opencb.biodata.formats.alignment.io.AlignmentRegionDataReader;
import org.opencb.biodata.formats.alignment.sam.io.AlignmentBamDataReader;
import org.opencb.biodata.formats.io.FileFormatException;
import org.opencb.biodata.formats.sequence.fasta.dbadaptor.CellBaseSequenceDBAdaptor;
import org.opencb.biodata.formats.sequence.fasta.dbadaptor.SequenceDBAdaptor;
import org.opencb.biodata.models.alignment.AlignmentRegion;
import org.opencb.commons.io.DataReader;
import org.opencb.commons.io.DataWriter;
import org.opencb.commons.run.Runner;
import org.opencb.commons.run.Task;
import org.opencb.datastore.core.ObjectMap;
import org.opencb.opencga.lib.auth.IllegalOpenCGACredentialsException;
import org.opencb.opencga.storage.core.alignment.AlignmentStorageManager;
import org.opencb.opencga.storage.core.alignment.adaptors.AlignmentQueryBuilder;
import org.opencb.opencga.storage.core.alignment.json.AlignmentCoverageJsonDataReader;
import org.opencb.opencga.storage.core.alignment.json.AlignmentCoverageJsonDataWriter;
import org.opencb.opencga.storage.core.alignment.tasks.AlignmentRegionCoverageCalculatorTask;
import org.opencb.opencga.storage.core.sequence.SqliteSequenceDBAdaptor;
import org.opencb.opencga.storage.mongodb.utils.MongoCredentials;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.*;

/**
 * Date 15/08/14.
 *
 * @author Jacobo Coll Moragon <jcoll@ebi.ac.uk>
 */
public class MongoDBAlignmentStorageManager extends AlignmentStorageManager {

    public static final String MONGO_DB_NAME = "opencga";
    public static final String STORAGE_SEQUENCE_DBADAPTOR = "OPENCGA.STORAGE.SEQUENCE.DB.ROOTDIR";

    //    private MongoCredentials mongoCredentials = null;
    private final Properties properties = new Properties();
    //private AlignmentMetaDataDBAdaptor metadata;
    //private static Path indexerManagerScript = Paths.get(Config.getGcsaHome(), Config.getAnalysisProperties().getProperty("OPENCGA.ANALYSIS.BINARIES.PATH"), "indexer", "indexerManager.py");
    protected static Logger logger = LoggerFactory.getLogger(MongoDBAlignmentStorageManager.class);


    public MongoDBAlignmentStorageManager(Path propertiesPath) {
        this();
        addPropertiesPath(propertiesPath);
    }

    public MongoDBAlignmentStorageManager() {
        super();
    }

    @Override
    public void addPropertiesPath(Path propertiesPath){
        if(propertiesPath != null && propertiesPath.toFile().exists()) {
            try {
                properties.load(new InputStreamReader(new FileInputStream(propertiesPath.toFile())));
            } catch (IOException e) {
                logger.error(e.getMessage(), e);
            }
        }
    }


    @Override
    public DataReader<AlignmentRegion> getDBSchemaReader(Path input) {
        String baseFileName = input.toString();
        String meanCoverageFile;
        String regionCoverageFile = baseFileName;
        if(baseFileName.endsWith(".bam")){
            regionCoverageFile = baseFileName + (Paths.get(baseFileName + ".coverage.json").toFile().exists() ?
                    ".coverage.json" :
                    ".coverage.json.gz");
            meanCoverageFile = baseFileName + (Paths.get(baseFileName + ".mean-coverage.json").toFile().exists() ?
                    ".mean-coverage.json" :
                    ".mean-coverage.json.gz");
        } else if(baseFileName.endsWith(".coverage.json")){
            meanCoverageFile = baseFileName.replaceFirst("coverage\\.json$", "mean-coverage.json");
        } else if(baseFileName.endsWith(".coverage.json.gz")){
            meanCoverageFile = baseFileName.replaceFirst("coverage\\.json\\.gz$", "mean-coverage.json.gz");
        } else {
            return null;
        }

//        String meanCoverage = input.toString()
//                .replaceFirst("coverage\\.json$", "mean-coverage.json")
//                .replaceFirst("coverage\\.json\\.gz$", "mean-coverage.json.gz");

        AlignmentCoverageJsonDataReader alignmentCoverageJsonDataReader = new AlignmentCoverageJsonDataReader(regionCoverageFile, meanCoverageFile);
        alignmentCoverageJsonDataReader.setReadRegionCoverage(false);
        return alignmentCoverageJsonDataReader;
    }

    @Override
    public DataWriter<AlignmentRegion> getDBWriter(Path credentials, String dbName, String fileId) {
        if (dbName == null || dbName.isEmpty()) {
            dbName = MONGO_DB_NAME;
            logger.info("Using default dbName in MongoDBAlignmentStorageManager.getDBWriter with fileId : " + fileId);
        }
        try {
            if(credentials != null && credentials.toFile().exists()) {
                Properties credentialsProperties = new Properties();
                credentialsProperties.load(new InputStreamReader(new FileInputStream(credentials.toString())));
                return new CoverageMongoWriter(new MongoCredentials(credentialsProperties), fileId);
            } else {
                return new CoverageMongoWriter(getMongoCredentials(dbName), fileId);
            }
        } catch (IOException e) {
            logger.error(e.toString(), e);
        }
        return null;
    }

    @Override
    public AlignmentQueryBuilder getDBAdaptor(String dbName) {
        SequenceDBAdaptor adaptor;
        if (dbName == null || dbName.isEmpty()) {
            dbName = properties.getProperty("OPENCGA.STORAGE.MONGO.ALIGNMENT.DB.NAME", MONGO_DB_NAME);
            logger.info("Using default dbName in MongoDBAlignmentStorageManager.getDBAdaptor()");
        }
        Path path = Paths.get(properties.getProperty(STORAGE_SEQUENCE_DBADAPTOR, ""));
        if (path == null || path.toString() == null || path.toString().isEmpty() || !path.toFile().exists()) {
            adaptor = new CellBaseSequenceDBAdaptor();
        } else {
            if(path.toString().endsWith("sqlite.db")){
                adaptor = new SqliteSequenceDBAdaptor(path);
            } else {
                adaptor = new CellBaseSequenceDBAdaptor(path);
            }
        }

        return new IndexedAlignmentDBAdaptor(adaptor, getMongoCredentials(dbName));
    }

    private MongoCredentials getMongoCredentials(String mongoDbName){
        try {   //TODO: Use user and password
            String mongoUser = properties.getProperty("OPENCGA.STORAGE.MONGO.ALIGNMENT.DB.USER", null);
            String mongoPassword = properties.getProperty("OPENCGA.STORAGE.MONGO.ALIGNMENT.DB.PASS", null);
            return new MongoCredentials(
                    properties.getProperty("OPENCGA.STORAGE.MONGO.ALIGNMENT.DB.HOST", "localhost"),
                    Integer.parseInt(properties.getProperty("OPENCGA.STORAGE.MONGO.ALIGNMENT.DB.PORT", "27017")),
                    mongoDbName,
                    null,
                    null
            );
            //this.mongoCredentials = new MongoCredentials(properties);
        } catch (IllegalOpenCGACredentialsException e) {
            logger.error(e.getMessage(), e);
            return null;
        }
    }

    /**
     * Copy into the output path                   : <outputPath>/<FILE_ID>.bam
     * Create the bai with the samtools            : <outputPath>/<FILE_ID>.bam.bai
     * Calculate the coverage                      : <outputPath>/<FILE_ID>.bam.mean-coverage.json[.gz]
     * Calculate the meanCoverage                  : <outputPath>/<FILE_ID>.bam.coverage.json[.gz]
     *
     * @param input         Sorted/unsorted bam file
     * @param pedigree      Not used
     * @param outputPath    Output path where files are created
     * @param params        Hash for extra params. FILE_ID, ENCRYPT, PLAIN, REGION_SIZE, MEAN_COVERAGE_SIZE_LIST
     * @throws IOException
     * @throws FileFormatException
     */
    @Override
    public void transform(Path input, Path pedigree, Path outputPath, ObjectMap params) throws IOException, FileFormatException {

//        String study = params.containsKey(STUDY)? params.get(STUDY).toString(): null;
        String defaultFileId = input.getFileName().toString().split("\\.")[0];
        String fileId = params.getString(FILE_ID, defaultFileId);
        String encrypt = params.getString(ENCRYPT, "null");
        boolean plain = params.getBoolean(PLAIN, false);
        boolean copy = params.getBoolean(COPY_FILE, params.containsKey(FILE_ID));
        int regionSize = params.getInt(REGION_SIZE, 200000);
        List<String> meanCoverageSizeList = params.getListAs(MEAN_COVERAGE_SIZE_LIST, String.class, new LinkedList<String>());
        //String fileName = inputPath.getFileName().toString();
        //Path sqliteSequenceDBPath = Paths.get("/media/Nusado/jacobo/opencga/sequence/human_g1k_v37.fasta.gz.sqlite.db");

        /*
         * 1 Transform into a BAM
         * 2 Sort
         * 3 Move to output
         * 4 Index (bai)
         * 5 Calculate Coverage
         */

        //1
        if(!input.toString().toLowerCase().endsWith(".bam")){

            //samtools -b -h -S file.sam > file.bam
//            String transform = "samtools -b -h " + inputPath.toString() + " -o " +
//            try {
//                Runtime.getRuntime().exec( "___" ).waitFor();
//            } catch (InterruptedException e) {
//                e.printStackTrace();
//            }
            System.out.println("[ERROR] Expected BAM file");
            throw new FileFormatException("Expected BAM file");
        }

        //2 Sort
        Path sortBam = sortAlignmentsFile(input);
        Path finalBamFile = outputPath.resolve(sortBam.getFileName()).toAbsolutePath();

//        Path bamFile = outputPath.resolve(fileId+".bam").toAbsolutePath();
//        if (!bamFile.toFile().exists()) {
//            //3
//            logger.info("Coping file. Encryption : " + encrypt);
//            long start = System.currentTimeMillis();
//            switch(encrypt){
//                case "aes-256":
//                    InputStream inputStream = new BufferedInputStream(new FileInputStream(sortBam.toFile()), 50000000);
//                    OutputStream outputStream = new BufferedOutputStream(new FileOutputStream(bamFile.toFile()), 50000000);       //TODO: ENCRYPT OUTPUT
//
//                    SAMFileReader reader = new SAMFileReader(inputStream);
//                    BAMFileWriter writer = new BAMFileWriter(outputStream, bamFile.toFile());
//
//                    writer.setSortOrder(reader.getFileHeader().getSortOrder(), true);   //Must be called before calling setHeader()
//                    writer.setHeader(reader.getFileHeader());
//                    SAMRecordIterator iterator = reader.iterator();
//                    while(iterator.hasNext()){
//                        writer.addAlignment(iterator.next());
//                    }
//
//                    writer.close();
//                    reader.close();
//                    break;
//                case "":
//                default:
//                    if(copy) {
//                        Files.copy(sortBam, bamFile);
//                    } else {
//                        bamFile = sortBam;
//                        logger.info("copy = false. Don't copy file.");
//                    }
//            }
//            long end = System.currentTimeMillis();
//            logger.info("end - start = " + (end - start)/1000.0+"s");
//        }

        //4
        //Path bamIndexFile = Paths.get(inputPath.toString() + ".bai");
        Path bamIndexFile = Paths.get(finalBamFile.toString() + ".bai");
//        Path bamIndexFile = Paths.get(outputPath.toString() , bamFile.toFile().getName().toString() + ".bai");
        if (!Files.exists(bamIndexFile)) {
            long start = System.currentTimeMillis();
            String indexBai = "samtools index " + sortBam.toString() + " " + bamIndexFile.toString();
            logger.info("Creating index : " + indexBai);
            try {
                Runtime.getRuntime().exec( indexBai ).waitFor();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
            long end = System.currentTimeMillis();
            logger.info("end - start = " + (end - start) / 1000.0 + "s");
        }

        //5 Calculate Coverage
        AlignmentDataReader reader;
//        if(inputPath.toString().endsWith(".sam")){
//            reader = new AlignmentSamDataReader(inputPath, study);
//        } else if (inputPath.toString().endsWith(".bam")) {
//            reader = new AlignmentBamDataReader(inputPath, study);
//        } else {
//            throw new UnsupportedOperationException("[ERROR] Unsuported file input format : " + inputPath);
//        }
        reader = new AlignmentBamDataReader(sortBam, null); //Read from sorted BamFile


        //Tasks
        List<Task<AlignmentRegion>> tasks = new LinkedList<>();
       // tasks.add(new AlignmentRegionCompactorTask(new SqliteSequenceDBAdaptor(sqliteSequenceDBPath)));
        AlignmentRegionCoverageCalculatorTask coverageCalculatorTask = new AlignmentRegionCoverageCalculatorTask();
        for (String size : meanCoverageSizeList) {
            coverageCalculatorTask.addMeanCoverageCalculator(size);
        }
        tasks.add(coverageCalculatorTask);

        //Writer
        List<DataWriter<AlignmentRegion>> writers = new LinkedList<>();
        //writers.add(new AlignmentRegionDataWriter(new AlignmentJsonDataWriter(reader, output.toString(), !plain)));
        String jsonOutputFiles = Paths.get(outputPath.toString(), finalBamFile.toFile().getName()).toString();
        writers.add(new AlignmentCoverageJsonDataWriter(jsonOutputFiles, !plain));



        //Runner
        AlignmentRegionDataReader regionReader = new AlignmentRegionDataReader(reader);
        regionReader.setMaxSequenceSize(regionSize);
        Runner<AlignmentRegion> runner = new Runner<>(regionReader, writers, tasks, 1);

        logger.info("Calculating coverage...");
        long start = System.currentTimeMillis();
        runner.run();
        long end = System.currentTimeMillis();
        logger.info("end - start = " + (end - start) / 1000.0 + "s");


        logger.info("done!");
    }

    private Path sortAlignmentsFile(Path input) throws IOException {
        Path sortBam;
        SAMFileReader reader = new SAMFileReader(input.toFile());
        switch (reader.getFileHeader().getSortOrder()) {
            case coordinate:
                sortBam = input;
                logger.info("File sorted.");
                break;
            case queryname:
            case unsorted:
            default:
                sortBam = Paths.get(input.toAbsolutePath().toString() + ".sort.bam");
                String sortCommand = "samtools sort -f " + input.toAbsolutePath().toString() + " " + sortBam.toString();
                logger.info("Sorting file : " + sortCommand);
                long start = System.currentTimeMillis();
                try {
                    Runtime.getRuntime().exec( sortCommand ).waitFor();
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
                long end = System.currentTimeMillis();
                logger.info("end - start = " + (end - start)/1000.0+"s");
                break;
        }
        reader.close();
        return sortBam;
    }


//    /**
//     * Sorts the BAM file and create index (bai file)
//     *
//     * @param inputBamPath Input BAM file path
//     * @param outputBamPath Output path for sorted BAM file and index
//     * @return
//     * @throws IOException
//     * @throws InterruptedException
//     */
//    public static String createIndex(Path inputBamPath, Path outputBamPath) throws IOException, InterruptedException {
//
//        String jobId = StringUtils.randomString(8);
//        String commandLine = indexerManagerScript + " -t bam -i " + inputBamPath + " --outdir " + outputBamPath;
//        try {
//            SgeManager.queueJob("indexer", jobId, 0, inputBamPath.getParent().toString(), commandLine);
//        } catch (Exception e) {
//            logger.error(e.toString());
////            throw new AnalysisExecutionException("ERROR: sge execution failed.");
//        }
//        return "indexer_" + jobId;
//    }

//    public AlignmentMetaDataDBAdaptor getMetadata() {
//        if(metadata == null){
//            this.metadata = new AlignmentMetaDataDBAdaptor(properties.getProperty("files-index", "/tmp/files-index.properties"));
//        }
//        return metadata;
//    }


    public Properties getProperties() {
        return properties;
    }
}
