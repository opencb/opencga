package org.opencb.opencga.storage.mongodb.alignment;

import org.opencb.biodata.formats.alignment.io.AlignmentDataReader;
import org.opencb.biodata.formats.alignment.io.AlignmentRegionDataReader;
import org.opencb.biodata.formats.alignment.sam.io.AlignmentBamDataReader;
import org.opencb.biodata.formats.alignment.sam.io.AlignmentSamDataReader;
import org.opencb.biodata.formats.io.FileFormatException;
import org.opencb.biodata.formats.sequence.fasta.dbadaptor.CellBaseSequenceDBAdaptor;
import org.opencb.biodata.formats.sequence.fasta.dbadaptor.SequenceDBAdaptor;
import org.opencb.biodata.models.alignment.AlignmentRegion;
import org.opencb.commons.io.DataReader;
import org.opencb.commons.io.DataWriter;
import org.opencb.commons.run.Runner;
import org.opencb.commons.run.Task;
import org.opencb.opencga.lib.auth.IllegalOpenCGACredentialsException;
import org.opencb.opencga.storage.core.alignment.AlignmentStorageManager;
import org.opencb.opencga.storage.core.alignment.adaptors.AlignmentQueryBuilder;
import org.opencb.opencga.storage.core.alignment.json.AlignmentCoverageJsonDataReader;
import org.opencb.opencga.storage.core.alignment.json.AlignmentCoverageJsonDataWriter;
import org.opencb.opencga.storage.core.alignment.tasks.AlignmentRegionCoverageCalculatorTask;
import org.opencb.opencga.storage.mongodb.sequence.SqliteSequenceDBAdaptor;
import org.opencb.opencga.storage.mongodb.utils.MongoCredentials;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.*;

/**
 * Created by jacobo on 15/08/14.
 */
public class MongoDBAlignmentStorageManager extends AlignmentStorageManager {

    public static final String MONGO_DB_NAME = "opencga";

    private MongoCredentials mongoCredentials = null;
    private final Properties properties = new Properties();
    private AlignmentMetaDataDBAdaptor metadata;
   // private static Path indexerManagerScript = Paths.get(Config.getGcsaHome(), Config.getAnalysisProperties().getProperty("OPENCGA.ANALYSIS.BINARIES.PATH"), "indexer", "indexerManager.py");
    protected static Logger logger = LoggerFactory.getLogger(MongoDBAlignmentStorageManager.class);


    public MongoDBAlignmentStorageManager() {

    }
    public MongoDBAlignmentStorageManager(Path propertiesPath) {
        super(propertiesPath);
        if(propertiesPath != null) {
            setPropertiesPath(propertiesPath);
        }

        this.metadata = new AlignmentMetaDataDBAdaptor(properties.getProperty("files-index", "/tmp/files-index.properties"));
    }

    @Override
    public void setPropertiesPath(Path propertiesPath){
        try {
                properties.load(new InputStreamReader(new FileInputStream(propertiesPath.toFile())));
            } catch (IOException e) {
                logger.error(e.getMessage(), e);
        }
    }


    @Override
    public DataReader<AlignmentRegion> getDBSchemaReader(Path input) {
        String meanCoverage = input.toString()
                .replaceFirst("coverage\\.json$", "mean-coverage.json")
                .replaceFirst("coverage\\.json\\.gz$", "mean-coverage.json.gz");

        return new AlignmentCoverageJsonDataReader(input.toString(), meanCoverage);
    }

    @Override
    public DataWriter<AlignmentRegion> getDBWriter(Path credentials, String fileId) {
        try {
            if(credentials != null) {
                Properties credentialsProperties = new Properties();
                credentialsProperties.load(new InputStreamReader(new FileInputStream(credentials.toString())));
                return new CoverageMongoWriter(new MongoCredentials(credentialsProperties), fileId);
            } else {
                return new CoverageMongoWriter(this.mongoCredentials, fileId);
            }
        } catch (IOException e) {
            logger.error(e.toString(), e);
        }
        return null;
    }


    @Override
    public AlignmentQueryBuilder getDBAdaptor(Path path) {
        SequenceDBAdaptor adaptor;
        if (path == null) {
            adaptor = new CellBaseSequenceDBAdaptor();
        } else {
            if(path.toString().endsWith("sqlite.db")){
                adaptor = new SqliteSequenceDBAdaptor(path);
            } else {
                adaptor = new CellBaseSequenceDBAdaptor(path);
            }
        }
        if(mongoCredentials == null){
            try {
                this.mongoCredentials = new MongoCredentials(
                        properties.getProperty("mongo_host","localhost"),
                        Integer.parseInt(properties.getProperty("mongo_port", "27017")),
                        properties.getProperty("mongo_db_name",MONGO_DB_NAME),
                        properties.getProperty("mongo_user",null),
                        properties.getProperty("mongo_password",null)
                );
                //this.mongoCredentials = new MongoCredentials(properties);
            } catch (IllegalOpenCGACredentialsException e) {
                logger.error(e.getMessage(), e);
            }
        }
        return new IndexedAlignmentDBAdaptor(adaptor, mongoCredentials);

    }


    @Override
    public void transform(Path inputPath, Path pedigree, Path output, Map<String, Object> params) throws IOException, FileFormatException {


        String study = params.containsKey(STUDY)? params.get(STUDY).toString(): STUDY;
        boolean plain = params.containsKey(PLAIN)? Boolean.parseBoolean(params.get(PLAIN).toString()): false;
        int regionSize = params.containsKey(REGION_SIZE)? Integer.parseInt(params.get(REGION_SIZE).toString()): 20000;
        List<String> meanCoverageSizeList = (List<String>) (params.containsKey(MEAN_COVERAGE_SIZE_LIST)? params.get(MEAN_COVERAGE_SIZE_LIST): new LinkedList<>());
        String fileName = inputPath.getFileName().toString();
        Path sqliteSequenceDBPath = Paths.get("/home/jacobo/Documentos/bioinfo/opencga/sequence/human_g1k_v37.fasta.gz.sqlite.db");

        /*
         * 1 Transform into a BAM
         * 2 Sort
         * 3 Index (bai)
         * 4 Add in metadata
         * 5 Calculate Coverage
         */

        //1
        if(!inputPath.toString().endsWith(".bam")){

            //samtools -b -h -S file.sam > file.bam

            System.out.println("[ERROR] Expected BAM file");
            throw new FileFormatException("Expected BAM file");
        }

        //2 Sort
        //samtools sort -o

        //3
        //name.bam
        //name.bam.bai
        Path inputBamIndexFile = Paths.get(inputPath.toString() + ".bai");
        if (!Files.exists(inputBamIndexFile)) {
            System.out.println("[ERROR] Expected BAI file");
            throw new FileFormatException("Expected BAI file");
        }


//        try {
//            Runtime.getRuntime().exec( "comand" ).waitFor();
//        } catch (InterruptedException e) {
//            e.printStackTrace();
//        }


        //4 Add in metadata
        String index = metadata.registerPath(inputPath);


        //5 Calculate Coverage
        //reader
        AlignmentDataReader reader;

        if(inputPath.toString().endsWith(".sam")){
            reader = new AlignmentSamDataReader(inputPath, study);
        } else if (inputPath.toString().endsWith(".bam")) {
            reader = new AlignmentBamDataReader(inputPath, study);
        } else {
            throw new UnsupportedOperationException("[ERROR] Unsuported file input format : " + inputPath);
        }


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
        writers.add(new AlignmentCoverageJsonDataWriter(Paths.get(output.toString(), fileName).toString(), !plain));



        //Runner
        AlignmentRegionDataReader regionReader = new AlignmentRegionDataReader(reader);
        regionReader.setMaxSequenceSize(regionSize);
        Runner<AlignmentRegion> runner = new Runner<>(regionReader, writers, tasks, 1);

        System.out.println("Indexing alignments...");
        runner.run();
        System.out.println("Alignments indexed!");
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

    public AlignmentMetaDataDBAdaptor getMetadata() {
        return metadata;
    }
}
