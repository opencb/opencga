package org.opencb.opencga.storage.mongodb.alignment;

import org.opencb.biodata.formats.alignment.io.AlignmentDataWriter;
import org.opencb.biodata.formats.alignment.io.AlignmentDataReader;
import org.opencb.biodata.formats.alignment.io.AlignmentRegionDataReader;
import org.opencb.biodata.formats.alignment.sam.io.AlignmentBamDataReader;
import org.opencb.biodata.formats.alignment.sam.io.AlignmentSamDataReader;
import org.opencb.biodata.formats.io.FileFormatException;
import org.opencb.biodata.formats.sequence.fasta.dbadaptor.CellBaseSequenceDBAdaptor;
import org.opencb.biodata.formats.sequence.fasta.dbadaptor.SequenceDBAdaptor;
import org.opencb.biodata.models.alignment.AlignmentRegion;
import org.opencb.commons.io.DataWriter;
import org.opencb.commons.run.Runner;
import org.opencb.commons.run.Task;
import org.opencb.opencga.lib.common.IOUtils;
import org.opencb.opencga.storage.core.alignment.AlignmentStorageManager;
import org.opencb.opencga.storage.core.alignment.adaptors.AlignmentQueryBuilder;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Map;
import java.util.Properties;

/**
 * Created by jacobo on 15/08/14.
 */
public class MongoDBAlignmentStorageManager extends AlignmentStorageManager {

    private Properties properties;
    private AlignmentMetaDataDBAdaptor metadata;
   // private static Path indexerManagerScript = Paths.get(Config.getGcsaHome(), Config.getAnalysisProperties().getProperty("OPENCGA.ANALYSIS.BINARIES.PATH"), "indexer", "indexerManager.py");
    protected static org.slf4j.Logger logger = LoggerFactory.getLogger(MongoDBAlignmentStorageManager.class);


    public MongoDBAlignmentStorageManager(Path propertiesPath) {
        super(propertiesPath);

        this.properties = new Properties();

        try {
            properties.load(new InputStreamReader(new FileInputStream(propertiesPath.toString())));
        } catch (IOException e) {
            e.printStackTrace();
        }

        this.metadata = new AlignmentMetaDataDBAdaptor(properties.getProperty("files-index", "/tmp/files-index.properties"));


    }



    @Override
    public AlignmentDataWriter getDBWriter(Path credentials) {
        return null;
    }

//    @Override
//    public AlignmentQueryBuilder getDBAdaptor(Path sqlitePath) {
//        SqliteCredentials sqliteCredentials;
//        try {
//            sqliteCredentials = new SqliteCredentials(sqlitePath);
//        } catch (IllegalOpenCGACredentialsException e) {
//            e.printStackTrace();
//            return null;
//        }
//        return new TabixAlignmentQueryBuilder(sqliteCredentials, null, null);
//    }

    @Override
    public AlignmentQueryBuilder getDBAdaptor(Path path) {
        SequenceDBAdaptor adaptor;
        if (path == null) {
            adaptor = new CellBaseSequenceDBAdaptor();
        } else {
            adaptor = new SqliteSequenceDBAdaptor(path);
        }
        return new IndexedAlignmentDBAdaptor(adaptor);
    }


    @Override
    public void transform(Path input, Path pedigree, Path output, Map<String, Object> params) throws IOException, FileFormatException {

        /*
         * 1º Transform into a BAM
         * 2º Sort
         * 3º Index (bai)
         * 4º Add in metadata
         * 5º Calculate Coverage
         */

        //1º
        if(!input.endsWith(".bam")){

            System.out.println("[ERROR] Expected BAM file");
            throw new FileFormatException("Expected BAM file");
        }

        //2º Sort

        //3º
        String fileName = input.getFileName().toString();
        //name.bam
        //name.bam.bai
        Path inputBamIndexFile = Paths.get(fileName + ".bai");
        if (!Files.exists(inputBamIndexFile)) {

            System.out.println("[ERROR] Expected BAI file");
            throw new FileFormatException("Expected BAI file");
        }


        //4º Add in metadata
        String index = metadata.registerPath(input);


//        String study = params.get("study").toString();
//        boolean plain = Boolean.parseBoolean(params.get("plain").toString());
//
//
//        try {
//            Runtime.getRuntime().exec( "comand" ).waitFor();
//        } catch (InterruptedException e) {
//            e.printStackTrace();
//        }
//
//        //reader
//        AlignmentDataReader reader;
//
//        if(input.endsWith(".sam")){
//            reader = new AlignmentSamDataReader(input, study);
//        } else if (input.endsWith(".bam")) {
//            reader = new AlignmentBamDataReader(input, study);
//        } else {
//            throw new UnsupportedOperationException("[ERROR] Unsuported file input format : " + input);
//        }
//
//
//        //Tasks
//        List<Task<AlignmentRegion>> tasks = new LinkedList<>();
//
//        //Writer
//        List<DataWriter<AlignmentRegion>> writers = new LinkedList<>();
//        //writers.add(new AlignmentRegionDataWriter(new AlignmentJsonDataWriter(reader, output.toString(), plain)));
//
//
//        //Runner
//        AlignmentRegionDataReader regionReader = new AlignmentRegionDataReader(reader);
//        Runner<AlignmentRegion> runner = new Runner<>(regionReader, writers, tasks, 1);
//
//        System.out.println("Indexing alignments...");
//        runner.run();
//        System.out.println("Alignments indexed!");
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
