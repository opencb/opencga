package org.opencb.opencga.storage.core.alignment;

import net.sf.samtools.SAMFileHeader;
import net.sf.samtools.SAMFileReader;
import org.opencb.biodata.formats.alignment.io.AlignmentDataReader;
import org.opencb.biodata.formats.alignment.io.AlignmentRegionDataWriter;
import org.opencb.biodata.formats.alignment.sam.io.AlignmentBamDataReader;
import org.opencb.biodata.formats.io.FileFormatException;
import org.opencb.biodata.formats.alignment.io.AlignmentRegionDataReader;
import org.opencb.biodata.models.alignment.AlignmentRegion;
import org.opencb.commons.io.DataWriter;
import org.opencb.commons.run.Runner;
import org.opencb.commons.run.Task;
import org.opencb.datastore.core.ObjectMap;
import org.opencb.opencga.storage.core.StorageManager;
import org.opencb.opencga.storage.core.alignment.adaptors.AlignmentDBAdaptor;
import org.opencb.opencga.storage.core.alignment.json.AlignmentCoverageJsonDataReader;
import org.opencb.opencga.storage.core.alignment.json.AlignmentCoverageJsonDataWriter;
import org.opencb.opencga.storage.core.alignment.json.AlignmentJsonDataReader;
import org.opencb.opencga.storage.core.alignment.json.AlignmentJsonDataWriter;
import org.opencb.opencga.storage.core.alignment.tasks.AlignmentRegionCoverageCalculatorTask;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.LinkedList;
import java.util.List;
import java.util.Properties;

/**
 * Created by jacobo on 14/08/14.
 */
public abstract class AlignmentStorageManager implements StorageManager<DataWriter<AlignmentRegion>, AlignmentDBAdaptor> { //DataReader<AlignmentRegion>,


    public static final String MEAN_COVERAGE_SIZE_LIST = "meanCoverageSizeList";
    public static final String PLAIN = "plain";
    public static final String REGION_SIZE = "regionSize";
    public static final String STUDY = "study";
    public static final String FILE_ID = "fileId";
    public static final String FILE_ALIAS = "fileAlias";
    public static final String WRITE_ALIGNMENTS = "writeAlignments";
    public static final String INCLUDE_COVERAGE = "includeCoverage";
    public static final String CREATE_BAI = "createBai";
    public static final String ENCRYPT = "encrypt";
    public static final String COPY_FILE = "copy";
    public static final String DB_NAME = "dbName";

    protected final Properties properties = new Properties();
    protected Logger logger = LoggerFactory.getLogger(AlignmentStorageManager.class);

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

    public Properties getProperties() {
        return properties;
    }

    @Override
    public void extract(URI from, URI to, ObjectMap params) {

    }

    @Override
    public void preTransform(URI inputUri, ObjectMap params) throws IOException, FileFormatException {
        checkUri(inputUri, "input file");
        Path input = Paths.get(inputUri);
        checkBamFile(new FileInputStream(input.toFile()), input.getFileName().toString());  //Check if BAM file is sorted
    }

    /**
     * if FILE_ALIAS == null
     *  FILE_ALIAS = fileName - ".bam"
     *
     * if ENCRYPT
     *  Copy into the output path                   : <outputPath>/<FILE_ALIAS>.encrypt.bam                 (pending)
     * if !ENCRYPT && COPY_FILE
     *  Encrypt into the output path                : <outputPath>/<FILE_ALIAS>.bam                         (pending)
     * if CREATE_BAI
     *  Create the bai with the samtools            : <outputPath>/<FILE_ALIAS>.bam.bai
     * if WRITE_ALIGNMENTS
     *  Write Json alignments                       : <outputPath>/<FILE_ALIAS>.bam.alignments.json[.gz]
     * if INCLUDE_COVERAGE
     *  Calculate the coverage                      : <outputPath>/<FILE_ALIAS>.bam.coverage.json[.gz]
     * if INCLUDE_COVERAGE && MEAN_COVERAGE_SIZE_LIST
     *  Calculate the meanCoverage                  : <outputPath>/<FILE_ALIAS>.bam.mean-coverage.json[.gz]
     *
     *
     * @param inputUri      Sorted bam file
     * @param pedigree      Not used
     * @param outputUri     Output path where files are created
     * @param params        Hash for extra params. FILE_ID, ENCRYPT, PLAIN, REGION_SIZE, MEAN_COVERAGE_SIZE_LIST
     * @throws IOException
     * @throws FileFormatException
     */
    @Override
    public void transform(URI inputUri, URI pedigree, URI outputUri, ObjectMap params)
            throws IOException, FileFormatException {

        checkUri(inputUri, "input file");
        checkUri(outputUri, "output directory");

        Path input = Paths.get(inputUri.getPath());
        Path output = Paths.get(outputUri.getPath());
        Path bamFile = input;

        checkBamFile(new FileInputStream(input.toFile()), input.getFileName().toString());  //Check if BAM file is sorted

        boolean plain = params.getBoolean(PLAIN, false);
        boolean writeJsonAlignments = params.getBoolean(WRITE_ALIGNMENTS, true);
        boolean includeCoverage = params.getBoolean(INCLUDE_COVERAGE, false);
        boolean createBai = params.getBoolean(CREATE_BAI, false);
        int regionSize = params.getInt(REGION_SIZE,
                Integer.parseInt(properties.getProperty("OPENCGA.STORAGE.ALIGNMENT.TRANSFORM.REGION_SIZE", "200000")));
        List<String> meanCoverageSizeList = params.getListAs(MEAN_COVERAGE_SIZE_LIST, String.class, new LinkedList<String>());
        String defaultFileAlias = input.getFileName().toString().substring(0, input.getFileName().toString().lastIndexOf("."));
        String fileAlias = params.getString(FILE_ALIAS, defaultFileAlias);

//        String encrypt = params.getString(ENCRYPT, "null");
//        boolean copy = params.getBoolean(COPY_FILE, params.containsKey(FILE_ID));
//        String fileName = inputPath.getFileName().toString();
//        Path sqliteSequenceDBPath = Paths.get("/media/Nusado/jacobo/opencga/sequence/human_g1k_v37.fasta.gz.sqlite.db");


        //1 Encrypt
        //encrypt(encrypt, bamFile, fileId, output, copy);

        //2 Index (bai)
        if(createBai) {
            Path bamIndexFile = output.resolve(fileAlias + ".bam.bai");
            if (!Files.exists(bamIndexFile)) {
                long start = System.currentTimeMillis();
                String indexBai = "samtools index " + input.toString() + " " + bamIndexFile.toString();
                logger.info("Creating index : " + indexBai);
                try {
                    Runtime.getRuntime().exec(indexBai).waitFor();
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
                long end = System.currentTimeMillis();
                logger.info("end - start = " + (end - start) / 1000.0 + "s");
            }
        }

        //3 Calculate Coverage and transform

        //Reader
        AlignmentDataReader reader;
        reader = new AlignmentBamDataReader(bamFile, null); //Read from sorted BamFile


        //Tasks
        List<Task<AlignmentRegion>> tasks = new LinkedList<>();
        // tasks.add(new AlignmentRegionCompactorTask(new SqliteSequenceDBAdaptor(sqliteSequenceDBPath)));
        if(includeCoverage) {
            AlignmentRegionCoverageCalculatorTask coverageCalculatorTask = new AlignmentRegionCoverageCalculatorTask();
            for (String size : meanCoverageSizeList) {
                coverageCalculatorTask.addMeanCoverageCalculator(size);
            }
            tasks.add(coverageCalculatorTask);
        }

        //Writers
        List<DataWriter<AlignmentRegion>> writers = new LinkedList<>();
        String jsonOutputFiles = output.resolve(fileAlias + ".bam").toString();

        if(writeJsonAlignments) {
            writers.add(new AlignmentRegionDataWriter(new AlignmentJsonDataWriter(reader, jsonOutputFiles, !plain)));
        }
        if(includeCoverage) {
            AlignmentCoverageJsonDataWriter alignmentCoverageJsonDataWriter =
                    new AlignmentCoverageJsonDataWriter(jsonOutputFiles, !plain);
            alignmentCoverageJsonDataWriter.setChunkSize(
                    Integer.parseInt(properties.getProperty("OPENCGA.STORAGE.ALIGNMENT.TRANSFORM.COVERAGE_CHUNK_SIZE", "1000")));
            writers.add(alignmentCoverageJsonDataWriter);
        }
        if(writers.isEmpty()) {
            logger.warn("No writers for transform-alignments!");
        }


        //Runner
        AlignmentRegionDataReader regionReader = new AlignmentRegionDataReader(reader);
        regionReader.setMaxSequenceSize(regionSize);
        Runner<AlignmentRegion> runner = new Runner<>(regionReader, writers, tasks, 1);

        logger.info("Transforming alignments...");
        long start = System.currentTimeMillis();
        runner.run();
        long end = System.currentTimeMillis();
        logger.info("end - start = " + (end - start) / 1000.0 + "s");


        logger.info("done!");
    }

    @Override
    public void postTransform(URI output, ObjectMap params) throws IOException, FileFormatException {

    }

    protected Path encrypt(String encrypt, Path bamFile, String fileName, Path outdir, boolean copy) throws IOException {
        logger.info("Coping file. Encryption : " + encrypt);
        long start = System.currentTimeMillis();
        if(fileName == null || fileName.isEmpty()) {
            fileName = bamFile.getFileName().toString();
        } else {
            fileName += ".bam";
        }
        Path destFile;
        switch(encrypt){
            case "aes-256": {
                destFile = outdir.resolve(fileName + ".encrypt");
//                InputStream inputStream = new BufferedInputStream(new FileInputStream(sortBam.toFile()), 50000000);
//                OutputStream outputStream = new BufferedOutputStream(new FileOutputStream(bamFile.toFile()), 50000000);       //TODO: ENCRYPT OUTPUT
//
//                SAMFileReader reader = new SAMFileReader(inputStream);
//                BAMFileWriter writer = new BAMFileWriter(outputStream, bamFile.toFile());
//
//                writer.setSortOrder(reader.getFileHeader().getSortOrder(), true);   //Must be called before calling setHeader()
//                writer.setHeader(reader.getFileHeader());
//                SAMRecordIterator iterator = reader.iterator();
//                while(iterator.hasNext()){
//                    writer.addAlignment(iterator.next());
//                }
//
//                writer.close();
//                reader.close();
//                break;
                throw new UnsupportedOperationException("Encryption not supported");
            }
            default: {
                if (copy) {
                    destFile = outdir.resolve(fileName);
                    Files.copy(bamFile, destFile);
                } else {
                    logger.info("copy = false. Don't copy file.");
                    destFile = bamFile;
                }
            }
        }
        long end = System.currentTimeMillis();
        logger.info("end - start = " + (end - start) / 1000.0 + "s");
        return destFile;
    }

    protected Path sortAlignmentsFile(Path input, Path outdir) throws IOException {
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
                sortBam = outdir.resolve(input.getFileName().toString() + ".sort.bam");
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


    protected AlignmentJsonDataReader getAlignmentJsonDataReader(URI input) throws IOException {
        if (!input.getScheme().equals("file")) {
            throw new IOException("URI is not a valid path");
        }

        String baseFileName = input.getPath();
        String alignmentFile = baseFileName;
        String headerFile;
        if(baseFileName.endsWith(".bam")){
            alignmentFile = baseFileName + (Paths.get(baseFileName + ".alignments.json").toFile().exists() ?
                    ".alignments.json" :
                    ".alignments.json.gz");
            headerFile = baseFileName + (Paths.get(baseFileName + ".header.json").toFile().exists() ?
                    ".header.json" :
                    ".header.json.gz");
        } else if(baseFileName.endsWith(".alignments.json")){
            headerFile = baseFileName.replaceFirst("alignments\\.json$", "header.json");
        } else if(baseFileName.endsWith(".alignments.json.gz")){
            headerFile = baseFileName.replaceFirst("alignments\\.json\\.gz$", "header.json.gz");
        } else {
            throw new IOException("Invalid input file : " + input.toString());
        }
        if (!Paths.get(alignmentFile).toFile().exists()) {
            throw new FileNotFoundException(alignmentFile);
        }
        if (!Paths.get(headerFile).toFile().exists()) {
            throw new FileNotFoundException(headerFile);
        }

        return new AlignmentJsonDataReader(alignmentFile, headerFile);
    }

    protected AlignmentCoverageJsonDataReader getAlignmentCoverageJsonDataReader(Path input) {
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

        return new AlignmentCoverageJsonDataReader(regionCoverageFile, meanCoverageFile);
    }

    /**
     * Check if the file is a sorted binary bam file.
     * @param is            Bam InputStream
     * @param bamFileName   Bam FileName
     * @throws IOException
     */
    protected void checkBamFile(InputStream is, String bamFileName) throws IOException {
        SAMFileReader reader = new SAMFileReader(is);
        boolean binary = reader.isBinary();
        SAMFileHeader.SortOrder sortOrder = reader.getFileHeader().getSortOrder();
        reader.close();

        if (!binary) {
            throw new IOException("Expected binary SAM file. File " + bamFileName + " is not binary.");
        }

        switch (sortOrder) {
            case coordinate:
                logger.debug("File {} sorted.", bamFileName);
                break;
            case queryname:
            case unsorted:
            default:
                throw new IOException("Expected sorted Bam file. " +
                        "File " + bamFileName + " is an unsorted bam (" + sortOrder.name() + ")");
        }
    }

    protected void checkUri(URI uri, String uriName) throws IOException {
        if(uri == null || uri.getScheme() != null && !uri.getScheme().equals("file")) {
            throw new IOException("Expected file:// uri scheme for " + uriName);
        }
    }

}
