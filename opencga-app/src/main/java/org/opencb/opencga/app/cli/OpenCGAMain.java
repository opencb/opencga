package org.opencb.opencga.app.cli;


import com.beust.jcommander.ParameterException;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.Properties;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.apache.hadoop.conf.Configuration;
import org.opencb.biodata.formats.alignment.io.AlignmentDataReader;
import org.opencb.biodata.formats.alignment.io.AlignmentRegionDataReader;
import org.opencb.biodata.formats.alignment.io.AlignmentRegionDataWriter;
import org.opencb.biodata.formats.alignment.sam.io.AlignmentBamDataReader;
import org.opencb.biodata.formats.alignment.sam.io.AlignmentBamDataWriter;
import org.opencb.biodata.formats.alignment.sam.io.AlignmentSamDataReader;
import org.opencb.biodata.formats.alignment.sam.io.AlignmentSamDataWriter;
import org.opencb.biodata.formats.pedigree.io.PedigreePedReader;
import org.opencb.biodata.formats.pedigree.io.PedigreeReader;
import org.opencb.biodata.formats.variant.io.VariantReader;
import org.opencb.biodata.formats.variant.io.VariantWriter;
import org.opencb.biodata.formats.variant.vcf4.io.VariantVcfReader;
import org.opencb.biodata.models.alignment.AlignmentRegion;
import org.opencb.biodata.models.feature.Region;
import org.opencb.biodata.models.variant.Variant;
import org.opencb.biodata.models.variant.VariantSource;
import org.opencb.commons.containers.list.SortedList;
import org.opencb.commons.containers.map.QueryOptions;
import org.opencb.commons.io.DataWriter;
import org.opencb.commons.run.Runner;
import org.opencb.commons.run.Task;
import org.opencb.opencga.app.cli.OptionsParser.Command;
import org.opencb.opencga.app.cli.OptionsParser.CommandDownloadAlignments;
import org.opencb.opencga.app.cli.OptionsParser.CommandLoadAlignments;
import org.opencb.opencga.app.cli.OptionsParser.CommandLoadVariants;
import org.opencb.opencga.app.cli.OptionsParser.CommandTransformAlignments;
import org.opencb.opencga.app.cli.OptionsParser.CommandTransformVariants;
import org.opencb.opencga.lib.auth.*;
import org.opencb.opencga.storage.alignment.hbase.AlignmentHBaseDataReader;
import org.opencb.opencga.storage.alignment.hbase.AlignmentRegionCoverageHBaseDataWriter;
import org.opencb.opencga.storage.alignment.hbase.AlignmentRegionHBaseDataWriter;
import org.opencb.opencga.storage.alignment.json.AlignmentCoverageJsonDataWriter;
import org.opencb.opencga.storage.alignment.json.AlignmentJsonDataReader;
import org.opencb.opencga.storage.alignment.json.AlignmentJsonDataWriter;
import org.opencb.opencga.storage.alignment.tasks.AlignmentRegionCompactorTask;
import org.opencb.opencga.storage.alignment.tasks.AlignmentRegionCoverageCalculatorTask;
import org.opencb.opencga.storage.alignment.tasks.AlignmentRegionCoverageFromJsonTask;
import org.opencb.opencga.storage.variant.json.VariantJsonReader;
import org.opencb.opencga.storage.variant.json.VariantJsonWriter;
import org.opencb.opencga.storage.variant.mongodb.VariantMongoWriter;
import org.opencb.variant.lib.runners.VariantRunner;
import org.opencb.variant.lib.runners.tasks.VariantEffectTask;
import org.opencb.variant.lib.runners.tasks.VariantStatsTask;

/**
 * @author Cristina Yenyxe Gonzalez Garcia
 */
public class OpenCGAMain {
    
    private static final String[] transformAlignments = {
        "transform-alignments",
//        "-i", "/tmp/small.sam",
        "-i", "/tmp/HG00096.chrom20.ILLUMINA.bwa.GBR.low_coverage.20120522.bam",
//        "-i", "/tmp/NA06984.chrom20.ILLUMINA.bwa.CEU.low_coverage.20120522.bam",
//        "-a", "miAlignment",
        "-a", "HG00096",
        "-s", "pfc",
 //       "--study-alias", "pfc",
        "-o", "/tmp/",
     //   "--plain",
        "--include-coverage",
        "--mean-coverage", "1K",
        "--mean-coverage", "10K",
        "--mean-coverage", "1M"
    };
    private static final String[] loadAlignments = {
        "load-alignments",
        "-i", "/tmp/HG00096.alignments.json.gz",
     //   "-s", "pfc",
     //   "--study-alias", "PFC",
        "-b", "hbase",
        "-c", "/tmp/opencga.properties",
        //"--plain",
        "--include-coverage"
    };
    private static final String[] downloadAlignments = {
        "download-alignments",
        "-a", "HG00096",
        "-s", "pfc",
    //    "--study-alias", "PFC",
    //   "-b", "hbase",
        "-c", "/tmp/opencga.properties",
        "-o", "/tmp/ddd/",
        "--region", "20:62094-63094"
        //"--plain",
        //"--include-coverage"
    };
    
    
    public static void main(String[] args) throws IOException, InterruptedException, IllegalOpenCGACredentialsException {

//        if(args.length == 0){
//            System.out.println("Using test options");
//            args = downloadAlignments;
//        }
        OptionsParser parser = new OptionsParser();
        if (args.length == 0 || args.length > 0 && (args[0].equals("-h") || args[0].equals("--help"))) {
            System.out.println(parser.usage());
            return;
        }
        
        Command command = null;
        try {
            switch (parser.parse(args)) {
                case "load-variants":
                    command = parser.getLoadVariants();
                    break;
                case "transform-variants":
                    command = parser.getTransformVariants();
                    break;
                case "transform-alignments":
                    command = parser.getTransformAlignments();
                    break;
                case "load-alignments":
                    command = parser.getLoadAlignments();
                    break;
                case "download-alignments":
                    command = parser.getDownloadAlignments();
                    break;
                default:
                    System.out.println("Command not implemented");
                    System.exit(1);
            }
        } catch (ParameterException ex) {
            Logger.getLogger(OpenCGAMain.class.getName()).log(Level.SEVERE, ex.getMessage(), ex);
            System.out.println(parser.usage());
            System.exit(1);
        }
        
        if (command instanceof CommandLoadVariants) {
            CommandLoadVariants c = (CommandLoadVariants) command;
            
            Path variantsPath = Paths.get(c.input + ".variants.json.gz");
            Path filePath = Paths.get(c.input + ".file.json.gz");

            VariantSource source = new VariantSource(variantsPath.getFileName().toString(), null, null, null);
            indexVariants("load", source, variantsPath, filePath, null, c.backend, Paths.get(c.credentials), c.includeEffect, c.includeStats, c.includeSamples);
            
        } else if (command instanceof CommandTransformVariants) {
            CommandTransformVariants c = (CommandTransformVariants) command;
            
            Path variantsPath = Paths.get(c.file);
            Path pedigreePath = c.pedigree != null ? Paths.get(c.pedigree) : null;
            Path outdir = c.outdir != null ? Paths.get(c.outdir) : null;

            VariantSource source = new VariantSource(variantsPath.getFileName().toString(), c.fileId, c.studyId, c.study);
            indexVariants("transform", source, variantsPath, pedigreePath, outdir, "json", null, c.includeEffect, c.includeStats, c.includeSamples);
        
        } else if (command instanceof CommandTransformAlignments) {
            CommandTransformAlignments c = (CommandTransformAlignments) command;
            
            Path filePath = Paths.get(c.file);
            Path outdir = c.outdir != null ? Paths.get(c.outdir) : null;
            String backend = c.plain ? "json" : "json.gz";
            
            if (!filePath.toFile().exists()) {
                throw new IOException("[Error] File not found : " + c.file);
            }
            
            indexAlignments(
                    c.study, //c.studyId,
                    filePath,
                    c.fileId, outdir, null,
                    backend, null, null,     //Credentials not needed
                    true,
                    false,              //Can't be loaded
                    c.includeCoverage, c.meanCoverage);
        
        } else if (command instanceof CommandLoadAlignments){
            CommandLoadAlignments c = (CommandLoadAlignments) command;
            
            //Path filePath = Paths.get(c.dir, c.input + ".alignment" + (c.plain ? ".json" : ".json.gz"));
            Path filePath = Paths.get(c.input);
            Path credentialsPath = Paths.get(c.credentials);
            
            Configuration config = null;
//            config = HBaseConfiguration.create();
//            config.set("hbase.zookeeper.quorum", "mem10,mem09");
//            config.set("hbase.zookeeper.property.clientPort", "2181");
//            config.set("zookeeper.znode.parent", "/hbase-unsecure");
            
            if (!filePath.toFile().exists()) {
                throw new IOException("[Error] Input paile not found : " + c.input);
            }
            if (!credentialsPath.toFile().exists()) {
                throw new IOException("[Error] Credentials file not found : " + c.credentials);
            }
            
            indexAlignments(
                    null, //c.study, //c.studyId,
                    filePath,
                    null, null, null,         //No output file
                    c.backend, credentialsPath, config,
                    false,
                    c.includeCoverage, 
                    false, null);       //Coverage calculation is in transform, not in load.
            
            
        } else if(command instanceof CommandDownloadAlignments){
            CommandDownloadAlignments c = (CommandDownloadAlignments) command;
            downloadAlignments(c);
//            Path credentialsPath = Paths.get(c.credentials);
//            Path outdir = c.outdir != null ? Paths.get(c.outdir) : null;
//            Region region = new Region(c.region);
//            
//            if (!credentialsPath.toFile().exists()) {
//                throw new IOException("[Error] Credentials file not found : " + c.credentials);
//            }
//            
//            indexAlignments(
//                    null, //c.study, //c.studyId,
//                    null,
//                    c.fileId + ".hbase", outdir, region, //No output file
//                    c.format, credentialsPath, null,
//                    false,
//                    false,
//                    false, null);       //Coverage calculation is in transform, not in load.
        }
    }
    
    private static void indexVariants(String step, VariantSource source, Path mainFilePath, Path auxiliaryFilePath, Path outdir, String backend, 
                                      Path credentialsPath, boolean includeEffect, boolean includeStats, boolean includeSamples) 
            throws IOException, IllegalOpenCGACredentialsException {

        VariantReader reader;
        PedigreeReader pedReader = ("transform".equals(step) && auxiliaryFilePath != null) ? 
                new PedigreePedReader(auxiliaryFilePath.toString()) : null;

        if (source.getFileName().endsWith(".vcf") || source.getFileName().endsWith(".vcf.gz")) {
            reader = new VariantVcfReader(source, mainFilePath.toAbsolutePath().toString());
        } else if (source.getFileName().endsWith(".json") || source.getFileName().endsWith(".json.gz")) {
            assert(auxiliaryFilePath != null);
            reader = new VariantJsonReader(source, mainFilePath.toAbsolutePath().toString(), auxiliaryFilePath.toAbsolutePath().toString());
        } else {
            throw new IOException("Variants input file format not supported");
        }

        List<VariantWriter> writers = new ArrayList<>();
//        List<VariantAnnotator> annots = new ArrayList<>();
//        annots.add(new VariantControlMongoAnnotator());

        List<Task<Variant>> taskList = new SortedList<>();

        // TODO Restore when SQLite and Monbase are once again ready!!
        if (backend.equalsIgnoreCase("mongo")) {
            Properties properties = new Properties();
            properties.load(new InputStreamReader(new FileInputStream(credentialsPath.toString())));
            OpenCGACredentials credentials = new MongoCredentials(properties);
            writers.add(new VariantMongoWriter(source, (MongoCredentials) credentials, 
                    properties.getProperty("collection_variants", "variants"),
                    properties.getProperty("collection_files", "files")));
        } else if (backend.equalsIgnoreCase("json")) {
//            credentials = new MongoCredentials(properties);
            writers.add(new VariantJsonWriter(source, outdir));
        }/* else if (backend.equalsIgnoreCase("sqlite")) {
            credentials = new SqliteCredentials(properties);
            writers.add(new VariantVcfSqliteWriter((SqliteCredentials) credentials));
        } else if (backend.equalsIgnoreCase("monbase")) {
            credentials = new MonbaseCredentials(properties);
            writers.add(new VariantVcfMonbaseDataWriter(source, "opencga-hsapiens", (MonbaseCredentials) credentials));// TODO Restore when SQLite and Monbase are once again ready!!
        } */ 


        // If a JSON file is provided, then stats and effects do not need to be recalculated
        if (!source.getFileName().endsWith(".json") && !source.getFileName().endsWith(".json.gz")) {
            if (includeEffect) {
                taskList.add(new VariantEffectTask());
            }

            if (includeStats) {
                taskList.add(new VariantStatsTask(reader, source));
            }
        }

        for (VariantWriter variantWriter : writers) {
            variantWriter.includeSamples(includeSamples);
            variantWriter.includeEffect(includeEffect);
            variantWriter.includeStats(includeStats);
        }

        VariantRunner vr = new VariantRunner(source, reader, pedReader, writers, taskList);

        System.out.println("Indexing variants...");
        vr.run();
        System.out.println("Variants indexed!");
    }
    

    private static void indexAlignments(
            String study, //String studyId,
            Path filePath, 
            String fileId, Path outdir, Region region, 
            String backend, Path credentialsPath, Configuration config, 
            boolean compact,
            boolean loadCoverage,
            boolean calculeCoverage, List<String> meanCoverageValues) throws IOException {
        String TABLE_NAME = "alignments";
        AlignmentDataReader reader;
        //List<AlignmentDataWriter<Alignment, AlignmentHeader>> writers = new LinkedList<>();
        List<DataWriter<AlignmentRegion>> writers = new LinkedList<>();
        List<Task<AlignmentRegion>> tasks = new LinkedList<>();
        if(outdir == null) outdir = Paths.get(".");
        Properties properties = null;
        
        if(credentialsPath != null){
            properties = new Properties();
            properties.load(new InputStreamReader(new FileInputStream(credentialsPath.toString())));
        }
        /*
            Configure Readers
        */
        
        String filePathString = filePath.toAbsolutePath().toString();
        String lowerCaseFileName = filePath.getFileName().toString().toLowerCase();
        String baseFileName = filePathString.substring(0, filePathString.lastIndexOf("."));
        String extension = filePathString.substring(filePathString.lastIndexOf("."), filePathString.length());
        if (lowerCaseFileName.endsWith(".sam")) {
            reader = new AlignmentSamDataReader(filePathString, study);
        } else if (lowerCaseFileName.endsWith(".bam")) {
            reader = new AlignmentBamDataReader(filePathString, study);
        } else if (lowerCaseFileName.endsWith(".json")) {
//            String headerJsonFilename = filePathString.substring(0, filePathString.lastIndexOf(".", 2)) + ".header.json";
//            reader = new AlignmentJsonDataReader(filePathString, headerJsonFilename);
            baseFileName = baseFileName.substring(0, baseFileName.lastIndexOf("."));
            reader = new AlignmentJsonDataReader(baseFileName, false);
        } else if(lowerCaseFileName.endsWith(".json.gz")){
//            String headerJsonFilename = filePathString.substring(0, filePathString.lastIndexOf(".", 3)) + ".header.json.gz";
//            reader = new AlignmentJsonDataReader(filePathString, headerJsonFilename);
            baseFileName = baseFileName.substring(0, baseFileName.lastIndexOf("."));
            baseFileName = baseFileName.substring(0, baseFileName.lastIndexOf("."));
            extension = ".json.gz";
            reader = new AlignmentJsonDataReader(baseFileName, true);
        } else if(lowerCaseFileName.endsWith(".hbase")){
            AlignmentHBaseDataReader hbdr = new AlignmentHBaseDataReader(properties, TABLE_NAME,  baseFileName);
            reader = hbdr;
            if(region != null) {
                hbdr.setRegion(region);
            }
        } else {
            throw new IOException("Alignment input file format not supported : " + filePath);
        }
        
        
        /*
            Configure Backend
        */
        if(backend.equalsIgnoreCase("json") || backend.equalsIgnoreCase("json.gz")) {
            boolean gzip = backend.endsWith(".gz");
            String baseOutputFilename = Paths.get(outdir.toString(), fileId).toAbsolutePath().toString();
            writers.add(new AlignmentRegionDataWriter(new AlignmentJsonDataWriter(reader, baseOutputFilename, gzip)));
            if(calculeCoverage || loadCoverage){
                writers.add(new AlignmentCoverageJsonDataWriter(baseOutputFilename, gzip));
            }
        } else if (backend.equalsIgnoreCase("hbase")) {
            if (calculeCoverage || loadCoverage) {
                if (properties != null){
                    writers.add(new AlignmentRegionCoverageHBaseDataWriter(properties, TABLE_NAME , Paths.get(baseFileName).getFileName().toString()));
                }else{
                    writers.add(new AlignmentRegionCoverageHBaseDataWriter(config, TABLE_NAME, Paths.get(baseFileName).getFileName().toString()));
                }
            }
            if (properties != null){
                writers.add(new AlignmentRegionHBaseDataWriter( properties, TABLE_NAME, Paths.get(baseFileName).getFileName().toString(), reader));
            }else{
                writers.add(new AlignmentRegionHBaseDataWriter( config, TABLE_NAME, Paths.get(baseFileName).getFileName().toString(), reader));
            }
        } else if (backend.equalsIgnoreCase("sam")) {
            if(calculeCoverage || loadCoverage){
                throw new UnsupportedOperationException("Can't write coverage in this backend.");
            }
            System.out.println("[CAUTION] Unimplemented Extraction");
            if(fileId == null){
                fileId = "/tmp/unimplemented.sam";
            }
            writers.add(new AlignmentRegionDataWriter(new AlignmentSamDataWriter(fileId, reader)));
        } else {
            throw new IOException("Alignment backend format not supported : " + backend);
        }
        
        /*
         Configure Tasks
         */
        if(compact){
            QueryOptions queryOptions = new QueryOptions();
            AlignmentRegionCompactorTask alignmentRegionCompactorTask = new AlignmentRegionCompactorTask(queryOptions);
            tasks.add(alignmentRegionCompactorTask);
        }
        if(calculeCoverage){
            AlignmentRegionCoverageCalculatorTask coverageTask = new AlignmentRegionCoverageCalculatorTask();
            for(String name : meanCoverageValues){
                coverageTask.addMeanCoverageCalculator(name);
            }
            tasks.add(coverageTask);
        }
        if(loadCoverage) {
            String coverageJsonFilename;
            if (lowerCaseFileName.endsWith(".json")) {
                //coverageJsonFilename = filePathString.substring(0, filePathString.lastIndexOf(".", 2)) + ".coverage.json";
                coverageJsonFilename = baseFileName += ".coverage.json";
            } else if (lowerCaseFileName.endsWith(".json.gz")) {
                //coverageJsonFilename = filePathString.substring(0, filePathString.lastIndexOf(".", 2)) + ".coverage.json.gz";
                coverageJsonFilename = baseFileName += ".coverage.json.gz";
            } else {
                throw new UnsupportedOperationException("Coverage can be loaded only from Json");
            }
            AlignmentRegionCoverageFromJsonTask alignmentRegionCoverageFromJsonTask = new AlignmentRegionCoverageFromJsonTask(coverageJsonFilename);
            tasks.add(alignmentRegionCoverageFromJsonTask);
        }
        
        
        AlignmentRegionDataReader regionReader = new AlignmentRegionDataReader(reader);
        Runner<AlignmentRegion> runner = new Runner<>(regionReader, writers, tasks, 1);
        
        runner.run();
    }


    //TODO
    private static void transformAlignments(CommandTransformAlignments c) throws IOException{
        AlignmentDataReader reader;
        List<DataWriter<AlignmentRegion>> writers = new LinkedList<>();
        List<Task<AlignmentRegion>> tasks = new LinkedList<>();

        if(c.file.endsWith(".sam")){
            reader = new AlignmentSamDataReader(c.file, c.study);
        } else if (c.file.endsWith(".bam")) {
            reader = new AlignmentBamDataReader(c.file, c.study);
        } else {
            throw new UnsupportedOperationException("[ERROR] Unsuported file input format : " + c.file);
        }
        
        writers.add(new AlignmentRegionDataWriter(new AlignmentJsonDataWriter(reader, Paths.get(c.outdir,c.fileId).toString(), c.plain)));
        
        AlignmentRegionDataReader regionReader = new AlignmentRegionDataReader(reader);
        Runner<AlignmentRegion> runner = new Runner<>(regionReader, writers, tasks, 1);

        runner.run();
    }
    
    //TODO
    private static void loadAlignments(CommandLoadAlignments c) throws IOException  {
        AlignmentDataReader reader;
        Properties properties = null;
        if (c.credentials != null) {
            properties = new Properties();
            properties.load(new InputStreamReader(new FileInputStream(c.credentials)));
        }
        
        switch(c.backend){
            case "hbase" :
                
                break;
            default:
                throw new UnsupportedOperationException("[ERROR] Unsupported backend : " + c.backend);
        }
        
    }
    private static void downloadAlignments(CommandDownloadAlignments c) throws IOException {
        AlignmentDataReader reader;
        Properties properties = null;

        if (c.credentials != null) {
            properties = new Properties();
            properties.load(new InputStreamReader(new FileInputStream(c.credentials)));
        }
        switch(c.backend){
            case "hbase":
                AlignmentHBaseDataReader hb = new AlignmentHBaseDataReader(properties, "alignments", c.alias);
                if(c.region != null){
                    hb.setRegion(new Region(c.region));
                }
                reader = hb;
                break;
            default:
                throw new UnsupportedOperationException("[ERROR] Unsupported backend : " + c.backend);
        }
        List<DataWriter<AlignmentRegion>> writers = new LinkedList<>();
        boolean gzip = false;
        switch(c.format){
            case "json.gz":
                gzip = true;
            case "json":
                writers.add(new AlignmentRegionDataWriter(new AlignmentJsonDataWriter(reader, Paths.get(c.outdir, c.alias).toString(), gzip)));
                break;
            case "sam":
                writers.add(new AlignmentRegionDataWriter(new AlignmentSamDataWriter(Paths.get(c.outdir, c.alias).toString(), reader)));
                break;
            case "bam":
                writers.add(new AlignmentRegionDataWriter(new AlignmentBamDataWriter(Paths.get(c.outdir, c.alias).toString(), reader)));
                break;
            default:
                throw new UnsupportedOperationException("[ERROR] Unsupported format : " + c.format);
        }
        List<Task<AlignmentRegion>> tasks = new LinkedList<>();

        AlignmentRegionDataReader regionReader = new AlignmentRegionDataReader(reader);
        Runner<AlignmentRegion> runner = new Runner<>(regionReader, writers, tasks, 1);

        runner.run();
    }
    
}
