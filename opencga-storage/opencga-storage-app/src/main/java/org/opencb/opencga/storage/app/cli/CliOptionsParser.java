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

package org.opencb.opencga.storage.app.cli;

import com.beust.jcommander.*;
import com.beust.jcommander.converters.CommaParameterSplitter;
import org.opencb.biodata.models.variant.VariantSource;
import org.opencb.biodata.models.variant.VariantStudy;

import java.util.*;

/**
 * Created by imedina on 02/03/15.
 */
public class CliOptionsParser {

    private final JCommander jcommander;

    private final GeneralOptions generalOptions;
    private final CommonCommandOptions commonCommandOptions;

    private final CreateAccessionsCommandOption createAccessionsCommandOption;

    private final IndexAlignmentsCommandOptions indexAlignmentsCommandOptions;
    private final IndexVariantsCommandOptions indexVariantsCommandOptions;
//    private final IndexSequenceCommandOptions indexSequenceCommandOptions;

    private final QueryAlignmentsCommandOptions queryAlignmentsCommandOptions;
    private final QueryVariantsCommandOptions queryVariantsCommandOptions;

    private final AnnotateVariantsCommandOptions annotateVariantsCommandOptions;
    private final StatsVariantsCommandOptions statsVariantsCommandOptions;


    public CliOptionsParser() {

        generalOptions = new GeneralOptions();

        jcommander = new JCommander(generalOptions);
        jcommander.setProgramName("opencga-storage2.sh");

        commonCommandOptions = new CommonCommandOptions();

        createAccessionsCommandOption = new CreateAccessionsCommandOption();
        indexAlignmentsCommandOptions = new IndexAlignmentsCommandOptions();
        indexVariantsCommandOptions = new IndexVariantsCommandOptions();
//        indexSequenceCommandOptions = new IndexSequenceCommandOptions();
        queryAlignmentsCommandOptions = new QueryAlignmentsCommandOptions();
        queryVariantsCommandOptions = new QueryVariantsCommandOptions();
        annotateVariantsCommandOptions = new AnnotateVariantsCommandOptions();
        statsVariantsCommandOptions = new StatsVariantsCommandOptions();

        jcommander.addCommand("create-accessions", createAccessionsCommandOption);
        jcommander.addCommand("index-alignments", indexAlignmentsCommandOptions);
        jcommander.addCommand("index-variants", indexVariantsCommandOptions);
//        jcommander.addCommand("index-sequence", indexSequenceCommandOptions);
        jcommander.addCommand("fetch-alignments", queryAlignmentsCommandOptions);
        jcommander.addCommand("fetch-variants", queryVariantsCommandOptions);
        jcommander.addCommand("annotate-variants", annotateVariantsCommandOptions);
        jcommander.addCommand("stats-variants", statsVariantsCommandOptions);
    }

    public void parse(String[] args) throws ParameterException {
        jcommander.parse(args);
    }

    public String getCommand() {
        return (jcommander.getParsedCommand() != null) ? jcommander.getParsedCommand(): "";
    }

    public boolean isHelp() {
        String parsedCommand = jcommander.getParsedCommand();
        if (parsedCommand != null) {
            JCommander jCommander = jcommander.getCommands().get(parsedCommand);
            List<Object> objects = jCommander.getObjects();
            if (!objects.isEmpty() && objects.get(0) instanceof CommonCommandOptions) {
                return ((CommonCommandOptions) objects.get(0)).help;
            }
        }
        return getCommonCommandOptions().help;
    }

    public void printUsage(){
        if(getCommand().isEmpty()) {
            jcommander.usage();
        } else {
            jcommander.usage(getCommand());
        }
    }

    @Deprecated
    public String usage() {
        StringBuilder builder = new StringBuilder();
        String parsedCommand = jcommander.getParsedCommand();
        if(parsedCommand != null && !parsedCommand.isEmpty()){
            jcommander.usage(parsedCommand, builder);
        } else {
            jcommander.usage(builder);
        }
        return builder.toString();//.replaceAll("\\^.*Default: false\\$\n", "");
    }

    public class GeneralOptions {

        @Parameter(names = {"-h", "--help"}, help = true)
        public boolean help;

        @Parameter(names = {"--version"})
        public boolean version;

    }

    public class CommonCommandOptions {

        @Parameter(names = { "-h", "--help" }, description = "Print this help", help = true)
        public boolean help;

        @Parameter(names = {"-L", "--log-level"}, description = "One of the following: 'error', 'warn', 'info', 'debug', 'trace'")
        public String logLevel = "info";

        @Deprecated
        @Parameter(names = {"-v", "--verbose"}, description = "Increase the verbosity of logs")
        public boolean verbose = false;

        @Parameter(names = {"-C", "--conf" }, description = "Properties path")
        public String configFile;

        @Parameter(names = {"--storage-engine"}, arity = 1, description = "One of the listed ones in storage-configuration.yml")
        public String storageEngine;

        @DynamicParameter(names = "-D", description = "Storage engine specific parameters go here comma separated, ie. -Dmongodb.compression=snappy", hidden = false)
        public Map<String, String> params = new HashMap<>(); //Dynamic parameters must be initialized

        @Deprecated
        @Parameter(names = { "--sm-name" }, description = "StorageManager class name (Must be in the classpath).")
        public String storageManagerName;

        @Deprecated
        @Parameter(names = {"--storage-engine-config"}, arity = 1, description = "Path of the file with options to overwrite storage-<Plugin>.properties")
        public String storageEngineConfigFile;

    }


    @Parameters(commandNames = {"create-accessions"}, commandDescription = "Creates accession IDs for an input file")
    public class CreateAccessionsCommandOption extends CommonCommandOptions {

        @Parameter(names = {"-i", "--input"}, description = "File to annotation with accession IDs", required = true, arity = 1)
        public String input;

        @Parameter(names = {"-o", "--outdir"}, description = "Directory where the output file will be saved", arity = 1)
        public String outdir;

        @Parameter(names = {"-p", "--prefix"}, description = "Accession IDs prefix", arity = 1)
        public String prefix;

        @Parameter(names = {"-s", "--study-alias"}, description = "Unique ID for the study where the file is classified (used for prefixes)",
                required = true, arity = 1)//, validateValueWith = StudyIdValidator.class)
        public String studyId;

        @Parameter(names = {"-r", "--resume-from-accession"}, description = "Starting point to generate accessions (will not be included)", arity = 1)
        public String resumeFromAccession;

//        class StudyIdValidator implements IValueValidator<String> {
//            @Override
//            public void validate(String name, String value) throws ParameterException {
//                if (value.length() < 6) {
//                    throw new ParameterException("The study ID must be at least 6 characters long");
//                }
//            }
//        }
    }


    class IndexCommandOptions extends CommonCommandOptions {

        @Parameter(names = {"-i", "--input"}, description = "File to index in the selected backend", required = true, arity = 1)
        public String input;

        @Parameter(names = {"-o", "--outdir"}, description = "Directory where output files will be saved (optional)", arity = 1, required = false)
        public String outdir;

        @Parameter(names = {"--file-id"}, description = "Unique ID for the file", required = true, arity = 1)
        public String fileId;

        @Parameter(names = {"--transform"}, description = "If present it only runs the transform stage, no load is executed")
        boolean transform = false;

        @Parameter(names = {"--load"}, description = "If present only the load stage is executed, transformation is skipped")
        boolean load = false;

        @Deprecated
        @Parameter(names = {"-c", "--credentials"}, description = "Path to the file where the backend credentials are stored", required = false, arity = 1)
        public String credentials;

        @Parameter(names = {"-d", "--database"}, description = "DataBase name to load the data", required = false, arity = 1)
        public String dbName;

        @Parameter(names = {"--study-configuration-file"}, description = "File with the study configuration. org.opencb.opencga.storage.core.StudyConfiguration", required = false, arity = 1)
        String studyConfigurationFile;

    }

    @Parameters(commandNames = {"index-alignments"}, commandDescription = "Index alignment file")
    public class IndexAlignmentsCommandOptions extends IndexCommandOptions {

        @Parameter(names = "--calculate-coverage", description = "Calculate coverage while indexing")
        public boolean calculateCoverage = true;

        @Parameter(names = "--mean-coverage", description = "Specify the chunk sizes to calculate average coverage. Only works if flag \"--calculate-coverage\" is also given. Please specify chunksizes as CSV: --mean-coverage 200,400", required = false)
        public List<String> meanCoverage;
    }

    @Parameters(commandNames = {"index-variants"}, commandDescription = "Index variants file")
    public class IndexVariantsCommandOptions extends IndexCommandOptions {

        @Parameter(names = {"--study-name"}, description = "Full name of the study where the file is classified", required = false, arity = 1)
        public String study;

        @Parameter(names = {"-s", "--study-id"}, description = "Unique ID for the study where the file is classified", required = true, arity = 1)
        public String studyId;

        @Parameter(names = {"-p", "--pedigree"}, description = "File containing pedigree information (in PED format, optional)", arity = 1)
        public String pedigree;

        @Parameter(names = {"--sample-ids"}, description = "CSV list of sampleIds. <sampleName>:<sampleId>[,<sampleName>:<sampleId>]*")
        public List<String> sampleIds;

        @Deprecated
        @Parameter(names = {"--include-stats"}, description = "Save statistics information available on the input file")
        public boolean includeStats = false;

        @Parameter(names = {"--include-genotypes"}, description = "Index including the genotypes")
        public boolean includeGenotype = false;

        @Parameter(names = {"--compress-genotypes"}, description = "Store genotypes as lists of samples")
        public boolean compressGenotypes = false;

        @Deprecated
        @Parameter(names = {"--include-src"}, description = "Store also the source vcf row of each variant")
        public boolean includeSrc = false;

        @Parameter(names = {"--aggregated"}, description = "Aggregated VCF File: basic, EVS or ExAC", arity = 1)
        public VariantSource.Aggregation aggregated = VariantSource.Aggregation.NONE;

        @Parameter(names = {"-t", "--study-type"}, description = "One of the following: FAMILY, TRIO, CONTROL, CASE, CASE_CONTROL, PAIRED, PAIRED_TUMOR, COLLECTION, TIME_SERIES", arity = 1)
        public VariantStudy.StudyType studyType = VariantStudy.StudyType.CASE_CONTROL;

        @Parameter(names = {"--gvcf"}, description = "[PENDING] The input file is in gvcf format")
        public boolean gvcf = false;

        @Parameter(names = {"--bgzip"}, description = "[PENDING] The input file is in bgzip format")
        public boolean bgzip = false;

        @Parameter(names = {"--calculate-stats"}, description = "Calculate statistics information over de indexed variants after the load step (optional)")
        public boolean calculateStats = false;

        @Parameter(names = {"--annotate"}, description = "Annotate indexed variants after the load step (optional)")
        public boolean annotate = false;

        @Parameter(names = {"--annotator"}, description = "Annotation source {cellbase_rest, cellbase_db_adaptor}")
        public org.opencb.opencga.storage.core.variant.annotation.VariantAnnotationManager.AnnotationSource annotator = null;

        @Parameter(names = {"--overwrite-annotations"}, description = "Overwrite annotations in variants already present")
        public boolean overwriteAnnotations = false;

        @Parameter(names = {"--annotator-config"}, description = "Path to the file with the configuration of the annotator")
        public String annotatorConfigFile;
    }



    class QueryCommandOptions extends CommonCommandOptions {

        @Parameter(names = {"-b", "--backend"}, description = "StorageManager plugin used to index files into: mongodb (default), hbase (pending)", required = false, arity = 1)
        public String backend = "mongodb";

        @Parameter(names = {"-d", "--database"}, description = "DataBase name", required = false, arity = 1)
        public String dbName;

        @Deprecated
        @Parameter(names = {"-c", "--credentials"}, description = "Path to the file where the backend credentials are stored", required = false, arity = 1)
        public String credentials;

        @Parameter(names = {"-r","--region"}, description = " [CSV]", required = false)
        public List<String> regions = new LinkedList<>();

        @Parameter(names = {"--region-gff-file"}, description = "", required = false)
        public String gffFile;

        @Parameter(names = {"-o", "--output"}, description = "Output file. Default: stdout", required = false, arity = 1)
        public String output;

        @Parameter(names = {"--output-format"}, description = "Output format: vcf(default), vcf.gz, json, json.gz", required = false, arity = 1)
        public String outputFormat = "vcf";

    }

    @Parameters(commandNames = {"fetch-alignments"}, commandDescription = "Search over indexed alignments")
    public class QueryAlignmentsCommandOptions extends QueryCommandOptions {

        //Filter parameters
        @Parameter(names = {"-a", "--alias"}, description = "File unique ID.", required = false, arity = 1)
        public String fileId;

        @Parameter(names = {"--file-path"}, description = "", required = false, arity = 1)
        public String filePath;

        @Parameter(names = {"--include-coverage"}, description = " [CSV]", required = false)
        public boolean coverage = false;

        @Parameter(names = {"-H", "--histogram"}, description = " ", required = false, arity = 1)
        public boolean histogram = false;

        @Parameter(names = {"--view-as-pairs"}, description = " ", required = false)
        public boolean asPairs;

        @Parameter(names = {"--process-differences"}, description = " ", required = false)
        public boolean processDifferences;

        @Parameter(names = {"-S","--stats-filter"}, description = " [CSV]", required = false)
        public List<String> stats = new LinkedList<>();

    }

    @Parameters(commandNames = {"fetch-variants"}, commandDescription = "Search over indexed variants")
    public class QueryVariantsCommandOptions extends QueryCommandOptions {

        @Parameter(names = {"--study-alias"}, description = " [CSV]", required = false)
        public String studyAlias;

        @Parameter(names = {"-a", "--alias"}, description = "File unique ID. [CSV]", required = false, arity = 1)
        public String fileId;

        @Parameter(names = {"-e", "--effect"}, description = " [CSV]", required = false, arity = 1)
        public String effect;

        @Parameter(names = {"--id"}, description = " [CSV]", required = false)
        public String id;

        @Parameter(names = {"-t", "--type"}, description = " [CSV]", required = false)
        public String type;

        @Parameter(names = {"-g", "--gene"}, description = " [CSV]", required = false)
        public String gene;

        @Parameter(names = {"--reference"}, description = " [CSV]", required = false)
        public String reference;

        @Parameter(names = {"-S","--stats-filter"}, description = " [CSV]", required = false)
        public List<String> stats = new LinkedList<>();

        @Parameter(names = {"--annot-filter"}, description = " [CSV]", required = false)
        public List<String> annot = new LinkedList<>();

    }



    @Parameters(commandNames = {"annotate-variants"}, commandDescription = "Create and load variant annotations into the database")
    public class AnnotateVariantsCommandOptions extends CommonCommandOptions {

        @Parameter(names = {"--create"}, description = "Run only the creation of the annotations to a file (specified by --output-filename)")
        public boolean create = false;

        @Parameter(names = {"--load"}, description = "Run only the load of the annotations into the DB from FILE")
        public boolean load = false;

        @Parameter(names = {"--annotator"}, description = "Annotation source {cellbase_rest, cellbase_db_adaptor}")
        public org.opencb.opencga.storage.core.variant.annotation.VariantAnnotationManager.AnnotationSource annotator;

        @Parameter(names = {"--overwrite-annotations"}, description = "Overwrite annotations in variants already present")
        public boolean overwriteAnnotations = false;

        @Parameter(names = {"--annotator-config"}, description = "Path to the file with the configuration of the annotator")
        public String annotatorConfig;

        @Parameter(names = {"-d", "--database"}, description = "DataBase name", required = false, arity = 1)
        public String dbName;

        @Deprecated
        @Parameter(names = {"-c", "--credentials"}, description = "Path to the file where the backend credentials are stored", required = false, arity = 1)
        public String credentials;

        @Parameter(names = {"--output-filename"}, description = "Output file name. Default: dbName", required = false, arity = 1)
        public String fileName;

        @Parameter(names = {"-o", "--outdir"}, description = "Output directory.", required = false, arity = 1)
        public String outdir;

        @Parameter(names = {"--species"}, description = "Species. Default hsapiens", required = false, arity = 1)
        public String species = "hsapiens";

        @Parameter(names = {"--assembly"}, description = "Assembly. Default GRc37", required = false, arity = 1)
        public String assembly = "GRc37";

        @Parameter(names = {"--filter-region"}, description = "Comma separated region filters", splitter = CommaParameterSplitter.class)
        public List<String> filterRegion;

        @Parameter(names = {"--filter-chromosome"}, description = "Comma separated chromosome filters", splitter = CommaParameterSplitter.class)
        public List<String> filterChromosome;

        @Parameter(names = {"--filter-gene"}, description = "Comma separated gene filters", splitter = CommaParameterSplitter.class)
        public String filterGene;

        @Parameter(names = {"--filter-annot-consequence-type"}, description = "Comma separated annotation consequence type filters", splitter = CommaParameterSplitter.class)
        public List filterAnnotConsequenceType = null; // TODO will receive CSV, only available when create annotations

    }



    @Parameters(commandNames = {"stats-variants"}, commandDescription = "Create and load stats into a database.")
    public class StatsVariantsCommandOptions extends CommonCommandOptions {

        @Parameter(names = {"--create"}, description = "Run only the creation of the stats to a file")
        public boolean create = false;

        @Parameter(names = {"--load"}, description = "Load the stats from an already existing FILE directly into the database. FILE is a prefix with structure <INPUT_FILENAME>.<TIME>")
        public boolean load = false;

        @Parameter(names = {"--overwrite-stats"}, description = "[PENDING] Overwrite stats in variants already present")
        public boolean overwriteStats = false;

        @Parameter(names = {"-s", "--study-id"}, description = "Unique ID for the study where the file is classified", required = true, arity = 1)
        public String studyId;

        @Parameter(names = {"-f", "--file-id"}, description = "Unique ID for the file", required = true, arity = 1)
        public String fileId;

        @Parameter(names = {"-d", "--database"}, description = "DataBase name", required = false, arity = 1)
        public String dbName;

        @Deprecated
        @Parameter(names = {"-c", "--credentials"}, description = "Path to the file where the backend credentials are stored", required = false, arity = 1)
        public String credentials;

        @Parameter(names = {"--output-filename"}, description = "Output file name. Default: database name", required = false, arity = 1)
        public String fileName;

        @Parameter(names = {"-o", "--outdir"}, description = "Output directory.", required = false, arity = 1)
        public String outdir = ".";

/* TODO: filters?
        @Parameter(names = {"--filter-region"}, description = "Comma separated region filters", splitter = CommaParameterSplitter.class)
        List<String> filterRegion = null;

        @Parameter(names = {"--filter-chromosome"}, description = "Comma separated chromosome filters", splitter = CommaParameterSplitter.class)
        List<String> filterChromosome = null;

        @Parameter(names = {"--filter-gene"}, description = "Comma separated gene filters", splitter = CommaParameterSplitter.class)
        String filterGene = null;

        @Parameter(names = {"--filter-annot-consequence-type"}, description = "Comma separated annotation consequence type filters", splitter = CommaParameterSplitter.class)
        List filterAnnotConsequenceType = null; // TODO will receive CSV, only available when create annotations
        */
    }


    public GeneralOptions getGeneralOptions() {
        return generalOptions;
    }

    public CommonCommandOptions getCommonCommandOptions() {
        return commonCommandOptions;
    }

    public CreateAccessionsCommandOption getCreateAccessionsCommandOption() {
        return createAccessionsCommandOption;
    }

    public IndexVariantsCommandOptions getIndexVariantsCommandOptions() {
        return indexVariantsCommandOptions;
    }

    public IndexAlignmentsCommandOptions getIndexAlignmentsCommandOptions() {
        return indexAlignmentsCommandOptions;
    }

//    IndexSequenceCommandOptions getIndexSequenceCommandOptions() {
//        return indexSequenceCommandOptions;
//    }

    public QueryVariantsCommandOptions getQueryVariantsCommandOptions() {
        return queryVariantsCommandOptions;
    }

    public QueryAlignmentsCommandOptions getQueryAlignmentsCommandOptions() {
        return queryAlignmentsCommandOptions;
    }

    public AnnotateVariantsCommandOptions getAnnotateVariantsCommandOptions() {
        return annotateVariantsCommandOptions;
    }
    public StatsVariantsCommandOptions getStatsVariantsCommandOptions() {
        return statsVariantsCommandOptions;
    }

}
