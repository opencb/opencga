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

import com.beust.jcommander.ParameterException;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.io.Files;
import org.opencb.biodata.formats.feature.gff.Gff;
import org.opencb.biodata.formats.feature.gff.io.GffReader;
import org.opencb.biodata.formats.io.FileFormatException;
import org.opencb.biodata.formats.variant.vcf4.VcfRecord;
import org.opencb.biodata.formats.variant.vcf4.io.VcfRawReader;
import org.opencb.biodata.formats.variant.vcf4.io.VcfRawWriter;
import org.opencb.biodata.models.feature.Region;
import org.opencb.biodata.models.variant.Variant;
import org.opencb.biodata.models.variant.VariantSource;
import org.opencb.biodata.models.variant.VariantSourceEntry;
import org.opencb.biodata.models.variant.stats.VariantStats;
import org.opencb.commons.io.DataWriter;
import org.opencb.commons.run.Runner;
import org.opencb.commons.run.Task;
import org.opencb.datastore.core.ObjectMap;
import org.opencb.datastore.core.Query;
import org.opencb.datastore.core.QueryOptions;
import org.opencb.datastore.core.QueryResult;
import org.opencb.opencga.core.common.Config;
import org.opencb.opencga.core.common.TimeUtils;
import org.opencb.opencga.core.tools.accession.CreateAccessionTask;
import org.opencb.opencga.storage.core.StorageManagerException;
import org.opencb.opencga.storage.core.StorageManagerFactory;
import org.opencb.opencga.storage.core.StudyConfiguration;
import org.opencb.opencga.storage.core.alignment.AlignmentStorageManager;
import org.opencb.opencga.storage.core.alignment.adaptors.AlignmentDBAdaptor;
import org.opencb.opencga.storage.core.sequence.SqliteSequenceDBAdaptor;
import org.opencb.opencga.storage.core.variant.FileStudyConfigurationManager;
import org.opencb.opencga.storage.core.variant.VariantStorageManager;
import org.opencb.opencga.storage.core.variant.adaptors.VariantDBAdaptor;
import org.opencb.opencga.storage.core.variant.annotation.*;
import org.opencb.opencga.storage.core.variant.annotation.VariantAnnotationManager;
import org.opencb.opencga.storage.core.variant.io.json.VariantSourceEntryJsonMixin;
import org.opencb.opencga.storage.core.variant.io.json.VariantSourceJsonMixin;
import org.opencb.opencga.storage.core.variant.io.json.VariantStatsJsonMixin;
import org.opencb.opencga.storage.core.variant.stats.VariantStatisticsManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.sql.SQLException;
import java.util.*;
import java.util.stream.Collectors;

/**
 * @author Cristina Yenyxe Gonzalez Garcia
 */
@Deprecated
public class OpenCGAStorageMain {

    //    private static final String OPENCGA_HOME = System.getenv("OPENCGA_HOME");
    private static String opencgaHome;
    public static final String OPENCGA_STORAGE_ANNOTATOR = "OPENCGA.STORAGE.ANNOTATOR";

    protected static Logger logger = null;// LoggerFactory.getLogger(OpenCGAStorageMain.class);
    private static OptionsParser parser;


    static {

    }

    public static void main(String[] args) {

        parser = new OptionsParser();
        OptionsParser.Command command = null;
        try {
            String parsedCommand = parser.parse(args);
            String logLevel = "info";
            if (parser.getGeneralParameters().verbose) {
                logLevel = "debug";
            }
            if (parser.getGeneralParameters().logLevel != null) {
                logLevel = parser.getGeneralParameters().logLevel;
            }
            setLogLevel(logLevel);
            Config.setOpenCGAHome();

            if(parser.getGeneralParameters().help || args.length == 0) {
                System.out.println(parser.usage());
                return;
            }
            if (parser.getGeneralParameters().version) {
                String version = Config.getStorageProperties().getProperty("OPENCGA.STORAGE.VERSION");
                printVersion(version);
                return;
            }
            command = parser.getCommand();
            if (command == null) {
                System.out.println("Command not implemented");
                System.exit(1);
            }
        } catch (ParameterException ex) {
            System.out.println(ex.getMessage());
            System.out.println(parser.usage());
            System.exit(1);
        }


        try {
            if (command instanceof OptionsParser.CommandIndexAlignments) {    //TODO: Create method AlignmentStorageManager.index() ??
                OptionsParser.CommandIndexAlignments c = (OptionsParser.CommandIndexAlignments) command;
                indexAlignments(c);

            } else if (command instanceof OptionsParser.CommandIndexSequence) {
                OptionsParser.CommandIndexSequence c = (OptionsParser.CommandIndexSequence) command;
                indexSequence(c);

            } else if (command instanceof OptionsParser.CommandIndexVariants) {
                OptionsParser.CommandIndexVariants c = (OptionsParser.CommandIndexVariants) command;
                indexVariants(c);

            } else if (command instanceof OptionsParser.CommandCreateAccessions) {
                OptionsParser.CommandCreateAccessions c = (OptionsParser.CommandCreateAccessions) command;

                Path variantsPath = Paths.get(c.input);
                Path outdir = c.outdir != null ? Paths.get(c.outdir) : null;

                VariantSource source = new VariantSource(variantsPath.getFileName().toString(), null, c.studyId, null);
                createAccessionIds(variantsPath, source, c.prefix, c.resumeFromAccession, outdir);

            } else if (command instanceof OptionsParser.CommandFetchVariants) {
                OptionsParser.CommandFetchVariants c = (OptionsParser.CommandFetchVariants) command;

                /**
                 * Open connection
                 */
                VariantStorageManager variantStorageManager = StorageManagerFactory.get().getVariantStorageManager(c.backend);
//                if (c.credentials != null && !c.credentials.isEmpty()) {
//                    variantStorageManager.addConfigUri(new URI(null, c.credentials, null));
//                }

                ObjectMap params = new ObjectMap();
                VariantDBAdaptor dbAdaptor = variantStorageManager.getDBAdaptor(c.dbName);

                /**
                 * Parse Regions
                 */
                List<Region> regions = null;
                GffReader gffReader = null;
                if (c.regions != null && !c.regions.isEmpty()) {
                    regions = new LinkedList<>();
                    for (String csvRegion : c.regions) {
                        for (String strRegion : csvRegion.split(",")) {
                            Region region = new Region(strRegion);
                            regions.add(region);
                            logger.info("Parsed region: {}", region);
                        }
                    }
                } else if (c.gffFile != null && !c.gffFile.isEmpty()) {
                    try {
                        gffReader = new GffReader(c.gffFile);
                    } catch (NoSuchMethodException e) {
                        e.printStackTrace();
                    }
//                throw new UnsupportedOperationException("Unsuppoted GFF file");
                }

                /**
                 * Parse QueryOptions
                 */
                QueryOptions options = new QueryOptions(new HashMap<>(c.params));

                if (c.studyAlias != null && !c.studyAlias.isEmpty()) {
                    options.add("studies", Arrays.asList(c.studyAlias.split(",")));
                }
                if (c.fileId != null && !c.fileId.isEmpty()) {
                    options.add("files", Arrays.asList(c.fileId.split(",")));
                }
                if (c.effect != null && !c.effect.isEmpty()) {
                    options.add("annot", Arrays.asList(c.effect.split(",")));
                }

                if (c.stats != null && !c.stats.isEmpty()) {
                    for (String csvStat : c.stats) {
                        for (String stat : csvStat.split(",")) {
                            int index = stat.indexOf("<");
                            index = index >= 0 ? index : stat.indexOf("!");
                            index = index >= 0 ? index : stat.indexOf("~");
                            index = index >= 0 ? index : stat.indexOf("<");
                            index = index >= 0 ? index : stat.indexOf(">");
                            index = index >= 0 ? index : stat.indexOf("=");
                            if (index < 0) {
                                throw new UnsupportedOperationException("Unknown stat filter operation: " + stat);
                            }
                            String name = stat.substring(0, index);
                            String cond = stat.substring(index);

//                        if("maf".equals(name) || "mgf".equals(name) || "missingAlleles".equals(name) || "missingGenotypes".equals(name)) {
                            if (name.matches("maf|mgf|missingAlleles|missingGenotypes")) {
                                options.put(name, cond);
                            } else {
                                throw new UnsupportedOperationException("Unknown stat filter name: " + name);
                            }
                            logger.info("Parsed stat filter: {} {}", name, cond);
                        }
                    }
                }
                if (c.id != null && !c.id.isEmpty()) {   //csv
                    options.add("id", c.id);
                }
                if (c.gene != null && !c.gene.isEmpty()) {   //csv
                    options.add("gene", c.gene);
                }
                if (c.type != null && !c.type.isEmpty()) {   //csv
                    options.add("type", c.type);
                }
                if (c.reference != null && !c.reference.isEmpty()) {   //csv
                    options.add("reference", c.reference);
                }


                /**
                 * Run query
                 */
                int subListSize = 20;
                logger.info("options = " + options.toJson());
                if (regions != null && !regions.isEmpty()) {
                    for(int i = 0; i < (regions.size()+subListSize-1)/subListSize; i++) {
                        List<Region> subRegions = regions.subList(
                                i * subListSize,
                                Math.min((i + 1) * subListSize, regions.size()));

                        logger.info("subRegions = " + subRegions);
//                    List<QueryResult<Variant>> queryResults = dbAdaptor.getAllVariants(subRegions, options);
                        List<QueryResult<Variant>> queryResults = dbAdaptor.getAllVariantsByRegionList(subRegions, options);
                        StringBuilder sb = new StringBuilder();
                        for (QueryResult<Variant> queryResult : queryResults) {
                            printQueryResult(queryResult, sb);
                        }
                        System.out.println(sb);
                    }
                } else if(gffReader != null) {
                    List<Gff> gffList;
                    List<Region> subRegions;
                    while((gffList = gffReader.read(subListSize)) != null) {
                        subRegions = new ArrayList<>(subListSize);
                        for (Gff gff : gffList) {
                            subRegions.add(new Region(gff.getSequenceName(), gff.getStart(), gff.getEnd()));
                        }
                        logger.info("subRegions = " + subRegions);
                        List<QueryResult<Variant>> queryResults = dbAdaptor.getAllVariantsByRegionList(subRegions, options);
                        StringBuilder sb = new StringBuilder();
                        for (QueryResult<Variant> queryResult : queryResults) {
                            printQueryResult(queryResult, sb);
                        }
                        System.out.println(sb);
                    }
                } else {
                    System.out.println(printQueryResult(dbAdaptor.getAllVariants(options), null));
                }


            } else if (command instanceof OptionsParser.CommandFetchAlignments) {
                OptionsParser.CommandFetchAlignments c = (OptionsParser.CommandFetchAlignments) command;

                /**
                 * Open connection
                 */
                AlignmentStorageManager alignmentStorageManager = StorageManagerFactory.get().getAlignmentStorageManager(c.backend);
//                if (c.credentials != null && !c.credentials.isEmpty()) {
//                    alignmentStorageManager.addConfigUri(new URI(null, c.credentials, null));
//                }

                ObjectMap params = new ObjectMap();
                AlignmentDBAdaptor dbAdaptor = alignmentStorageManager.getDBAdaptor(c.dbName);

                /**
                 * Parse Regions
                 */
                GffReader gffReader = null;
                List<Region> regions = null;
                if (c.regions != null && !c.regions.isEmpty()) {
                    regions = new LinkedList<>();
                    for (String csvRegion : c.regions) {
                        for (String strRegion : csvRegion.split(",")) {
                            Region region = new Region(strRegion);
                            regions.add(region);
                            logger.info("Parsed region: {}", region);
                        }
                    }
                } else if (c.gffFile != null && !c.gffFile.isEmpty()) {
                    try {
                        gffReader = new GffReader(c.gffFile);
                    } catch (NoSuchMethodException e) {
                        e.printStackTrace();
                    }
                    //throw new UnsupportedOperationException("Unsuppoted GFF file");
                }

                /**
                 * Parse QueryOptions
                 */
                QueryOptions options = new QueryOptions();

                if (c.fileId != null && !c.fileId.isEmpty()) {
                    options.add(AlignmentDBAdaptor.QO_FILE_ID, c.fileId);
                }
                options.add(AlignmentDBAdaptor.QO_INCLUDE_COVERAGE, c.coverage);
                options.add(AlignmentDBAdaptor.QO_VIEW_AS_PAIRS, c.asPairs);
                options.add(AlignmentDBAdaptor.QO_PROCESS_DIFFERENCES, c.processDifferences);
                if (c.histogram > 0) {
                    options.add(AlignmentDBAdaptor.QO_INCLUDE_COVERAGE, true);
                    options.add(AlignmentDBAdaptor.QO_HISTOGRAM, true);
                    options.add(AlignmentDBAdaptor.QO_INTERVAL_SIZE, c.histogram);
                }
                if (c.filePath != null && !c.filePath.isEmpty()) {
                    options.add(AlignmentDBAdaptor.QO_BAM_PATH, c.filePath);
                }


                if (c.stats != null && !c.stats.isEmpty()) {
                    for (String csvStat : c.stats) {
                        for (String stat : csvStat.split(",")) {
                            int index = stat.indexOf("<");
                            index = index >= 0 ? index : stat.indexOf("!");
                            index = index >= 0 ? index : stat.indexOf("~");
                            index = index >= 0 ? index : stat.indexOf("<");
                            index = index >= 0 ? index : stat.indexOf(">");
                            index = index >= 0 ? index : stat.indexOf("=");
                            if (index < 0) {
                                throw new UnsupportedOperationException("Unknown stat filter operation: " + stat);
                            }
                            String name = stat.substring(0, index);
                            String cond = stat.substring(index);

                            if (name.matches("")) {
                                options.put(name, cond);
                            } else {
                                throw new UnsupportedOperationException("Unknown stat filter name: " + name);
                            }
                            logger.info("Parsed stat filter: {} {}", name, cond);
                        }
                    }
                }


                /**
                 * Run query
                 */
                int subListSize = 20;
                logger.info("options = {}", options.toJson());
                if (c.histogram > 0) {
                    for (Region region : regions) {
                        System.out.println(dbAdaptor.getAllIntervalFrequencies(region, options));
                    }
                } else if (regions != null && !regions.isEmpty()) {
                    for (int i = 0; i < (regions.size() + subListSize - 1) / subListSize; i++) {
                        List<Region> subRegions = regions.subList(
                                i * subListSize,
                                Math.min((i + 1) * subListSize, regions.size()));

                        logger.info("subRegions = " + subRegions);
                        QueryResult queryResult = dbAdaptor.getAllAlignmentsByRegion(subRegions, options);
                        logger.info("{}", queryResult);
                        System.out.println(new ObjectMap("queryResult", queryResult).toJson());
                    }
                } else if (gffReader != null) {
                    List<Gff> gffList;
                    List<Region> subRegions;
                    while ((gffList = gffReader.read(subListSize)) != null) {
                        subRegions = new ArrayList<>(subListSize);
                        for (Gff gff : gffList) {
                            subRegions.add(new Region(gff.getSequenceName(), gff.getStart(), gff.getEnd()));
                        }

                        logger.info("subRegions = " + subRegions);
                        QueryResult queryResult = dbAdaptor.getAllAlignmentsByRegion(subRegions, options);
                        logger.info("{}", queryResult);
                        System.out.println(new ObjectMap("queryResult", queryResult).toJson());
                    }
                } else {
                    throw new UnsupportedOperationException("Unable to fetch over all the genome");
//                System.out.println(dbAdaptor.getAllAlignments(options));
                }
            } else if (command instanceof OptionsParser.CommandAnnotateVariants) {
                OptionsParser.CommandAnnotateVariants c = (OptionsParser.CommandAnnotateVariants) command;
                annotateVariants(c);
            } else if (command instanceof OptionsParser.CommandStatsVariants) {
                OptionsParser.CommandStatsVariants c = (OptionsParser.CommandStatsVariants) command;
                statsVariants(c);
            }
        } catch (Exception e) {
            logger.error(e.getMessage());
            logger.debug("", e);
            System.exit(1);
        }
    }

    private static void indexSequence(OptionsParser.CommandIndexSequence c)
            throws URISyntaxException, IOException, FileFormatException {
        if (c.input.endsWith(".fasta") || c.input.endsWith(".fasta.gz")) {
            Path input = Paths.get(new URI(c.input).getPath());
            Path outdir = c.outdir.isEmpty() ? input.getParent() : Paths.get(new URI(c.outdir).getPath());

            logger.info("Indexing Fasta : " + input.toString());
            long start = System.currentTimeMillis();
            SqliteSequenceDBAdaptor sqliteSequenceDBAdaptor = new SqliteSequenceDBAdaptor();
            File index = null;
            try {
                index = sqliteSequenceDBAdaptor.index(input.toFile(), outdir);
            } catch (SQLException e) {
                e.printStackTrace();
                System.exit(1);
            }
            long end = System.currentTimeMillis();
            logger.info(
                    "Fasta file '" + input + "' indexed. " +
                            "Result: '" + index + "' . " +
                            "Time = " + (end - start) + "ms");
        } else {
            throw new IOException("Unknown file type");
        }
    }

    private static void indexAlignments(OptionsParser.CommandIndexAlignments c)
            throws ClassNotFoundException, IllegalAccessException, InstantiationException, URISyntaxException, IOException, FileFormatException, StorageManagerException {
        AlignmentStorageManager alignmentStorageManager;
        String storageEngine = parser.getGeneralParameters().storageEngine;
        if (storageEngine == null || storageEngine.isEmpty()) {
            alignmentStorageManager = StorageManagerFactory.get().getAlignmentStorageManager();
        } else {
            alignmentStorageManager = StorageManagerFactory.get().getAlignmentStorageManager(storageEngine);
        }
        URI input = new URI(null, c.input, null);
//        if(c.credentials != null && !c.credentials.isEmpty()) {
//            alignmentStorageManager.addConfigUri(new URI(null, c.credentials, null));
//        }

        URI outdir;
        if (c.outdir != null && !c.outdir.isEmpty()) {
            outdir = new URI(null, c.outdir + (c.outdir.endsWith("/") ? "" : "/"), null).resolve(".");
        } else {
            outdir = input.resolve(".");
        }

        assertDirectoryExists(outdir);

        ObjectMap params = new ObjectMap();
        params.putAll(parser.getGeneralParameters().params);

        if (c.fileId != 0) {
            params.put(AlignmentStorageManager.Options.FILE_ID.key(), c.fileId);
        }
        params.put(AlignmentStorageManager.Options.PLAIN.key(), false);
        params.put(AlignmentStorageManager.Options.MEAN_COVERAGE_SIZE_LIST.key(), c.meanCoverage);
        params.put(AlignmentStorageManager.Options.INCLUDE_COVERAGE.key(), c.calculateCoverage);
        params.put(AlignmentStorageManager.Options.DB_NAME.key(), c.dbName);
        params.put(AlignmentStorageManager.Options.COPY_FILE.key(), false);
        params.put(AlignmentStorageManager.Options.ENCRYPT.key(), "null");

        params.putAll(c.params);

        boolean extract, transform, load;
        URI nextFileUri = input;

        if (!c.load && !c.transform) {  // if not present --transform nor --load, do both
            extract = true;
            transform = true;
            load = true;
        } else {
            extract = c.transform;
            transform = c.transform;
            load = c.load;
        }

        if (extract) {
            logger.info("-- Extract alignments -- {}", input);
            nextFileUri = alignmentStorageManager.extract(input, outdir);
        }

        if (transform) {
            logger.info("-- PreTransform alignments -- {}", nextFileUri);
            nextFileUri = alignmentStorageManager.preTransform(nextFileUri);
            logger.info("-- Transform alignments -- {}", nextFileUri);
            nextFileUri = alignmentStorageManager.transform(nextFileUri, null, outdir);
            logger.info("-- PostTransform alignments -- {}", nextFileUri);
            nextFileUri = alignmentStorageManager.postTransform(nextFileUri);
        }

        if (load) {
            logger.info("-- PreLoad alignments -- {}", nextFileUri);
            nextFileUri = alignmentStorageManager.preLoad(nextFileUri, outdir);
            logger.info("-- Load alignments -- {}", nextFileUri);
            nextFileUri = alignmentStorageManager.load(nextFileUri);
            logger.info("-- PostLoad alignments -- {}", nextFileUri);
            nextFileUri = alignmentStorageManager.postLoad(nextFileUri, outdir);
        }
    }

    private static void indexVariants(OptionsParser.CommandIndexVariants c)
            throws ClassNotFoundException, IllegalAccessException, InstantiationException, URISyntaxException, IOException, FileFormatException, StorageManagerException {
        VariantStorageManager variantStorageManager;
        String storageEngine = parser.getGeneralParameters().storageEngine;
        variantStorageManager = StorageManagerFactory.get().getVariantStorageManager(storageEngine);
//        if(c.credentials != null && !c.credentials.isEmpty()) {
//            variantStorageManager.addConfigUri(new URI(null, c.credentials, null));
//        }

        URI variantsUri = new URI(null, c.input, null);
        URI pedigreeUri = c.pedigree != null && !c.pedigree.isEmpty() ? new URI(null, c.pedigree, null) : null;
        URI outdirUri;
        if (c.outdir != null && !c.outdir.isEmpty()) {
            outdirUri = new URI(null, c.outdir + (c.outdir.endsWith("/") ? "" : "/"), null).resolve(".");
        } else {
            outdirUri = variantsUri.resolve(".");
        }
        assertDirectoryExists(outdirUri);

        ObjectMap params = new ObjectMap();
        params.put(VariantStorageManager.Options.STUDY_ID.key(), c.studyId);
        params.put(VariantStorageManager.Options.FILE_ID.key(), c.fileId);
        params.put(VariantStorageManager.Options.SAMPLE_IDS.key(), c.sampleIds);
        params.put(VariantStorageManager.Options.CALCULATE_STATS.key(), c.calculateStats);
        params.put(VariantStorageManager.Options.INCLUDE_STATS.key(), c.includeStats);
        params.put(VariantStorageManager.Options.INCLUDE_GENOTYPES.key(), c.includeGenotype);   // TODO rename samples to genotypes
        params.put(VariantStorageManager.Options.INCLUDE_SRC.key(), c.includeSrc);
//        params.put(VariantStorageManager.Options.COMPRESS_GENOTYPES.key(), c.compressGenotypes);
        params.put(VariantStorageManager.Options.AGGREGATED_TYPE.key(), c.aggregated);
        params.put(VariantStorageManager.Options.DB_NAME.key(), c.dbName);
        params.put(VariantStorageManager.Options.ANNOTATE.key(), c.annotate);
        params.put(VariantAnnotationManager.OVERWRITE_ANNOTATIONS, c.overwriteAnnotations);
        if (c.studyConfigurationFile != null && !c.studyConfigurationFile.isEmpty()) {
            params.put(FileStudyConfigurationManager.STUDY_CONFIGURATION_PATH, c.studyConfigurationFile);
        }

        if(c.annotate) {
            //Get annotator config
            Properties annotatorProperties = Config.getStorageProperties();
            if(c.annotatorConfig != null && !c.annotatorConfig.isEmpty()) {
                annotatorProperties.load(new FileInputStream(c.annotatorConfig));
            }
//            params.put(VariantAnnotationManager.ANNOTATOR_PROPERTIES, annotatorProperties);

            //Get annotation source
            VariantAnnotationManager.AnnotationSource annotatorSource = c.annotator;
            if(annotatorSource == null) {
                annotatorSource = VariantAnnotationManager.AnnotationSource.valueOf(
                        annotatorProperties.getProperty(
                                OPENCGA_STORAGE_ANNOTATOR,
                                VariantAnnotationManager.AnnotationSource.CELLBASE_REST.name()
                        ).toUpperCase()
                );
            }
            params.put(VariantAnnotationManager.ANNOTATION_SOURCE, annotatorSource);
        }
        
        if (c.aggregationMappingFile != null) {
            Properties aggregationMappingProperties = new Properties();
            try {
                aggregationMappingProperties.load(new FileInputStream(c.aggregationMappingFile));
                params.put(VariantStorageManager.Options.AGGREGATION_MAPPING_PROPERTIES.key(), aggregationMappingProperties);
            } catch (FileNotFoundException e) {
                logger.error("Aggregation mapping file {} not found. Population stats won't be parsed.", c.aggregationMappingFile);
            }
        }

        params.putAll(c.params);

        URI nextFileUri = variantsUri;


        boolean extract, transform, load;

        if (!c.load && !c.transform) {
            extract = true;
            transform = true;
            load = true;
        } else {
            extract = c.transform;
            transform = c.transform;
            load = c.load;
        }

        if (extract) {
            logger.info("-- Extract variants -- {}", variantsUri);
            nextFileUri = variantStorageManager.extract(variantsUri, outdirUri);
        }

        if (transform) {
            logger.info("-- PreTransform variants -- {}", nextFileUri);
            nextFileUri = variantStorageManager.preTransform(nextFileUri);
            logger.info("-- Transform variants -- {}", nextFileUri);
            nextFileUri = variantStorageManager.transform(nextFileUri, pedigreeUri, outdirUri);
            logger.info("-- PostTransform variants -- {}", nextFileUri);
            nextFileUri = variantStorageManager.postTransform(nextFileUri);
        }

        if (load) {
            logger.info("-- PreLoad variants -- {}", nextFileUri);
            nextFileUri = variantStorageManager.preLoad(nextFileUri, outdirUri);
            logger.info("-- Load variants -- {}", nextFileUri);
            nextFileUri = variantStorageManager.load(nextFileUri);
            logger.info("-- PostLoad variants -- {}", nextFileUri);
            nextFileUri = variantStorageManager.postLoad(nextFileUri, outdirUri);
        }

    }

    private static void createAccessionIds(Path variantsPath, VariantSource source, String globalPrefix, String fromAccession, Path outdir)
            throws IOException {
        String studyId = source.getStudyId();
        String studyPrefix = studyId.substring(studyId.length() - 6);
        VcfRawReader reader = new VcfRawReader(variantsPath.toString());

        List<DataWriter> writers = new ArrayList<>();
        String variantsFilename = Files.getNameWithoutExtension(variantsPath.getFileName().toString());
        if (variantsPath.toString().endsWith(".gz")) {
            variantsFilename = Files.getNameWithoutExtension(variantsFilename);
        }
        writers.add(new VcfRawWriter(reader, outdir.toString() + "/" + variantsFilename + "_accessioned" + ".vcf"));

        List<Task<VcfRecord>> taskList = new ArrayList<>();
        taskList.add(new CreateAccessionTask(source, globalPrefix, studyPrefix, fromAccession));

        Runner vr = new Runner(reader, writers, taskList);

        System.out.println("Accessioning variants with prefix " + studyPrefix + "...");
        vr.run();
        System.out.println("Variants accessioned!");
    }

    private static void annotateVariants(OptionsParser.CommandAnnotateVariants c)
            throws ClassNotFoundException, IllegalAccessException, InstantiationException, URISyntaxException, IOException, VariantAnnotatorException, StorageManagerException {
        /**
         * Create DBAdaptor
         */
        VariantStorageManager variantStorageManager = StorageManagerFactory.get().getVariantStorageManager(parser.getGeneralParameters().storageEngine);
//        if(c.credentials != null && !c.credentials.isEmpty()) {
//            variantStorageManager.addConfigUri(new URI(null, c.credentials, null));
//        }
        ObjectMap params = new ObjectMap();
        VariantDBAdaptor dbAdaptor = variantStorageManager.getDBAdaptor(c.dbName);

        /**
         * Create Annotator
         */
        Properties annotatorProperties = Config.getStorageProperties();
        if(c.annotatorConfig != null && !c.annotatorConfig.isEmpty()) {
            annotatorProperties.load(new FileInputStream(c.annotatorConfig));
        }


        VariantAnnotationManager.AnnotationSource annotatorSource = c.annotator;
        if(annotatorSource == null) {
            annotatorSource = VariantAnnotationManager.AnnotationSource.valueOf(
                    annotatorProperties.getProperty(
                            OPENCGA_STORAGE_ANNOTATOR,
                            VariantAnnotationManager.AnnotationSource.CELLBASE_REST.name()
                    ).toUpperCase()
            );
        }
        logger.info("Annotating with {}", annotatorSource);
        VariantAnnotator annotator = null; //VariantAnnotationManager.buildVariantAnnotator(annotatorSource, annotatorProperties, c.species, c.assembly);
        VariantAnnotationManager variantAnnotationManager =
                new VariantAnnotationManager(annotator, dbAdaptor);

        /**
         * Annotation options
         */
        Query query = new Query();
        if (c.filterRegion != null) {
            query.put(VariantDBAdaptor.VariantQueryParams.REGION.key(), c.filterRegion);
        }
        if (c.filterChromosome != null) {
            query.put(VariantDBAdaptor.VariantQueryParams.CHROMOSOME.key(), c.filterChromosome);
        }
        if (c.filterGene != null) {
            query.put(VariantDBAdaptor.VariantQueryParams.GENE.key(), c.filterGene);
        }
        if (c.filterAnnotConsequenceType != null) {
            query.put(VariantDBAdaptor.VariantQueryParams.ANNOT_CONSEQUENCE_TYPE.key(), c.filterAnnotConsequenceType);
        }
        if (!c.overwriteAnnotations) {
            query.put(VariantDBAdaptor.VariantQueryParams.ANNOTATION_EXISTS.key(), false);
        }
        URI outputUri = new URI(null , c.outdir, null);
        if (outputUri.getScheme() == null || outputUri.getScheme().isEmpty()) {
            outputUri = new URI("file", c.outdir, null);
        }
        Path outDir = Paths.get(outputUri.resolve(".").getPath());

        /**
         * Create and load annotations
         */
        boolean doCreate = c.create, doLoad = c.load != null;
        if (!c.create && c.load == null) {
            doCreate = true;
            doLoad = true;
        }

        URI annotationFile = null;
        if (doCreate) {
            long start = System.currentTimeMillis();
            logger.info("Starting annotation creation ");
            annotationFile = variantAnnotationManager.createAnnotation(outDir, c.fileName.isEmpty() ? c.dbName : c.fileName, query, new QueryOptions());
            logger.info("Finished annotation creation {}ms", System.currentTimeMillis() - start);
        }

        if (doLoad) {
            long start = System.currentTimeMillis();
            logger.info("Starting annotation load");
            if (annotationFile == null) {
//                annotationFile = new URI(null, c.load, null);
                annotationFile = Paths.get(c.load).toUri();
            }
            variantAnnotationManager.loadAnnotation(annotationFile, new QueryOptions());
            logger.info("Finished annotation load {}ms", System.currentTimeMillis() - start);
        }

    }

    private static void statsVariants(OptionsParser.CommandStatsVariants c)
            throws IllegalAccessException, InstantiationException, ClassNotFoundException, URISyntaxException, IOException, StorageManagerException {

//        Path studyConfigurationPath = Paths.get(c.studyConfigurationFile);
//        StudyConfiguration studyConfiguration = StudyConfiguration.read(studyConfigurationPath);

        /**
         * query options
         */
        QueryOptions queryOptions = new QueryOptions();
//        VariantSource variantSource = new VariantSource(null, c.fileId, c.studyId, null);
//        queryOptions.put(VariantStorageManager.VARIANT_SOURCE, variantSource);
        queryOptions.put(VariantStorageManager.Options.DB_NAME.key(), c.dbName);
        queryOptions.put(VariantStorageManager.Options.OVERWRITE_STATS.key(), c.overwriteStats);
        queryOptions.put(VariantStorageManager.Options.FILE_ID.key(), c.fileId);
        queryOptions.put(VariantStorageManager.Options.STUDY_ID.key(), c.studyId);
//        queryOptions.put(VariantStorageManager.STUDY_CONFIGURATION, studyConfiguration);
        if (c.studyConfigurationFile != null && !c.studyConfigurationFile.isEmpty()) {
            queryOptions.put(FileStudyConfigurationManager.STUDY_CONFIGURATION_PATH, c.studyConfigurationFile);
        }


        Map<String, Set<String>> cohorts = null;
        if (c.cohort != null && !c.cohort.isEmpty()) {
            cohorts = new LinkedHashMap<>(c.cohort.size());
            for (Map.Entry<String, String> entry : c.cohort.entrySet()) {
                cohorts.put(entry.getKey(), new HashSet<>(Arrays.asList(entry.getValue().split(","))));
            }
        }


        /**
         * Create DBAdaptor
         */
        VariantStorageManager variantStorageManager = StorageManagerFactory.get().getVariantStorageManager(parser.getGeneralParameters().storageEngine);
//        if(c.credentials != null && !c.credentials.isEmpty()) {
//            variantStorageManager.addConfigUri(new URI(null, c.credentials, null));
//        }
        VariantDBAdaptor dbAdaptor = variantStorageManager.getDBAdaptor(c.dbName);
//        dbAdaptor.setConstantSamples(Integer.toString(c.fileId));    // TODO jmmut: change to studyId when we remove fileId
        StudyConfiguration studyConfiguration = variantStorageManager.getStudyConfiguration(queryOptions);
        /**
         * Create and load stats
         */
        URI outputUri = new URI(c.fileName);
        URI directoryUri = outputUri.resolve(".");
        String filename = outputUri.equals(directoryUri) ? VariantStorageManager.buildFilename(studyConfiguration.getStudyId(), c.fileId)
                : Paths.get(outputUri.getPath()).getFileName().toString();
        assertDirectoryExists(directoryUri);
        VariantStatisticsManager variantStatisticsManager = new VariantStatisticsManager();

        boolean doCreate = true;
        boolean doLoad = true;
//        doCreate = c.create;
//        doLoad = c.load != null;
//        if (!c.create && c.load == null) {
//            doCreate = doLoad = true;
//        } else if (c.load != null) {
//            filename = c.load;
//        }

        try {

            Map<String, Integer> cohortNameIds = c.cohortIds.entrySet().stream().collect(Collectors.toMap(Map.Entry::getKey, e -> Integer.parseInt(e.getValue())));

            if (doCreate) {
                filename += "." + TimeUtils.getTime();
                outputUri = outputUri.resolve(filename);
                outputUri = variantStatisticsManager.createStats(dbAdaptor, outputUri, cohorts, cohortNameIds, studyConfiguration, queryOptions);
            }

            if (doLoad) {
                outputUri = outputUri.resolve(filename);
                variantStatisticsManager.loadStats(dbAdaptor, outputUri, studyConfiguration, queryOptions);
            }
        } catch (Exception e) {   // file not found? wrong file id or study id? bad parameters to ParallelTaskRunner?
            e.printStackTrace();
            logger.error(e.getMessage());
            System.exit(1);
        }
    }

    public static StringBuilder printQueryResult(QueryResult queryResult, StringBuilder sb) throws JsonProcessingException {
        if (sb == null) {
            sb = new StringBuilder();
        }
        ObjectMapper jsonObjectMapper;

        JsonFactory factory = new JsonFactory();
        jsonObjectMapper = new ObjectMapper(factory);

        jsonObjectMapper.addMixIn(VariantSourceEntry.class, VariantSourceEntryJsonMixin.class);
        jsonObjectMapper.addMixIn(VariantSource.class, VariantSourceJsonMixin.class);
        jsonObjectMapper.addMixIn(VariantStats.class, VariantStatsJsonMixin.class);

        jsonObjectMapper.setSerializationInclusion(JsonInclude.Include.NON_NULL);
//                objectMapper.setSerializationInclusion(JsonInclude.Include.NON_EMPTY);
        jsonObjectMapper.setSerializationInclusion(JsonInclude.Include.NON_DEFAULT);

        sb.append(jsonObjectMapper.writeValueAsString(queryResult)).append("\n");
        return sb;
    }


    private static String getDefault(String value, String defaultValue) {
        return value == null || value.isEmpty() ? defaultValue : value;
    }

    private static void checkNull(Integer value, String name) {
        if(value == null || value.equals(0)) {
            missingValue(name);
        }
    }

    private static void checkNull(String value, String name) {
        if(value == null || value.isEmpty()) {
            missingValue(name);
        }
    }

    private static void missingValue(String name) {
        logger.info("The following options are required: " + name + "");
        logger.info(parser.usage());
//            throw new IllegalArgumentException("The following options are required: " + name + "");
        System.exit(1);
    }

    private static void printVersion(String version) {
        System.out.println("OpenCGA Storage CLI. Version " + version);
    }

    private static void setLogLevel(String logLevel) {
// This small hack allow to configure the appropriate Logger level from the command line, this is done
// by setting the DEFAULT_LOG_LEVEL_KEY before the logger object is created.
        System.setProperty(org.slf4j.impl.SimpleLogger.DEFAULT_LOG_LEVEL_KEY, logLevel);
        logger = LoggerFactory.getLogger(OpenCGAStorageMain.class);
    }

    private static void assertDirectoryExists(URI outdir){
        if (!java.nio.file.Files.exists(Paths.get(outdir.getPath()))) {
            logger.error("given output directory {} does not exist, please create it first.", outdir);
            System.exit(1);
        }
    }
}
