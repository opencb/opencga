/*
 * Copyright 2015-2017 OpenCB
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

package org.opencb.opencga.app.cli.internal.executors;

import org.ga4gh.models.ReadAlignment;
import org.opencb.biodata.models.alignment.RegionCoverage;
import org.opencb.biodata.tools.alignment.stats.AlignmentGlobalStats;
import org.opencb.commons.datastore.core.DataResponse;
import org.opencb.commons.datastore.core.ObjectMap;
import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.hpg.bigdata.analysis.tools.ExecutorMonitor;
import org.opencb.hpg.bigdata.analysis.tools.Status;
import org.opencb.opencga.analysis.alignment.AlignmentStorageManager;
import org.opencb.opencga.analysis.wrappers.BwaWrapperAnalysis;
import org.opencb.opencga.analysis.wrappers.DeeptoolsWrapperAnalysis;
import org.opencb.opencga.analysis.wrappers.SamtoolsWrapperAnalysis;
import org.opencb.opencga.app.cli.internal.options.AlignmentCommandOptions;
import org.opencb.opencga.catalog.exceptions.CatalogException;
import org.opencb.opencga.client.rest.OpenCGAClient;
import org.opencb.opencga.storage.core.alignment.AlignmentDBAdaptor;

import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Map;

/**
 * Created on 09/05/16
 *
 * @author Jacobo Coll &lt;jacobo167@gmail.com&gt;
 */
public class AlignmentCommandExecutor extends InternalCommandExecutor {

    private final AlignmentCommandOptions alignmentCommandOptions;
//    private AlignmentStorageEngine alignmentStorageManager;

    public AlignmentCommandExecutor(AlignmentCommandOptions options) {
        super(options.analysisCommonOptions);
        alignmentCommandOptions = options;
    }

    @Override
    public void execute() throws Exception {
        logger.debug("Executing variant command line");

//        String subCommandString = alignmentCommandOptions.getParsedSubCommand();
        String subCommandString = getParsedSubCommand(alignmentCommandOptions.jCommander);
        configure();
        switch (subCommandString) {
            case "index":
                index();
                break;
            case "query":
                query();
                break;
            case "stats":
                stats();
                break;
            case "coverage":
                coverage();
                break;
            case "delete":
                delete();
                break;
            case BwaWrapperAnalysis.ID:
                bwa();
                break;
            case SamtoolsWrapperAnalysis.ID:
                samtools();
                break;
            case DeeptoolsWrapperAnalysis.ID:
                deeptools();
                break;
            default:
                logger.error("Subcommand not valid");
                break;

        }
    }

    private void index() throws Exception {
        AlignmentCommandOptions.IndexAlignmentCommandOptions cliOptions = alignmentCommandOptions.indexAlignmentCommandOptions;

        ObjectMap params = new ObjectMap();
        params.put("transform", true);
        // TODO: Calculate coverage and stats

        String sessionId = cliOptions.commonOptions.token;

        // Initialize monitor
        ExecutorMonitor monitor = new ExecutorMonitor();
        Thread hook = new Thread(() -> {
            monitor.stop(new Status(Status.ERROR));
            logger.info("Running ShutdownHook. Tool execution has being aborted.");
        });

        Path outDirPath = Paths.get(cliOptions.outdirId);

        Runtime.getRuntime().addShutdownHook(hook);
        monitor.start(outDirPath);

        // Run index
        AlignmentStorageManager alignmentStorageManager =
                new AlignmentStorageManager(catalogManager, storageEngineFactory);
        alignmentStorageManager.index(cliOptions.study, cliOptions.fileId, outDirPath, params, sessionId);

        // Stop monitor
        Runtime.getRuntime().removeShutdownHook(hook);
        monitor.stop(new Status(Status.DONE));
    }


    private void query() throws InterruptedException, CatalogException, IOException {
        ObjectMap objectMap = new ObjectMap();
        objectMap.putIfNotNull("sid", alignmentCommandOptions.queryAlignmentCommandOptions.commonOptions.token);
        objectMap.putIfNotNull("study", alignmentCommandOptions.queryAlignmentCommandOptions.study);
        objectMap.putIfNotNull(AlignmentDBAdaptor.QueryParams.REGION.key(), alignmentCommandOptions.queryAlignmentCommandOptions.region);
        objectMap.putIfNotNull(AlignmentDBAdaptor.QueryParams.MIN_MAPQ.key(),
                alignmentCommandOptions.queryAlignmentCommandOptions.minMappingQuality);
        objectMap.putIfNotNull(AlignmentDBAdaptor.QueryParams.CONTAINED.key(),
                alignmentCommandOptions.queryAlignmentCommandOptions.contained);
        objectMap.putIfNotNull(AlignmentDBAdaptor.QueryParams.MD_FIELD.key(),
                alignmentCommandOptions.queryAlignmentCommandOptions.mdField);
        objectMap.putIfNotNull(AlignmentDBAdaptor.QueryParams.BIN_QUALITIES.key(),
                alignmentCommandOptions.queryAlignmentCommandOptions.binQualities);
        objectMap.putIfNotNull(QueryOptions.LIMIT, alignmentCommandOptions.queryAlignmentCommandOptions.limit);
        objectMap.putIfNotNull(QueryOptions.SKIP, alignmentCommandOptions.queryAlignmentCommandOptions.skip);
        objectMap.putIfNotNull(QueryOptions.COUNT, alignmentCommandOptions.queryAlignmentCommandOptions.count);

        OpenCGAClient openCGAClient = new OpenCGAClient(clientConfiguration);
        DataResponse<ReadAlignment> alignments = openCGAClient.getAlignmentClient()
                .query(alignmentCommandOptions.queryAlignmentCommandOptions.fileId, objectMap);

        for (ReadAlignment readAlignment : alignments.allResults()) {
            System.out.println(readAlignment);
        }
    }

    private void stats() throws CatalogException, IOException {
        ObjectMap objectMap = new ObjectMap();
        objectMap.putIfNotNull("sid", alignmentCommandOptions.statsAlignmentCommandOptions.commonOptions.token);
        objectMap.putIfNotNull("study", alignmentCommandOptions.statsAlignmentCommandOptions.study);
        objectMap.putIfNotNull("region", alignmentCommandOptions.statsAlignmentCommandOptions.region);
        objectMap.putIfNotNull("minMapQ", alignmentCommandOptions.statsAlignmentCommandOptions.minMappingQuality);
        if (alignmentCommandOptions.statsAlignmentCommandOptions.contained) {
            objectMap.put("contained", alignmentCommandOptions.statsAlignmentCommandOptions.contained);
        }

        OpenCGAClient openCGAClient = new OpenCGAClient(clientConfiguration);
        DataResponse<AlignmentGlobalStats> globalStats = openCGAClient.getAlignmentClient()
                .stats(alignmentCommandOptions.statsAlignmentCommandOptions.fileId, objectMap);

        for (AlignmentGlobalStats alignmentGlobalStats : globalStats.allResults()) {
            System.out.println(alignmentGlobalStats.toJSON());
        }
    }

    private void coverage() throws CatalogException, IOException {
        ObjectMap objectMap = new ObjectMap();
        objectMap.putIfNotNull("sid", alignmentCommandOptions.coverageAlignmentCommandOptions.commonOptions.token);
        objectMap.putIfNotNull("study", alignmentCommandOptions.coverageAlignmentCommandOptions.study);
        objectMap.putIfNotNull("region", alignmentCommandOptions.coverageAlignmentCommandOptions.region);
        objectMap.putIfNotNull("minMapQ", alignmentCommandOptions.coverageAlignmentCommandOptions.minMappingQuality);
        if (alignmentCommandOptions.coverageAlignmentCommandOptions.contained) {
            objectMap.put("contained", alignmentCommandOptions.coverageAlignmentCommandOptions.contained);
        }

        OpenCGAClient openCGAClient = new OpenCGAClient(clientConfiguration);
        DataResponse<RegionCoverage> globalStats = openCGAClient.getAlignmentClient()
                .coverage(alignmentCommandOptions.coverageAlignmentCommandOptions.fileId, objectMap);

        for (RegionCoverage regionCoverage : globalStats.allResults()) {
            System.out.println(regionCoverage.toString());
        }
    }

    private void delete() {
        throw new UnsupportedOperationException();
    }

    //-------------------------------------------------------------------------
    // W R A P P E R S     A N A L Y S I S
    //-------------------------------------------------------------------------

    // BWA

    private void bwa() throws Exception {
        AlignmentCommandOptions.BwaCommandOptions cliOptions = alignmentCommandOptions.bwaCommandOptions;
        ObjectMap params = new ObjectMap();
        params.putAll(cliOptions.basicCommandOptions.params);

        BwaWrapperAnalysis bwa = new BwaWrapperAnalysis();
        bwa.setUp(appHome, catalogManager, storageEngineFactory, params, Paths.get(cliOptions.outdir),
                cliOptions.basicCommandOptions.token);

        bwa.setStudy(cliOptions.study);

        bwa.setCommand(cliOptions.command)
                .setFastaFile(cliOptions.fastaFile)
                .setIndexBaseFile(cliOptions.indexBaseFile)
                .setFastq1File(cliOptions.fastq1File)
                .setFastq2File(cliOptions.fastq2File)
                .setSamFile(cliOptions.samFile);

        bwa.start();
    }

    // Samtools

    private void samtools() throws Exception {
        AlignmentCommandOptions.SamtoolsCommandOptions cliOptions = alignmentCommandOptions.samtoolsCommandOptions;
        ObjectMap params = new ObjectMap();
        params.putAll(cliOptions.basicCommandOptions.params);

        SamtoolsWrapperAnalysis samtools = new SamtoolsWrapperAnalysis();
        samtools.setUp(appHome, catalogManager, storageEngineFactory, params, Paths.get(cliOptions.outdir),
                cliOptions.basicCommandOptions.token);

        samtools.setStudy(cliOptions.study);

        samtools.setCommand(cliOptions.command)
                .setInputFile(cliOptions.inputFile)
                .setOutputFile(cliOptions.outputFile);

        samtools.start();
    }

    // Deeptools

    private void deeptools() throws Exception {
        AlignmentCommandOptions.DeeptoolsCommandOptions cliOptions = alignmentCommandOptions.deeptoolsCommandOptions;
        ObjectMap params = new ObjectMap();
        params.putAll(cliOptions.basicCommandOptions.params);

        DeeptoolsWrapperAnalysis deeptools = new DeeptoolsWrapperAnalysis();
        deeptools.setUp(appHome, catalogManager, storageEngineFactory, params, Paths.get(cliOptions.outdir),
                cliOptions.basicCommandOptions.token);

        deeptools.setStudy(cliOptions.study);

        deeptools.setExecutable(cliOptions.executable)
                .setBamFile(cliOptions.bamFile)
                .setCoverageFile(cliOptions.coverageFile);

        deeptools.start();
    }

    //-------------------------------------------------------------------------
    // M I S C E L A N E O U S     M E T H O D S
    //-------------------------------------------------------------------------

    private void addParam(Map<String, String> map, String key, Object value) {
        if (value == null) {
            return;
        }

        if (value instanceof String) {
            if (!((String) value).isEmpty()) {
                map.put(key, (String) value);
            }
        } else if (value instanceof Integer) {
            map.put(key, Integer.toString((int) value));
        } else if (value instanceof Boolean) {
            map.put(key, Boolean.toString((boolean) value));
        } else {
            throw new UnsupportedOperationException();
        }
    }

}
