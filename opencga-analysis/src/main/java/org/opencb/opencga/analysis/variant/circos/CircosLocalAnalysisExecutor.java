/*
 * Copyright 2015-2020 OpenCB
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

package org.opencb.opencga.analysis.variant.circos;

import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.collections.MapUtils;
import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.time.StopWatch;
import org.opencb.biodata.models.alignment.RegionCoverage;
import org.opencb.biodata.models.clinical.interpretation.DiseasePanel;
import org.opencb.biodata.models.core.Gene;
import org.opencb.biodata.models.core.Region;
import org.opencb.biodata.models.variant.StudyEntry;
import org.opencb.biodata.models.variant.Variant;
import org.opencb.biodata.models.variant.avro.BreakendMate;
import org.opencb.biodata.models.variant.avro.ConsequenceType;
import org.opencb.biodata.models.variant.avro.StructuralVariation;
import org.opencb.biodata.models.variant.avro.VariantType;
import org.opencb.cellbase.client.rest.CellBaseClient;
import org.opencb.commons.datastore.core.Query;
import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.commons.datastore.core.QueryResponse;
import org.opencb.commons.utils.DockerUtils;
import org.opencb.opencga.analysis.StorageToolExecutor;
import org.opencb.opencga.analysis.alignment.AlignmentStorageManager;
import org.opencb.opencga.analysis.variant.manager.VariantStorageManager;
import org.opencb.opencga.catalog.exceptions.CatalogException;
import org.opencb.opencga.catalog.utils.ParamUtils;
import org.opencb.opencga.core.common.TimeUtils;
import org.opencb.opencga.core.exceptions.ToolException;
import org.opencb.opencga.core.exceptions.ToolExecutorException;
import org.opencb.opencga.core.models.panel.Panel;
import org.opencb.opencga.core.models.variant.CircosAnalysisParams;
import org.opencb.opencga.core.models.variant.CircosTrack;
import org.opencb.opencga.core.response.OpenCGAResult;
import org.opencb.opencga.core.tools.annotations.ToolExecutor;
import org.opencb.opencga.core.tools.variant.CircosAnalysisExecutor;
import org.opencb.opencga.storage.core.exceptions.StorageEngineException;
import org.opencb.opencga.storage.core.variant.adaptors.iterators.VariantDBIterator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.*;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static org.opencb.biodata.models.variant.avro.VariantType.*;
import static org.opencb.opencga.analysis.variant.manager.VariantCatalogQueryUtils.PANEL;
import static org.opencb.opencga.analysis.wrappers.OpenCgaWrapperAnalysis.DOCKER_INPUT_PATH;
import static org.opencb.opencga.analysis.wrappers.OpenCgaWrapperAnalysis.DOCKER_OUTPUT_PATH;
import static org.opencb.opencga.core.api.ParamConstants.*;
import static org.opencb.opencga.storage.core.variant.adaptors.VariantQueryParam.*;

@ToolExecutor(id="opencga-local", tool = CircosAnalysis.ID,
        framework = ToolExecutor.Framework.LOCAL, source = ToolExecutor.Source.STORAGE)
public class CircosLocalAnalysisExecutor extends CircosAnalysisExecutor implements StorageToolExecutor {

    public final static String R_DOCKER_IMAGE = "opencb/opencga-r:2.0.0-rc2";
    public static final Pattern CNV_PATTERN = Pattern.compile("^(INFO:[a-zA-Z\\\\.]+)((([ ])*([\\+\\-*\\\\])+([ ])*(INFO:[a-zA-Z\\\\.]+))*)$");

    private VariantStorageManager storageManager;

    private Map<String, String> errors;

    private Logger logger = LoggerFactory.getLogger(this.getClass());

    public CircosLocalAnalysisExecutor() {
        super();
    }

    public CircosLocalAnalysisExecutor(String study, CircosAnalysisParams params, VariantStorageManager storageManager) {
        super(study, params);
        this.storageManager = storageManager;
    }

    @Override
    public VariantStorageManager getVariantStorageManager() throws ToolExecutorException {
        if (storageManager == null) {
            storageManager = StorageToolExecutor.super.getVariantStorageManager();
        }
        return storageManager;
    }

    public void run() throws ToolException, IOException {
        // Create query
        Query query = new Query();
        if (MapUtils.isNotEmpty(getCircosParams().getQuery())) {
            query.putAll(getCircosParams().getQuery());
        }

        logger.info("getCircosParams().getQuery() = " + getCircosParams().getQuery());
        logger.info("getStudy() = " + getStudy());
        //query.put(STUDY.key(), getStudy());

        // Error management
        errors = new HashMap<>();

        // In parallel, launch a query per track
        ExecutorService threadPool = Executors.newFixedThreadPool(this.circosParams.getTracks().size());
        List<Future<Boolean>> futureList = new ArrayList<>(this.circosParams.getTracks().size());
        for (CircosTrack track : this.circosParams.getTracks()) {
            // track.getType().name();
            switch (track.getType()) {
                case SNV:
                case INDEL:
                case DELETION:
                case INSERTION:
                    futureList.add(threadPool.submit(getNamedThread(track.getType().name(), () -> variantQuery(query, track))));
                    break;
                case CNV:
                    futureList.add(threadPool.submit(getNamedThread(track.getType().name(), () -> cnvQuery(query, track))));
                    break;
                case REARRANGEMENT:
                    futureList.add(threadPool.submit(getNamedThread(track.getType().name(), () -> rearrangementQuery(query, track))));
                    break;
                case RAINPLOT:
                    futureList.add(threadPool.submit(getNamedThread(track.getType().name(), () -> rainplotQuery(query, track))));
                    break;
                case GENE:
                    futureList.add(threadPool.submit(getNamedThread(track.getType().name(), () -> geneQuery(query, track))));
                    break;
                case COVERAGE:
                    futureList.add(threadPool.submit(getNamedThread(track.getType().name(), () -> coverageQuery(query, track))));
                    break;
                case COVERAGE_RATIO:
                    futureList.add(threadPool.submit(getNamedThread(track.getType().name(), () -> coverageRatioQuery(query, track))));
                    break;
            }
        }

        if (MapUtils.isEmpty(errors)) {
            // Write Circos config in JSON format
            File circosFile = getOutDir().resolve("circos.config.json").toFile();
            FileUtils.write(circosFile, circosParams.toJson());

            if (!circosFile.exists()) {
                throw new ToolException("Error writing Circos config JSON file");
            }

            // Execute R script
            // circos.R circos.config.json
            String rScriptPath = getExecutorParams().getString("opencgaHome") + "/analysis/R/" + getToolId();
            List<AbstractMap.SimpleEntry<String, String>> inputBindings = new ArrayList<>();
            inputBindings.add(new AbstractMap.SimpleEntry<>(rScriptPath, DOCKER_INPUT_PATH));
            AbstractMap.SimpleEntry<String, String> outputBinding = new AbstractMap.SimpleEntry<>(getOutDir().toAbsolutePath().toString(),
                    DOCKER_OUTPUT_PATH);
            String scriptParams = "R CMD Rscript --vanilla " + DOCKER_INPUT_PATH + "/circos.R "
                    + " --out_path " + DOCKER_OUTPUT_PATH
                    + " --config_file " + DOCKER_OUTPUT_PATH + circosFile.getName();


            StopWatch stopWatch = StopWatch.createStarted();
            String cmdline = DockerUtils.run(R_DOCKER_IMAGE, inputBindings, outputBinding, scriptParams, null);
            logger.info("Docker command line: " + cmdline);
            logger.info("Execution time: " + TimeUtils.durationToString(stopWatch));
        } else {
            StringBuilder msg = new StringBuilder();
            for (Map.Entry<String, String> error : errors.entrySet()) {
                msg.append("Error on track ").append(error.getKey()).append(": ").append(error.getValue()).append(". ");
            }
            throw new ToolException(msg.toString());
        }
    }


    /**
     * Create file with the distance of consecutive SNV, INDEL, DELETION, INSERTION variants.
     *
     * @param query General query
     * @param track Circos track
     * @return True or false depending on successs
     */
    private boolean variantQuery(Query query, CircosTrack track) {
        PrintWriter pw = null;

        try {
            File trackFile = getTrackFilename(track.getType().name());
            pw = new PrintWriter(trackFile);
            pw.println("chromosome\tchromStart\tchromEnd\tref\talt");

            // Create variant query
            Query variantQuery = new Query(query);
            if (MapUtils.isNotEmpty(track.getQuery())) {
                variantQuery.putAll(track.getQuery());
            }

            variantQuery.put(TYPE.key(), track.getType().name());

            QueryOptions queryOptions = new QueryOptions(QueryOptions.INCLUDE, "id");

            logger.info(track.getType().name() + " track, query: " + variantQuery.toJson());
            logger.info(track.getType().name() + " track, query options: " + queryOptions.toJson());

            VariantDBIterator iterator = storageManager.iterator(variantQuery, queryOptions, getToken());
            while (iterator.hasNext()) {
                Variant v = iterator.next();
                pw.println("chr" + v.getChromosome() + "\t" + v.getStart() + "\t" + v.getEnd() + "\t" + v.getReference() + "\t"
                        + v.getAlternate());

            }

            // Set color if necessary
            if (!track.getDisplay().containsKey("color") || StringUtils.isEmpty(track.getDisplay().get("color"))) {
                String color;
                switch (track.getType()) {
                    case SNV:
                        color = "red";
                        break;
                    case INDEL:
                        color = "blue";
                        break;
                    case DELETION:
                        color = "yellow";
                        break;
                    case INSERTION:
                        color = "green";
                        break;
                    default:
                        color = "orange";
                        break;
                }
                track.getDisplay().put("color", color);
            }

            // Set track file
            track.setFile(trackFile.getAbsolutePath());

        } catch (CatalogException | StorageEngineException | FileNotFoundException e) {
            errors.put(track.getType().name(), e.getMessage());
            return false;
        } finally {
            if (pw != null) {
                pw.close();
            }
        }

        return true;
    }

    /**
     * Create file with the distance of consecutive SNV variants.
     *
     * @param query General query
     * @param track Circos track
     * @return True or false depending on successs
     */
    private boolean rainplotQuery(Query query, CircosTrack track) {
        PrintWriter pw = null;
        PrintWriter pwOut = null;
        try {
            File trackFile = getTrackFilename(track.getType().name());

            pw = new PrintWriter(trackFile);
            pw.println("chromosome\tchromStart\tchromEnd\tref\talt\tlogDistPrev\tcolor");

            pwOut = new PrintWriter(new File(trackFile.getAbsoluteFile() + ".discarded"));

            // Create rainplot query
            Query rainplotQuery = new Query(query);
            if (MapUtils.isNotEmpty(track.getQuery())) {
                rainplotQuery.putAll(track.getQuery());
            }
            rainplotQuery.put(TYPE.key(), SNV);

            QueryOptions queryOptions = new QueryOptions()
                    .append(QueryOptions.INCLUDE, "id")
                    .append(QueryOptions.SORT, true);

            logger.info(track.getType().name() + " track, query: " + rainplotQuery.toJson());
            logger.info(track.getType().name() + " track, query options: " + queryOptions.toJson());

            // Get colors
            Map<String, String> colors = getRainplotColors();

            VariantDBIterator iterator = storageManager.iterator(rainplotQuery, queryOptions, getToken());

            int prevStart = 0;
            String currentChrom = "";
            while (iterator.hasNext()) {
                Variant v = iterator.next();
                if (v.getStart() > v.getEnd()) {
                    // Sanity check
                    pwOut.println(v.toString() + "\tStart  (" + v.getStart() + ") is bigger than end (" + v.getEnd() + ")");
                } else {
                    if (!v.getChromosome().equals(currentChrom)) {
                        prevStart = 0;
                        currentChrom = v.getChromosome();
                    }
                    String color, key = v.getReference() + v.getAlternate();
                    if (colors.containsKey(key)) {
                        color = colors.get(key);
                    } else {
                        color = "grey";
                    }
                    pw.println("chr" + v.getChromosome() + "\t" + v.getStart() + "\t" + v.getEnd() + "\t" + v.getReference() + "\t"
                            + v.getAlternate() + "\t" + Math.log10(v.getStart() - prevStart) + "\t" + color);
                    prevStart = v.getStart();
                }
            }
        } catch(Exception e) {
            errors.put(track.getType().name(), e.getMessage());
            return false;
        } finally {
            if (pw != null) {
                pw.close();
            }
            if (pwOut != null) {
                pwOut.close();
            }
        }
        return true;
    }

    /**
     * Create file with copy-number variants.
     *
     * @param query General query
     * @param track Circos track
     * @return True or false depending on successs
     */
    private boolean cnvQuery(Query query, CircosTrack track) {
        PrintWriter pw = null;
        PrintWriter pwOut = null;
        try {
            File trackFile = getTrackFilename(track.getType().name());

            pw = new PrintWriter(trackFile);
            pw.println("chromosome\tchromStart\tchromEnd\tdata\tcolor");

            pwOut = new PrintWriter(new File(trackFile.getAbsoluteFile() + ".discarded"));

            // Create CNV query
            Query cnvQuery = new Query(query);
            if (MapUtils.isNotEmpty(track.getQuery())) {
                cnvQuery.putAll(track.getQuery());
            }
            cnvQuery.put(TYPE.key(), CNV);

            if (StringUtils.isEmpty(track.getData())) {
                throw new ToolException("Field 'data' can not be empty when plotting CNV tracks");
            }

            String infoName1;
            String infoName2;
            String operator;
            Matcher matcher = CNV_PATTERN.matcher(track.getData());
            if (matcher.find()) {
                infoName1 = matcher.group(1);
                operator = matcher.group(5);
                infoName2 = matcher.group(7);
            } else {
                throw new ToolException("Invalid format in field 'data' in CNV track: " + track.getData());
            }

            QueryOptions queryOptions = new QueryOptions(QueryOptions.INCLUDE, "id,studies");

            logger.info(track.getType().name() + " track, query: " + cnvQuery.toJson());
            logger.info(track.getType().name() + " track, query options: " + queryOptions.toJson());

            // Get colors
            Map<Double, String> colors = getCnvColors();

            VariantDBIterator iterator = storageManager.iterator(cnvQuery, queryOptions, getToken());
            while (iterator.hasNext()) {
                Variant v = iterator.next();

                if (CollectionUtils.isEmpty(v.getStudies())) {
                    pwOut.println(v.toString() + "\tStudies is empty");
                } else {
                    String strScore1 = null;
                    String strScore2 = null;

                    StudyEntry studyEntry = v.getStudies().get(0);
                    if (StringUtils.isNotEmpty(infoName1)) {
                        strScore1 = studyEntry.getSampleData(cnvQuery.getString(SAMPLE.key()), infoName1.substring(5));
                        if (StringUtils.isNotEmpty(infoName2)) {
                            strScore2 = studyEntry.getSampleData(cnvQuery.getString(SAMPLE.key()), infoName2.substring(5));
                        }
                    }

                    Number data = Double.parseDouble(strScore1);
                    if (operator != null) {
                        switch (operator) {
                            case "+":
                                data = Double.parseDouble(strScore1) + Double.parseDouble(strScore2);
                                break;
                            case "-":
                                data = Double.parseDouble(strScore1) - Double.parseDouble(strScore2);
                                break;
                            case "*":
                                data = Double.parseDouble(strScore1) * Double.parseDouble(strScore2);
                                break;
                            case "/":
                                data = Double.parseDouble(strScore1) / Double.parseDouble(strScore2);
                                break;
                        }
                    }
                    pw.println("chr" + v.getChromosome() + "\t" + v.getStart() + "\t" + v.getEnd() + "\t" + data + "\t"
                            + getColorByRange((double) data, colors));

                }
            }
        } catch (Exception e) {
            errors.put(track.getType().name(), e.getMessage());
            return false;
        } finally {
            if (pw != null) {
                pw.close();
            }
            if (pwOut != null) {
                pwOut.close();
            }
        }
        return true;
    }


    /**
     * Create file with gene info to plot the corresponding Circos track.
     *
     * @param query General query
     * @param track Circos track
     * @return True or false depending on successs
     */
    private boolean geneQuery(Query query, CircosTrack track) {
        PrintWriter pw = null;

        try {
            File trackFile = getTrackFilename(track.getType().name());
            pw = new PrintWriter(trackFile);
            pw.println("chromosome\tchromStart\tchromEnd");

            Query variantQuery = new Query(query);
            if (MapUtils.isNotEmpty(track.getQuery())) {
                variantQuery.putAll(track.getQuery());
            }

            QueryOptions queryOptions = new QueryOptions(QueryOptions.INCLUDE, "id,annotation");

            logger.info(track.getType().name() + " track, query: " + variantQuery.toJson());
            logger.info(track.getType().name() + " track, query options: " + queryOptions.toJson());

            Set<String> geneIds = new HashSet<>();

            VariantDBIterator iterator = storageManager.iterator(variantQuery, queryOptions, getToken());
            while (iterator.hasNext()) {
                Variant v = iterator.next();
                if (v.getAnnotation() != null && CollectionUtils.isNotEmpty(v.getAnnotation().getConsequenceTypes())) {
                    for (ConsequenceType ct : v.getAnnotation().getConsequenceTypes()) {
                        if (StringUtils.isNotEmpty(ct.getEnsemblGeneId())) {
                            geneIds.add(ct.getEnsemblGeneId());
                        }
                    }
                }
            }

            // Add genes from the field 'include'
            if (CollectionUtils.isNotEmpty(track.getInclude())) {
                geneIds.addAll(track.getInclude());
            }

            // Call CellBase in order to get gene information
            CellBaseClient cellBaseClient = storageManager.getCellBaseUtils(variantQuery.getString(STUDY.key()), getToken())
                    .getCellBaseClient();
            queryOptions = new QueryOptions(QueryOptions.INCLUDE, "id,chromosome,start,end");
            QueryResponse<Gene> geneResponse = cellBaseClient.getGeneClient().get(new ArrayList<>(geneIds), queryOptions);

            // Write gene info in track file
            for (Gene gene : geneResponse.allResults()) {
                pw.println("chr" + gene.getChromosome() + "\t" + gene.getStart() + "\t" + gene.getEnd());
            }

            // Set track file
            track.setFile(trackFile.getAbsolutePath());

        } catch (CatalogException | StorageEngineException | IOException e) {
            e.printStackTrace();
            errors.put(track.getType().name(), e.getMessage());
            return false;
        } finally {
            if (pw != null) {
                pw.close();
            }
        }

        return true;
    }

    /**
     * Create file with coverage info to plot the corresponding Circos track..
     *
     * @param query General query
     * @param track Circos track
     * @return True or false depending on successs
     */
    private boolean coverageQuery(Query query, CircosTrack track) {
        PrintWriter pw = null;

        try {
            // Color management
            String colorLabel;
            Map<Double, String> colors = null;
            if (!track.getDisplay().containsKey("color") || StringUtils.isEmpty(track.getDisplay().get("color"))) {
                colors = getCoverageColors();
                colorLabel = "";
            } else {
                colorLabel = "\tcolor";
            }

            AlignmentStorageManager alignmentStorageManager = getAlignmentStorageManager();

            Map<String, String> trackQuery = track.getQuery();

            String inputFile = trackQuery.get(FILE.key());
            ParamUtils.checkIsSingleID(inputFile, FILE.key());

            File trackFile = getTrackFilename(track.getType().name());
            pw = new PrintWriter(trackFile);
            pw.println("chromosome\tchromStart\tchromEnd\tcoverage" + colorLabel);

            List<Region> regions = getRegionsFromQuery(trackQuery, alignmentStorageManager);

            // Compute window size
            int windowSize = computeWindowSize();

            for (Region region : regions) {
                OpenCGAResult<RegionCoverage> coverageResult = alignmentStorageManager.coverageQuery(study, inputFile, region, 0,
                        Integer.MAX_VALUE, windowSize, getToken());



                for (RegionCoverage regionCoverage : coverageResult.getResults()) {
                    if (regionCoverage.getValues() != null && regionCoverage.getValues().length > 0) {
                        int start = regionCoverage.getStart();
                        int end = start + regionCoverage.getWindowSize() - 1;
                        if (end > regionCoverage.getEnd()) {
                            end = regionCoverage.getEnd();
                        }
                        for (double coverageValue : regionCoverage.getValues()) {
                            if (colors != null) {
                                System.out.println("chr" + regionCoverage.getChromosome() + "\t" + start + "\t" + end + "\t" + coverageValue
                                        + "\t" + getColorByRange(coverageValue, colors));

                                pw.println("chr" + regionCoverage.getChromosome() + "\t" + start + "\t" + end + "\t" + coverageValue
                                        + "\t" + getColorByRange(coverageValue, colors));
                            } else {
                                pw.println("chr" + regionCoverage.getChromosome() + "\t" + start + "\t" + end + "\t" + coverageValue);
                            }
                            start += regionCoverage.getWindowSize();
                            end += regionCoverage.getWindowSize();
                            if (end > regionCoverage.getEnd()) {
                                end = regionCoverage.getEnd();
                            }
                        }
                    }
                }
            }

            // Set track file
            track.setFile(trackFile.getAbsolutePath());

        } catch (Exception e) {
            errors.put(track.getType().name(), e.getMessage());
            return false;
        } finally {
            if (pw != null) {
                pw.close();
            }
        }

        return true;
    }

    /**
     * Create file with coverage ratio (from two files) to plot the corresponding Circos track..
     *
     * @param query General query
     * @param track Circos track
     * @return True or false depending on successs
     */
    private boolean coverageRatioQuery(Query query, CircosTrack track) {
        PrintWriter pw = null;

        try {
            // Color management
            String colorLabel;
            Map<Double, String> colors = null;
            if (!track.getDisplay().containsKey("color") || StringUtils.isEmpty(track.getDisplay().get("color"))) {
                colors = getCoverageRatioColors();
                colorLabel = "";
            } else {
                colorLabel = "\tcolor";
            }

            AlignmentStorageManager alignmentStorageManager = getAlignmentStorageManager();

            Map<String, String> trackQuery = track.getQuery();

            String somaticFile = trackQuery.get(FILE_ID_1_PARAM);
            ParamUtils.checkIsSingleID(somaticFile, FILE_ID_1_PARAM);
            String germlineFile = trackQuery.get(FILE_ID_1_PARAM);
            ParamUtils.checkIsSingleID(germlineFile, FILE_ID_2_PARAM);

            File trackFile = getTrackFilename(track.getType().name());
            pw = new PrintWriter(trackFile);
            pw.println("chromosome\tchromStart\tchromEnd\tratio" + colorLabel);


            List<Region> regions = getRegionsFromQuery(trackQuery, alignmentStorageManager);

            // Compute window size
            int windowSize = computeWindowSize();

            boolean skipLog2 = Boolean.parseBoolean(trackQuery.getOrDefault(SKIP_LOG2_PARAM, "false"));


            // Getting total counts for file #1: somatic file
            OpenCGAResult<Long> somaticResult = alignmentStorageManager.getTotalCounts(study, somaticFile, getToken());
            if (CollectionUtils.isEmpty(somaticResult.getResults()) || somaticResult.getResults().get(0) == 0) {
                throw new ToolException("Coverage ratio: impossible get total counts for file " + somaticFile);
            }
            long somaticTotalCounts = somaticResult.getResults().get(0);

            // Getting total counts for file #2: germline file
            OpenCGAResult<Long> germlineResult = alignmentStorageManager.getTotalCounts(study, germlineFile, getToken());
            if (CollectionUtils.isEmpty(germlineResult.getResults()) || germlineResult.getResults().get(0) == 0) {
                throw new ToolException("Coverage ratio: impossible get total counts for file " + germlineFile);
            }
            long germlineTotalCounts = germlineResult.getResults().get(0);

            // Compute (log2) coverage ratio for each region given
            for (Region region : regions) {

                OpenCGAResult<RegionCoverage> ratioResult = alignmentStorageManager.coverageRatioQuery(study, somaticFile,
                        somaticTotalCounts, germlineFile, germlineTotalCounts,
                        region, windowSize, skipLog2, getToken());

                if (CollectionUtils.isNotEmpty(ratioResult.getResults())) {
                    for (RegionCoverage regionCoverage : ratioResult.getResults()) {
                        if (regionCoverage.getValues() != null && regionCoverage.getValues().length > 0) {
                            int start = regionCoverage.getStart();
                            int end = start + regionCoverage.getWindowSize() - 1;
                            if (end > regionCoverage.getEnd()) {
                                end = regionCoverage.getEnd();
                            }
                            for (double coverageValue : regionCoverage.getValues()) {
                                if (colors != null) {
                                    System.out.println("chr" + regionCoverage.getChromosome() + "\t" + start + "\t" + end + "\t" + coverageValue
                                            + "\t" + getColorByRange(coverageValue, colors));

                                    pw.println("chr" + regionCoverage.getChromosome() + "\t" + start + "\t" + end + "\t" + coverageValue
                                            + "\t" + getColorByRange(coverageValue, colors));
                                } else {
                                    pw.println("chr" + regionCoverage.getChromosome() + "\t" + start + "\t" + end + "\t" + coverageValue);
                                }
                                start += regionCoverage.getWindowSize();
                                end += regionCoverage.getWindowSize();
                                if (end > regionCoverage.getEnd()) {
                                    end = regionCoverage.getEnd();
                                }
                            }
                        }
                    }
                }
            }

            // Set track file
            track.setFile(trackFile.getAbsolutePath());

        } catch (Exception e) {
            errors.put(track.getType().name(), e.getMessage());
            return false;
        } finally {
            if (pw != null) {
                pw.close();
            }
        }

        return true;
    }

    /**
     * Create file with rearrangement variants.
     *
     * @param query General query
     * @param track Circos track
     * @return True or false depending on successs
     */
    private boolean rearrangementQuery(Query query, CircosTrack track) {
        PrintWriter pw = null;
        PrintWriter pwOut = null;
        try {
            File trackFile = getTrackFilename(track.getType().name());

            pw = new PrintWriter(trackFile);
            pw.println("Chromosome\tchromStart\tchromEnd\tChromosome.1\tchromStart.1\tchromEnd.1\ttype\tcolor");

            pwOut = new PrintWriter(new File(trackFile.getAbsoluteFile() + ".discarded"));

            // Create rainplot query
            Query rearrangementQuery = new Query(query);
            if (MapUtils.isNotEmpty(track.getQuery())) {
                rearrangementQuery.putAll(track.getQuery());
            }
            rearrangementQuery.put(TYPE.key(), "DELETION,TRANSLOCATION,INVERSION,DUPLICATION,BREAKEND");

            QueryOptions queryOptions = new QueryOptions(QueryOptions.INCLUDE, "id,sv");

            logger.info(track.getType().name() + " track, query: " + rearrangementQuery.toJson());
            logger.info(track.getType().name() + " track, query options: " + queryOptions.toJson());

            VariantDBIterator iterator = storageManager.iterator(rearrangementQuery, queryOptions, getToken());

            Map<VariantType, String> colors = getRearrangementColors();
            while (iterator.hasNext()) {
                Variant v = iterator.next();
                String type = null;
                switch (v.getType()) {
                    case DELETION: {
                        type = "DEL";
                        break;
                    }
                    case BREAKEND:
                    case TRANSLOCATION: {
                        type = "BND";
                        break;
                    }
                    case DUPLICATION: {
                        type = "DUP";
                        break;
                    }
                    case INVERSION: {
                        type = "INV";
                        break;
                    }
                    default: {
                        // Sanity check
                        pwOut.println(v.toString() + "\tUnknown type: " + v.getType() + ". Valid values: " + DELETION + ", " + BREAKEND
                                + ", " + TRANSLOCATION + ", " + DUPLICATION + ", " + INVERSION);

                        break;
                    }
                }

                if (type != null) {
                    // Check structural variation
                    StructuralVariation sv = v.getSv();
                    if (sv != null) {
                        if (sv.getBreakend() != null) {
                            if (sv.getBreakend().getMate() != null) {
                                BreakendMate mate = sv.getBreakend().getMate();
                                pw.println("chr" + v.getChromosome() + "\t" + v.getStart() + "\t" + v.getEnd() + "\tchr"
                                        + mate.getChromosome() + "\t" + mate.getPosition() + "\t" + mate.getPosition() + "\t" + type
                                        + "\t" + colors.get(v.getType()));
                            } else {
                                pwOut.println(v.toString() + "\tBreakend mate is empy (variant type: " + v.getType() + ")");
                            }
                        } else {
                            pwOut.println(v.toString() + "\tBreakend is empy (variant type: " + v.getType() + ")");
                        }
                    } else {
                        pwOut.println(v.toString() + "\tSV is empy (variant type: " + v.getType() + ")");
                    }
                }
            }
        } catch (Exception e) {
            errors.put(track.getType().name(), e.getMessage());
            return false;
        } finally {
            if (pw != null) {
                pw.close();
            }
            if (pwOut != null) {
                pwOut.close();
            }
        }
        return true;
    }

    //------------------------------------------------------------------------//
    //
    // M I S C E L L A N E O U S     F U N C T I O N S
    //
    //------------------------------------------------------------------------//

    private <T> Callable<T> getNamedThread(String name, Callable<T> c) {
        String parentThreadName = Thread.currentThread().getName();
        return () -> {
            Thread.currentThread().setName(parentThreadName + "-" + name);
            return c.call();
        };
    }

    private File getTrackFilename(String name) {
        return getOutDir().resolve(name + "." + System.currentTimeMillis() + ".tsv").toFile();
    }

    private int computeWindowSize() {
        // TODO: compute window size from: genome_size / num_pixels, where num_pixels = 2 * PI * radius
        return 250000;
    }

    private List<Region> getRegionsFromQuery(Map<String, String> query, AlignmentStorageManager alignmentStorageManager)
            throws CatalogException, IOException, StorageEngineException {
        List<Region> inRegions = new ArrayList<>();

        // Input regions management
        if (query.containsKey(REGION.key())) {
            inRegions = Region.parseRegions(query.get(REGION.key()));
        }

        // Get genes and panel genes
        Set<String> geneSet = new HashSet<>();
        if (query.containsKey(GENE.key())) {
            geneSet.addAll(Arrays.asList(query.get(GENE.key()).split(",")));
        }
        if (query.containsKey(PANEL.key())) {
            List<String> panelIds = Arrays.asList(query.get(PANEL.key()).split(","));
            QueryOptions queryOptions = new QueryOptions("include", "genes");
            OpenCGAResult<Panel> panelResult = storageManager.getCatalogManager().getPanelManager().get(study, panelIds, queryOptions,
                    getToken());
            for (Panel panel : panelResult.getResults()) {
                if (CollectionUtils.isNotEmpty(panel.getGenes())) {
                    for (DiseasePanel.GenePanel gene : panel.getGenes()) {
                        geneSet.add(gene.getId());
                    }
                }
            }
        }

        // Merge regions and gene regions
        boolean onlyExons = Boolean.parseBoolean(query.getOrDefault(ONLY_EXONS_PARAM, "false"));
        int offset = Integer.parseInt(query.getOrDefault(OFFSET_DEFAULT, "500"));

        List<Region> outRegions = alignmentStorageManager.mergeRegions(inRegions, new ArrayList<>(geneSet), onlyExons, offset, study,
                getToken());

        // If input regions were not provided by the user, we compute the coverage taking into account the whole genome
        if (CollectionUtils.isEmpty(outRegions)) {
            outRegions = new ArrayList<>();
            //  TODO: get list of chromosomes from cellbase
            for (int i = 0; i <= 22; i++) {
                outRegions.add(new Region(String.valueOf(i)));
            }
        }

        return outRegions;
    }

    private Map<String, String> getRainplotColors() {
        Map<String, String> colors = new HashMap<>();

        colors.put("CA", "royalblue");
        colors.put("GT", "royalblue");
        colors.put("CG", "black");
        colors.put("GC", "black");
        colors.put("CT", "red");
        colors.put("GA", "red");
        colors.put("TA", "grey");
        colors.put("AT", "grey");
        colors.put("TC", "green2");
        colors.put("AG", "green2");
        colors.put("TG", "hotpink");
        colors.put("AC", "hotpink");

        return colors;
    }

    private Map<VariantType, String> getRearrangementColors() {
        Map<VariantType, String> colors = new HashMap<>();

        colors.put(DELETION, "coral2");
        colors.put(BREAKEND, "gray35");
        colors.put(TRANSLOCATION, "gray35");
        colors.put(DUPLICATION, "darkgreen");
        colors.put(INVERSION, "dodgerblue2");

        return colors;
    }

    private Map<Double, String> getCnvColors() {
        Map<Double, String> colors = new LinkedHashMap<>();

        colors.put(0d, "lightcoral");
        colors.put(2d, "lightgrey");
        colors.put(4d, "olivedrab1");
        colors.put(8d, "olivedrab2");
        colors.put(16d, "olivedrab3");
        colors.put(32d, "olivedrab4");
        colors.put(64d, "darkgreen");
        colors.put(1000d, "dodgerblue2");

        return colors;
    }

    private Map<Double, String> getCoverageColors() {
        Map<Double, String> colors = new LinkedHashMap<>();

        colors.put(0d, "lightcoral");
        colors.put(10d, "lightgrey");
        colors.put(20d, "olivedrab1");
        colors.put(30d, "olivedrab2");
        colors.put(40d, "olivedrab3");
        colors.put(50d, "olivedrab4");
        colors.put(75d, "darkgreen");
        colors.put(100d, "dodgerblue2");

        return colors;
    }

    private Map<Double, String> getCoverageRatioColors() {
        Map<Double, String> colors = new LinkedHashMap<>();

        colors.put(0.25d, "olivedrab1");
        colors.put(0.50d, "olivedrab2");
        colors.put(0.75d, "olivedrab3");
        colors.put(1d, "olivedrab4");
        colors.put(1.5d, "darkgreen");
        colors.put(2d, "dodgerblue2");

        return colors;
    }

    private String getColorByRange(double score, Map<Double, String> colors) {
        Iterator<Double> iterator = colors.keySet().iterator();
        Double prev = iterator.next();
        while (iterator.hasNext()) {
            Double curr = iterator.next();
            if (score >= prev && score < curr) {
                return colors.get(prev);

            }
            prev = curr;
        }
        return colors.get(prev);
    }
}
