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

package org.opencb.opencga.analysis.family.qc;

import com.fasterxml.jackson.core.JsonProcessingException;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.collections4.MapUtils;
import org.apache.commons.io.FileUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.math3.ode.nonstiff.RungeKuttaFieldIntegrator;
import org.opencb.biodata.models.clinical.qc.RelatednessReport;
import org.opencb.biodata.models.clinical.qc.RelatednessScore;
import org.opencb.biodata.models.variant.avro.VariantType;
import org.opencb.commons.datastore.core.Query;
import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.commons.utils.DockerUtils;
import org.opencb.opencga.analysis.ResourceUtils;
import org.opencb.opencga.analysis.individual.qc.IndividualQcUtils;
import org.opencb.opencga.analysis.variant.manager.VariantStorageManager;
import org.opencb.opencga.analysis.variant.relatedness.RelatednessAnalysis;
import org.opencb.opencga.analysis.wrappers.plink.PlinkWrapperAnalysisExecutor;
import org.opencb.opencga.catalog.exceptions.CatalogException;
import org.opencb.opencga.core.common.JacksonUtils;
import org.opencb.opencga.core.exceptions.ToolException;
import org.opencb.opencga.core.models.family.Family;
import org.opencb.opencga.core.models.individual.Individual;
import org.opencb.opencga.core.models.sample.Sample;
import org.opencb.opencga.storage.core.exceptions.StorageEngineException;
import org.opencb.opencga.storage.core.variant.adaptors.VariantQueryParam;
import org.opencb.opencga.storage.core.variant.query.VariantQueryUtils;

import java.io.*;
import java.net.URL;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.*;
import java.util.stream.Collectors;

import static org.opencb.opencga.storage.core.variant.io.VariantWriterFactory.VariantOutputFormat.TPED;

public class IBDComputation {

    private static final String BASENAME = "variants";
    private static final String FILTERED_BASENAME = "variants.filtered";
    private static final String FREQ_FILENAME = BASENAME + ".frq";
    private static final String PRUNE_IN_FILENAME = BASENAME + ".prune.in";

    public static RelatednessReport compute(String study, Family family, List<String> samples, String maf,
                                            Map<String, Map<String, Float>> thresholds, Path resourcesPath, Path outDir,
                                            VariantStorageManager storageManager, String token) throws ToolException {
        // Check resource (variants.frq and variants.prune.in) and download if necessary
        // TODO: download into the folder /analysis/relatedness
        try {
            URL url = new URL(ResourceUtils.URL + "analysis/" + RelatednessAnalysis.ID + "/" + PRUNE_IN_FILENAME);
            ResourceUtils.downloadThirdParty(url, outDir);
        } catch (IOException e) {
            throw new ToolException("Something wrong happened when downloading resource files during the relatedness analysis execution");
        }
        Path pruneInPath = outDir.resolve(PRUNE_IN_FILENAME);
//        Path pruneInPath = resourcesPath.resolve(PRUNE_IN_FILENAME);

        //        if (!resourcesPath.resolve(FREQ_FILENAME).toFile().exists()) {
//            // Download freq file from resources
//        }
        try {
            URL url = new URL(ResourceUtils.URL + "analysis/" + RelatednessAnalysis.ID + "/" + FREQ_FILENAME);
            ResourceUtils.downloadThirdParty(url, outDir);
        } catch (IOException e) {
            throw new ToolException("Something wrong happened when downloading resource files during the relatedness analysis execution");
        }
        Path freqPath = outDir.resolve(FREQ_FILENAME);
//        Path freqPath = resourcesPath.resolve(FREQ_FILENAME);
//        if (!resourcesPath.resolve(PRUNE_IN_FILENAME).toFile().exists()) {
//            // Download prune-in variant file from resources
//        }

        // Export family VCF
        if (family != null) {
            List<String> trio = getTrio(family);
            if (CollectionUtils.isNotEmpty(trio)) {
                exportFamily(BASENAME, study, trio.get(0), trio.get(1), trio.get(2), maf, outDir, storageManager, token);
            } else {
                // Check for valid samples for each individual
                List<String> familySamples = new ArrayList<>();
                List<String> individualIds = family.getMembers().stream().map(Individual::getId).collect(Collectors.toList());
                for (String individualId : individualIds) {
                    // Check valid sample for that individual
                    Sample sample = IndividualQcUtils.getValidSampleByIndividualId(study, individualId, storageManager.getCatalogManager(),
                            token);
                    familySamples.add(sample.getId());
                }
                exportFamily(BASENAME, study, familySamples, maf, outDir, storageManager, token);
            }
        } else {
            exportFamily(BASENAME, study, samples, maf, outDir, storageManager, token);
        }

        // Filter by prune-in variants
        try {
            filterFamilyTpedFile(outDir.resolve(BASENAME + ".tped"), outDir.resolve(FILTERED_BASENAME + ".tped"), pruneInPath);
        } catch (IOException e) {
            throw new ToolException("Something wrong happened when filtering variants during the relatedness analysis execution");
        }

        // Run IBD and return the result file (now autosome-file comprises X chromosome too)
        try {
            FileUtils.copyFile(outDir.resolve(BASENAME + ".tfam").toFile(), outDir.resolve(FILTERED_BASENAME + ".tfam").toFile());
        } catch (IOException e) {
            throw new ToolException("Something wrong happened when copying files during the relatedness analysis execution");
        }
        File outFile = runIBD(FILTERED_BASENAME, freqPath, outDir);

        if (!outFile.exists()) {
            throw new ToolException("Something wrong happened executing relatedness analysis");
        }

        RelatednessReport relatedness = new RelatednessReport()
                .setMethod("PLINK/IBD")
                .setMaf(maf)
                .setScores(parseRelatednessScores(outFile, family, thresholds));

        return relatedness;
    }

    private static List<String> getTrio(Family family) {
        if (family == null) {
            return null;
        }
        if (CollectionUtils.isEmpty(family.getMembers()) || family.getMembers().size() != 3) {
            return null;
        }

        Map<String, Individual> map = new HashMap<>();
        for (Individual member : family.getMembers()) {
            map.put(member.getId(), member);
        }

        for (Individual member : family.getMembers()) {
            if (member.getFather() != null && member.getMother() != null) {
                if (map.containsKey(member.getFather().getId()) && map.containsKey(member.getMother().getId())) {
                    List<String> trio = new LinkedList<>();
                    trio.add(member.getId());
                    trio.add(member.getFather().getId());
                    trio.add(member.getMother().getId());
                    return trio;
                }
            }
        }

        return null;
    }

    private static void exportFamily(String basename, String study, String child, String father, String mother, String maf, Path outDir,
                                     VariantStorageManager storageManager, String token) throws ToolException {
        // Create variant query
        Query query = new Query()
                .append(VariantQueryParam.STUDY.key(), study)
                .append(VariantQueryParam.TYPE.key(), VariantType.SNV)
                .append(VariantQueryParam.SAMPLE.key(), child)
                .append(VariantQueryParam.INCLUDE_SAMPLE.key(), child + "," + father + "," + mother)
                .append(VariantQueryParam.INCLUDE_SAMPLE_DATA.key(), "GT")
                // TODO: update when using whole genome
                .append(VariantQueryParam.REGION.key(), "22");
//                .append(VariantQueryParam.REGION.key(), Arrays.asList("1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22"
//                        .split(",")));

        // MAF parameter:
        //    - For annotated population studies, e.g.: 1000G:ALL>0.3
        //    - For cohort, e.g.: cohort:ALL>0.3
        if (maf.startsWith("cohort:")) {
            query.put(VariantQueryParam.STATS_ALT.key(), maf.substring(7));
        } else {
            query.put(VariantQueryParam.ANNOT_POPULATION_ALTERNATE_FREQUENCY.key(), maf);
        }

        // Create query options
        QueryOptions queryOptions = new QueryOptions().append(QueryOptions.INCLUDE, "id,studies.samples");

        // Export data
        exportData(query, queryOptions, basename, outDir, storageManager, token);
    }

    private static void exportFamily(String basename, String study, List<String> samples, String maf, Path outDir,
                                     VariantStorageManager storageManager, String token) throws ToolException {
        // Create variant query
        Query query = new Query()
                .append(VariantQueryParam.STUDY.key(), study)
                .append(VariantQueryParam.TYPE.key(), VariantType.SNV);

        String gt = samples.stream().map(s -> s + ":0/0,0/1,1/1").collect(Collectors.joining(";"));
        query.put(VariantQueryParam.GENOTYPE.key(), gt);
        //.append(VariantQueryParam.FILTER.key(), "PASS")

        // TODO: update when using whole genome
        //query.put(VariantQueryParam.REGION.key(), Arrays.asList("1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22".split(",")));
        query.put(VariantQueryParam.REGION.key(), "22");

        // MAF parameter:
        //    - For annotated population studies, e.g.: 1000G:ALL>0.3
        //    - For cohort, e.g.: cohort:ALL>0.3
        if (maf.startsWith("cohort:")) {
            query.put(VariantQueryParam.STATS_ALT.key(), maf.substring(7));
        } else {
            query.put(VariantQueryParam.ANNOT_POPULATION_ALTERNATE_FREQUENCY.key(), maf);
        }

        // Create query options
        QueryOptions queryOptions = new QueryOptions().append(QueryOptions.INCLUDE, "id,studies.samples");

        // Export data
        exportData(query, queryOptions, basename, outDir, storageManager, token);
    }

    private static void exportData(Query query, QueryOptions queryOptions, String basename, Path outDir,
                                   VariantStorageManager storageManager, String token) throws ToolException {
        System.out.println(">>>> export, query = " + query.toJson());
        System.out.println(">>>> export, query options = " + queryOptions.toJson());

        File tpedFile = outDir.resolve(basename + ".tped").toFile();
        File tfamFile = outDir.resolve(basename + ".tfam").toFile();

        // Export data
        try {
            storageManager.exportData(tpedFile.getAbsolutePath(), TPED, null, query, queryOptions, token);
        } catch (CatalogException | StorageEngineException e) {
            throw new ToolException(e);
        }

        // Sanity check
        if (!tpedFile.exists() || tpedFile.length() == 0) {
            throw new ToolException("No variants found when exporting data to TPED/TFAM format");
        }
        if (!tfamFile.exists()) {
            throw new ToolException("Something wrong exporting data to TPED/TFAM format");
        }
    }

    public static void filterFamilyTpedFile(Path tPedPath, Path tPedFilteredPath, Path pruneInPath) throws IOException {
        // Init map with prune-in variants
        Map<String, Integer> pruneInMap = new HashMap<>();
        BufferedReader br = org.opencb.commons.utils.FileUtils.newBufferedReader(pruneInPath);
        String line = br.readLine();
        if (line != null) {
            pruneInMap.put(line, 0);
        }
        while (line != null) {
            pruneInMap.put(line, 0);
            line = br.readLine();
        }
        br.close();

        // Get number of GT
        br = org.opencb.commons.utils.FileUtils.newBufferedReader(tPedPath);
        line = br.readLine();
        String[] split = line.split("\t");
        int numGT = split.length - 4;
        br.close();

        // Filter TPED file using prune-in map
        BufferedWriter bw = org.opencb.commons.utils.FileUtils.newBufferedWriter(tPedFilteredPath);
        br = org.opencb.commons.utils.FileUtils.newBufferedReader(tPedPath);
        line = br.readLine();
        while (line != null) {
            split = line.split("\t");
            if (pruneInMap.containsKey(split[1])) {
                bw.write(line);
                bw.write("\n");

                // Mark in map as written !!
                pruneInMap.put(split[1], 1);

                if (numGT == 0) {
                    numGT = split.length - 4;
                }
            }

            line = br.readLine();
        }
        br.close();

        // Write remaining prune-in variants
        for (Map.Entry<String, Integer> entry : pruneInMap.entrySet()) {
            if (entry.getValue() == 0) {
                split = entry.getKey().split(":");
                line = split[0] + "\t" + entry.getKey() + "\t0\t" + split[1];
                for (int i = 0; i < numGT; i++) {
                    line += ("\t0");
                }
                bw.write(line);
                bw.write("\n");
            }
        }
        bw.close();
    }

    private static File runIBD(String basename, Path freqPath, Path outDir) throws ToolException {
        // Input bindings
        List<AbstractMap.SimpleEntry<String, String>> inputBindings = new ArrayList<>();
        inputBindings.add(new AbstractMap.SimpleEntry<>(freqPath.getParent().toString(), "/input"));

        // Output bindings
        AbstractMap.SimpleEntry<String, String> outputBinding = new AbstractMap.SimpleEntry<>(outDir.toAbsolutePath().toString(),
                "/output");

        // Run IBD using PLINK in docker
        String plinkParams = "plink1.9 --tfile /output/" + basename + " --genome rel-check --read-freq /input/" + FREQ_FILENAME
                + " --out /output/" + basename;
        try {
            PlinkWrapperAnalysisExecutor plinkExecutor = new PlinkWrapperAnalysisExecutor();
            DockerUtils.run(plinkExecutor.getDockerImageName(), inputBindings, outputBinding, plinkParams, null);
        } catch (IOException e) {
            throw new ToolException(e);
        }

        // Check output file
        File outFile = new File(outputBinding.getKey() + "/" + basename + ".genome");
        if (!outFile.exists()) {
            throw new ToolException("Something wrong executing relatedness analysis (i.e., IBD/IBS computation) in PLINK docker.");
        }

        return outFile;
    }

    @Deprecated
    private static void createReadFreqFile(String tpedFilename, String mafFilename) throws ToolException {
        try (FileWriter f = new FileWriter(mafFilename, true);
             PrintWriter p = new PrintWriter(new BufferedWriter(f))) {

            p.println("CHR\tSNP\tA1\tA2\tMAF\tNCHROBS");

            FileInputStream fis = new FileInputStream(tpedFilename);
            Scanner sc = new Scanner(fis);
            while(sc.hasNextLine()) {
                String[] splits = sc.nextLine().split("\t");
                String[] varSplits = splits[1].split(":");

                int a1 = 0;
                int a2 = 0;
                for (int i = 4; i < splits.length; i++) {
                    if (splits[i].equals(varSplits[2])) {
                        a1++;
                    } else if (splits[i].equals(varSplits[3])) {
                        a2++;
                    }
                }
                p.println(splits[0] + "\t" + splits[1] + "\t" + varSplits[2] + "\t" + varSplits[3] + "\t" + (1.0d * a2 / (a1 + a2)) + "\t"
                        + (a1 + a2));
            }
        } catch (IOException e) {
            throw new ToolException(e);
        }
    }

    public static List<RelatednessScore> parseRelatednessScores(File file, Family family, Map<String, Map<String, Float>> thresholds)
            throws ToolException {
        List<RelatednessScore> scores = new ArrayList<>();

        String line;
        BufferedReader reader;
        try {
            reader = new BufferedReader(new FileReader(file));

            // Skip first line is the header
            // FID1         IID1 FID2         IID2 RT    EZ      Z0      Z1      Z2  PI_HAT PHE       DST     PPC   RATIO
            reader.readLine();

            while ((line = reader.readLine()) != null) {
                String[] splits = line.trim().split("\\s+");

                // Create relatedness score
                RelatednessScore score = new RelatednessScore();
                score.setSampleId1(splits[1]);
                score.setSampleId2(splits[3]);

                // Get reported relationship
                Family.FamiliarRelationship reportedRelationship = Family.FamiliarRelationship.UNKNOWN;
                if (family != null && MapUtils.isNotEmpty(family.getRoles()) && family.getRoles().containsKey(score.getSampleId1())) {
                    if (family.getRoles().get(score.getSampleId1()).containsKey(score.getSampleId2())) {
                        reportedRelationship = family.getRoles().get(score.getSampleId1()).get(score.getSampleId2());
                    }
                }
                score.setReportedRelationship(reportedRelationship.name());

                // Get inferred relationships and validation
                String validation = null;
                List<Family.FamiliarRelationship> inferredRelationships = inferredRelationship(splits[6], splits[7], splits[8], splits[9],
                        thresholds);
                // Inferred relationship list has always a minimum size of 1
                if (inferredRelationships.size() == 1
                        && ((inferredRelationships.get(0) == Family.FamiliarRelationship.UNKNOWN) ||
                        (inferredRelationships.get(0) == Family.FamiliarRelationship.OTHER))) {
                    score.setInferredRelationship(inferredRelationships.get(0).name());
                    validation = "UNKNOWN";
                } else {
                    score.setInferredRelationship(StringUtils.join(inferredRelationships, ", "));
                    if (reportedRelationship == Family.FamiliarRelationship.UNKNOWN
                            || reportedRelationship == Family.FamiliarRelationship.OTHER) {
                        validation = "UNKNOWN";
                    } else {
                        for (Family.FamiliarRelationship inferredRelationship : inferredRelationships) {
                            if (inferredRelationship == reportedRelationship) {
                                validation = "PASS";
                                break;
                            }
                        }
                        if (validation == null) {
                            validation = "FAIL";
                        }
                    }
                }
                score.setValidation(validation);

                Map<String, Object> values = new LinkedHashMap<>();
                values.put("RT", splits[4]);
                values.put("ez", splits[5]);
                values.put("z0", splits[6]);
                values.put("z1", splits[7]);
                values.put("z2", splits[8]);
                values.put("PiHat", splits[9]);
                score.setValues(values);

                // Add relatedness score to the report
                scores.add(score);
            }
            reader.close();
        } catch (IOException e) {
            throw new ToolException(e);
        }

        return scores;
    }

    public static List<Family.FamiliarRelationship> inferredRelationship(String z0, String z1, String z2, String piHat,
                                                                         Map<String, Map<String, Float>> thresholds) {
        float z0Score;
        float z1Score;
        float z2Score;
        float piHatScore;

        // Sanity check
        if (MapUtils.isEmpty(thresholds)) {
            return Collections.singletonList(Family.FamiliarRelationship.UNKNOWN);
        }

        try {
            z0Score = Float.parseFloat(z0);
        } catch (NumberFormatException e) {
            return Collections.singletonList(Family.FamiliarRelationship.UNKNOWN);
        }
        try {
            z1Score = Float.parseFloat(z1);
        } catch (NumberFormatException e) {
            return Collections.singletonList(Family.FamiliarRelationship.UNKNOWN);
        }
        try {
            z2Score = Float.parseFloat(z2);
        } catch (NumberFormatException e) {
            return Collections.singletonList(Family.FamiliarRelationship.UNKNOWN);
        }
        try {
            piHatScore = Float.parseFloat(piHat);
        } catch (NumberFormatException e) {
            return Collections.singletonList(Family.FamiliarRelationship.UNKNOWN);
        }

        List<Family.FamiliarRelationship> result = new ArrayList<>();
        for (Map.Entry<String, Map<String, Float>> entry : thresholds.entrySet()) {
            Map<String, Float> scores = entry.getValue();
            if (z0Score >= scores.get("minZ0") && z0Score <= scores.get("maxZ0")
                    && z1Score >= scores.get("minZ1") && z1Score <= scores.get("maxZ1")
                    && z2Score >= scores.get("minZ2") && z2Score <= scores.get("maxZ2")
                    && piHatScore >= scores.get("minPiHat") && piHatScore <= scores.get("maxPiHat")) {
                try {
                    result.add(Family.FamiliarRelationship.valueOf(entry.getKey()));
                } catch (Exception e) {
                    // Skip
                }
            }
        }

        return CollectionUtils.isEmpty(result) ? Collections.singletonList(Family.FamiliarRelationship.UNKNOWN) : result;
    }
}
