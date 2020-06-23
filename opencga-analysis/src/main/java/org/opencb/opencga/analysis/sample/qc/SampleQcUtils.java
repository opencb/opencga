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

package org.opencb.opencga.analysis.sample.qc;

import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.opencb.commons.datastore.core.Query;
import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.commons.exec.Command;
import org.opencb.commons.utils.DockerUtils;
import org.opencb.opencga.analysis.variant.manager.VariantStorageManager;
import org.opencb.opencga.analysis.wrappers.PlinkWrapperAnalysis;
import org.opencb.opencga.catalog.db.api.IndividualDBAdaptor;
import org.opencb.opencga.catalog.db.api.ProjectDBAdaptor;
import org.opencb.opencga.catalog.exceptions.CatalogException;
import org.opencb.opencga.catalog.managers.CatalogManager;
import org.opencb.opencga.core.exceptions.ToolException;
import org.opencb.opencga.core.models.common.Status;
import org.opencb.opencga.core.models.family.Family;
import org.opencb.opencga.core.models.individual.Individual;
import org.opencb.opencga.core.models.project.Project;
import org.opencb.opencga.core.models.sample.Sample;
import org.opencb.opencga.core.models.variant.InferredSexReport;
import org.opencb.opencga.core.models.sample.RelatednessReport;
import org.opencb.opencga.core.response.OpenCGAResult;
import org.opencb.opencga.storage.core.exceptions.StorageEngineException;
import org.opencb.opencga.storage.core.variant.adaptors.VariantQueryParam;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.nio.file.Path;
import java.util.*;
import java.util.stream.Collectors;

import static org.opencb.opencga.storage.core.variant.io.VariantWriterFactory.VariantOutputFormat.TPED;

public class SampleQcUtils {

    public static void selectMarkers(String basename, String study, List<String> samples, String maf, Path outDir,
                                     VariantStorageManager storageManager, String token) throws ToolException {
        AbstractMap.SimpleEntry<String, String> outputBinding = new AbstractMap.SimpleEntry<>(outDir.toAbsolutePath().toString(),
                "/data/output");

        // MAF parameter:
        //    - For annotated population studies, e.g.: 1kG_phase3:CEU>0.3
        //    - For cohort, e.g.: cohort:ALL>0.3

        // Apply filter: biallelic variants
        Query query = new Query()
                .append(VariantQueryParam.STUDY.key(), study)
                .append(VariantQueryParam.TYPE.key(), "SNV");

        String gt = samples.stream().map(s -> s + ":0/0,0/1,1/1").collect(Collectors.joining(";"));
        query.put(VariantQueryParam.GENOTYPE.key(), gt);
        //.append(VariantQueryParam.FILTER.key(), "PASS")

        // Export variants in format .tped and .tfam to run PLINK (only autosomal chromosomes)
        File tpedFile = outDir.resolve(basename + ".tped").toFile();
        File tfamFile = outDir.resolve(basename + ".tfam").toFile();
        query.put(VariantQueryParam.REGION.key(), Arrays.asList("1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22".split(",")));
        if (maf.startsWith("cohort:")) {
            query.put(VariantQueryParam.STATS_MAF.key(), maf.substring(7));
        } else {
            query.put(VariantQueryParam.ANNOT_POPULATION_MINOR_ALLELE_FREQUENCY.key(), maf);
        }

        exportData(tpedFile, tfamFile, query, storageManager, token);
        if (tpedFile.exists() && tpedFile.length() > 0) {
            pruneVariants(basename, outputBinding);
        }

        if (!tpedFile.exists() || tpedFile.length() == 0) {
            throw new ToolException("No variants found when exporting data to TPED/TFAM format");
        }
    }

    public static List<String> getSamples(String study, String familyId, CatalogManager catalogManager, String token)
            throws ToolException {
        Set<String> sampleSet = new HashSet<>();

        // Sanity check
        if (StringUtils.isEmpty(familyId)) {
            throw new ToolException("Missing family ID");
        }

        try {
            OpenCGAResult<Family> familyResult = catalogManager.getFamilyManager().get(study, familyId, QueryOptions.empty(), token);

            // Check family result
            if (familyResult.getResults().size() == 0) {
                throw new ToolException("Family not found for family ID '" + familyId + "'");
            }
            if (familyResult.getResults().size() > 1) {
                throw new ToolException("More than one family result for family ID '" + familyId + "'");
            }

            // Get list of individual IDs
            List<String> individualIds = familyResult.first().getMembers().stream().map(m -> m.getId()).collect(Collectors.toList());

            // Populate individual (and sample IDs) from individual IDs
            Query query = new Query(IndividualDBAdaptor.QueryParams.ID.key(), individualIds);
            QueryOptions queryOptions = new QueryOptions(QueryOptions.INCLUDE, "samples.id");

            OpenCGAResult<Individual> individualResult = catalogManager.getIndividualManager().search(study, query, queryOptions, token);
            for (Individual individual : individualResult.getResults()) {
                sampleSet.addAll(individual.getSamples().stream().map(s -> s.getId()).collect(Collectors.toList()));
            }
        } catch (CatalogException e) {
            throw new ToolException(e);
        }

        return sampleSet.stream().collect(Collectors.toList());
    }

    public static String getAssembly(String study, CatalogManager catalogManager, String token) throws CatalogException {
        String assembly = "";
        OpenCGAResult<Project> projectQueryResult;
        projectQueryResult = catalogManager.getProjectManager().get(new Query(ProjectDBAdaptor.QueryParams.STUDY.key(), study),
                new QueryOptions(QueryOptions.INCLUDE, ProjectDBAdaptor.QueryParams.ORGANISM.key()), token);
        if (CollectionUtils.isNotEmpty(projectQueryResult.getResults())) {
            assembly = projectQueryResult.first().getOrganism().getAssembly();
        }
        if (org.apache.commons.lang.StringUtils.isNotEmpty(assembly)) {
            assembly = assembly.toLowerCase();
        }
        return assembly;
    }

    public static RelatednessReport buildRelatednessReport(File file) throws ToolException {
        RelatednessReport relatednessReport = new RelatednessReport();

        // Set method
        relatednessReport.setMethod("IBD");

        BufferedReader reader;
        try {
            reader = new BufferedReader(new FileReader(file));
            // First line is the header
            // FID1         IID1 FID2         IID2 RT    EZ      Z0      Z1      Z2  PI_HAT PHE       DST     PPC   RATIO
            String line = reader.readLine();

            while ((line = reader.readLine()) != null) {
                String[] splits = line.trim().split("\\s+");

                // Create relatedness score
                RelatednessReport.RelatednessScore score = new RelatednessReport.RelatednessScore();
                score.setSampleId1(splits[1]);
                score.setSampleId2(splits[3]);
                score.setInferredRelationship(splits[4]);

                // TODO move to values
                score.setZ0(Double.parseDouble(splits[6]));
                score.setZ1(Double.parseDouble(splits[7]));
                score.setZ2(Double.parseDouble(splits[8]));
                score.setPiHat(Double.parseDouble(splits[9]));

                // Add relatedness score to the report
                relatednessReport.getScores().add(score);
            }
            reader.close();
        } catch (IOException e) {
            throw new ToolException(e);
        }

        return relatednessReport;
    }

    public static Family getFamilyById(String studyId, String familyId, CatalogManager catalogManager, String token)
            throws ToolException {
        OpenCGAResult<Family> familyResult;
        try {
            familyResult = catalogManager.getFamilyManager().get(studyId, familyId, QueryOptions.empty(), token);
        } catch (CatalogException e) {
            throw new ToolException(e);
        }
        if (familyResult.getNumResults() == 0) {
            throw new ToolException("Not found family for ID '" + familyId + "'.");
        }
        return familyResult.first();
    }

    public static Family getFamilyByIndividualId(String studyId, String individualId, CatalogManager catalogManager, String token) throws ToolException {
        Query query = new Query();
        query.put("members", individualId);
        OpenCGAResult<Family> familyResult;
        try {
            familyResult = catalogManager.getFamilyManager().search(studyId, query, QueryOptions.empty(), token);
        } catch (CatalogException e) {
            throw new ToolException(e);
        }
        if (familyResult.getNumResults() == 0) {
            throw new ToolException("None individual found for individual '" + individualId + "'.");
        }
        if (familyResult.getNumResults() > 1) {
            throw new ToolException("More than one family found for individual '" + individualId + "'.");
        }
        return familyResult.first();
    }

    public static Family getFamilyBySampleId(String studyId, String sampleId, CatalogManager catalogManager, String token)
            throws ToolException {
        Individual individual = getIndividualBySampleId(studyId, sampleId, catalogManager, token);
        return getFamilyByIndividualId(studyId, individual.getId(), catalogManager, token);
    }

    public static Individual getIndividualById(String studyId, String individualId, CatalogManager catalogManager, String token)
            throws ToolException {
        OpenCGAResult<Individual> individualResult;
        try {
            individualResult = catalogManager.getIndividualManager().get(studyId, individualId, QueryOptions.empty(),
                    token);
        } catch (CatalogException e) {
            throw new ToolException(e);
        }
        if (individualResult.getNumResults() == 0) {
            throw new ToolException("Not found individual for ID '" + individualId + "'.");
        }
        if (individualResult.getNumResults() > 1) {
            throw new ToolException("More than one individual found for ID '" + individualId + "'.");
        }
        return individualResult.first();
    }

    public static Individual getIndividualBySampleId(String studyId, String sampleId, CatalogManager catalogManager, String token) throws ToolException {
        Query query = new Query();
        query.put("samples", sampleId);
        OpenCGAResult<Individual> individualResult;
        try {
            individualResult = catalogManager.getIndividualManager().search(studyId, query, QueryOptions.empty(),
                    token);
        } catch (CatalogException e) {
            throw new ToolException(e);
        }
        if (individualResult.getNumResults() == 0) {
            throw new ToolException("None individual found for sample '" + sampleId + "'.");
        }
        if (individualResult.getNumResults() > 1) {
            throw new ToolException("More than one individual found for sample '" + sampleId + "'.");
        }
        return individualResult.first();
    }

    public static Sample getValidSampleByIndividualId(String studyId, String individualId, CatalogManager catalogManager, String token)
            throws ToolException {
        Sample sample = null;
        Query query = new Query();
        query.put("individualId", individualId);
        OpenCGAResult<Sample> sampleResult;
        try {
            sampleResult = catalogManager.getSampleManager().search(studyId, query, QueryOptions.empty(), token);
        } catch (CatalogException e) {
            throw new ToolException(e);
        }
        for (Sample individualSample : sampleResult.getResults()) {
            if (Status.READY.equals(individualSample.getInternal().getStatus().getName())) {
                if (sample != null) {
                    throw new ToolException("More than one valid sample found for individual '" + individualId + "'.");
                }
                sample = individualSample;
            }
        }
        return sample;
    }

    public static Sample getValidSampleById(String studyId, String sampleId, CatalogManager catalogManager, String token)
            throws ToolException {
        OpenCGAResult<Sample> sampleResult;
        try {
            sampleResult = catalogManager.getSampleManager().get(studyId, sampleId, QueryOptions.empty(), token);
        } catch (CatalogException e) {
            throw new ToolException(e);
        }
        if (sampleResult.getNumResults() == 0) {
            throw new ToolException("Not found sample for ID '" + sampleId + "'.");
        }
        if (sampleResult.getNumResults() > 1) {
            throw new ToolException("More than one sample found for ID '" + sampleId + "'.");
        }
        if (Status.READY.equals(sampleResult.first().getInternal().getStatus())) {
            throw new ToolException("Sample '" + sampleId + "' is not valid. It must be READY.");
        }
        return sampleResult.first();
    }

    public static List<Sample> getRelativeSamplesByFamilyId(String studyId, String familyId, CatalogManager catalogManager, String token)
            throws ToolException {
        List<Sample> samples = new ArrayList<>();

        OpenCGAResult<Family> familyResult;
        try {
            familyResult = catalogManager.getFamilyManager().get(studyId, familyId, QueryOptions.empty(), token);
        } catch (CatalogException e) {
            throw new ToolException(e);
        }
        if (familyResult.getNumResults() == 0) {
            throw new ToolException("None family found for ID '" + familyId + "'.");
        }
        if (familyResult.getNumResults() > 1) {
            throw new ToolException("More than one family found for ID '" + familyId + "'.");
        }
        Family family = familyResult.first();
        if (CollectionUtils.isEmpty(family.getMembers())) {
            throw new ToolException("Family '" + familyId + "' is empty (i.e., members not found).");
        }

        // Check for valid samples for each individual
        List<String> individualIds = family.getMembers().stream().map(Individual::getId).collect(Collectors.toList());
        for (String individualId : individualIds) {
            // Check valid sample for that individual
            Sample sample = getValidSampleByIndividualId(studyId, individualId, catalogManager, token);
            samples.add(sample);
        }
        return samples;
    }

    public static List<Sample> getRelativeSamplesByIndividualId(String studyId, String individualId, CatalogManager catalogManager,
                                                                String token) throws ToolException {
        // Get the family for that individual
        Family family = getFamilyByIndividualId(studyId, individualId, catalogManager, token);
        // And then search samples for the family members
        return getRelativeSamplesByFamilyId(studyId, family.getId(), catalogManager, token);
    }

    public static List<Sample> getRelativeSamplesBySampleId(String studyId, String sampleId, CatalogManager catalogManager,
                                                            String token) throws ToolException {
        // Get the family for that sample
        Family family = getFamilyBySampleId(studyId, sampleId, catalogManager, token);
        // And then search samples for the family members
        return getRelativeSamplesByFamilyId(studyId, family.getId(), catalogManager, token);
    }

    public static void updateSexReport(List<InferredSexReport> reports, String studyId, CatalogManager catalogManager, String token)
            throws ToolException {
        if (CollectionUtils.isNotEmpty(reports)) {
            for (InferredSexReport inferredSexReport : reports) {
                try {
                    Individual individual = SampleQcUtils.getIndividualBySampleId(studyId, inferredSexReport.getSampleId(), catalogManager, token);
                    inferredSexReport.setIndividualId(individual.getId());
                    inferredSexReport.setReportedSex(individual.getSex().name());
                    inferredSexReport.setReportedKaryotypicSex(individual.getKaryotypicSex().name());
                } catch (ToolException e) {
                    throw new ToolException(e);
                }
            }
        }
    }

    //-------------------------------------------------------------------------
    // P R I V A T E     M E T H O D S
    //-------------------------------------------------------------------------

    private static void exportData(File tpedFile, File tfamFile, Query query, VariantStorageManager storageManager,
                                   String token) throws ToolException {
        try {
            storageManager.exportData(tpedFile.getAbsolutePath(), TPED, null, query, QueryOptions.empty(), token);
        } catch(CatalogException | IOException | StorageEngineException e) {
            throw new ToolException(e);
        }

        if (!tpedFile.exists() || !tfamFile.exists()) {
            throw new ToolException("Something wrong exporting data to TPED/TFAM format");
        }
    }

    private static void pruneVariants(String basename, AbstractMap.SimpleEntry<String, String> outputBinding) throws ToolException {
        // Variant pruning using PLINK in docker
        String plinkParams = "plink --tfile /data/output/" + basename + " --indep 50 5 2 --out /data/output/" + basename;
        try {
            DockerUtils.run(PlinkWrapperAnalysis.PLINK_DOCKER_IMAGE, null, outputBinding, plinkParams, null);
        } catch (IOException e) {
            throw new ToolException(e);
        }
    }

    private static int getNumLines(File file) throws ToolException {
        Command cmd = new Command(file.getAbsolutePath() + " -wl");
        cmd.run();
        String output = cmd.getOutput();
        if (StringUtils.isEmpty(output)) {
            throw new ToolException("Error reading number of lines: " + file.getAbsolutePath());
        }

        return (Integer.parseInt(output.split("\t")[0]) - 1);
    }
}
