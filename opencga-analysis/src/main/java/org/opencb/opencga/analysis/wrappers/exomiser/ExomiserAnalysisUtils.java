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

package org.opencb.opencga.analysis.wrappers.exomiser;

import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.opencb.biodata.models.clinical.Disorder;
import org.opencb.biodata.models.clinical.Phenotype;
import org.opencb.biodata.models.clinical.pedigree.Member;
import org.opencb.biodata.models.clinical.pedigree.Pedigree;
import org.opencb.biodata.models.pedigree.IndividualProperty;
import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.opencga.analysis.ConfigurationUtils;
import org.opencb.opencga.analysis.individual.qc.IndividualQcUtils;
import org.opencb.opencga.analysis.variant.manager.VariantStorageManager;
import org.opencb.opencga.catalog.exceptions.CatalogException;
import org.opencb.opencga.catalog.exceptions.ResourceException;
import org.opencb.opencga.catalog.managers.CatalogManager;
import org.opencb.opencga.catalog.managers.FamilyManager;
import org.opencb.opencga.catalog.utils.ResourceManager;
import org.opencb.opencga.core.exceptions.ToolException;
import org.opencb.opencga.core.models.clinical.ClinicalAnalysis;
import org.opencb.opencga.core.models.family.Family;
import org.opencb.opencga.core.models.individual.Individual;
import org.opencb.opencga.storage.core.exceptions.StorageEngineException;
import org.opencb.opencga.storage.core.variant.adaptors.VariantQuery;
import org.opencb.opencga.storage.core.variant.io.VariantWriterFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.io.PrintWriter;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.*;

import static org.opencb.commons.utils.FileUtils.copyFile;
import static org.opencb.opencga.analysis.wrappers.exomiser.ExomiserWrapperAnalysisExecutor.*;


public class ExomiserAnalysisUtils  {
    // It must match the resources key in the exomiser/tool section in the configuration file
    public static final String HG19_RESOURCE_KEY = "HG19";
    public static final String HG38_RESOURCE_KEY = "HG38";
    public static final String PHENOTYPE_RESOURCE_KEY = "PHENOTYPE";

    private static Logger logger = LoggerFactory.getLogger(ExomiserAnalysisUtils.class);

    public static void prepareResources(String sampleId, String studyId, String clinicalAnalysisType, String exomiserVersion,
                                        CatalogManager catalogManager, String token, Path outDir, Path openCgaHome) throws ToolException {
        // Check HPOs, it will use a set to avoid duplicate HPOs,
        // and it will check both phenotypes and disorders
        logger.info("Checking individual for sample {} in study {}", sampleId, studyId);
        Set<String> hpos = new HashSet<>();
        Individual individual = IndividualQcUtils.getIndividualBySampleId(studyId, sampleId, catalogManager, token);

        // Set father and mother if necessary (family ?)
        if (individual.getFather() != null && StringUtils.isNotEmpty(individual.getFather().getId())) {
            individual.setFather(IndividualQcUtils.getIndividualById(studyId, individual.getFather().getId(), catalogManager, token));
        }
        if (individual.getMother() != null && StringUtils.isNotEmpty(individual.getMother().getId())) {
            individual.setMother(IndividualQcUtils.getIndividualById(studyId, individual.getMother().getId(), catalogManager, token));
        }

        logger.info("Individual found: {}", individual.getId());
        if (CollectionUtils.isNotEmpty(individual.getPhenotypes())) {
            for (Phenotype phenotype : individual.getPhenotypes()) {
                if (phenotype.getId().startsWith("HP:")) {
                    hpos.add(phenotype.getId());
                }
            }
        }
        if (CollectionUtils.isNotEmpty(individual.getDisorders())) {
            for (Disorder disorder : individual.getDisorders()) {
                if (disorder.getId().startsWith("HP:")) {
                    hpos.add(disorder.getId());
                }
            }
        }

        if (CollectionUtils.isEmpty(hpos)) {
            throw new ToolException("Missing phenotypes, i.e. HPO terms, for individual/sample (" + individual.getId() + "/" + sampleId
                    + ")");
        }
        logger.info("Getting HPO for individual {}: {}", individual.getId(), StringUtils.join(hpos, ","));

        List<String> samples = new ArrayList<>();
        samples.add(sampleId);

        // Check multi-sample (family) analysis
        Pedigree pedigree = null;
        if (ClinicalAnalysis.Type.FAMILY.name().equals(clinicalAnalysisType)) {
            if (individual.getMother() != null && individual.getMother().getId() != null
                    && individual.getFather() != null && individual.getFather().getId() != null) {
                Family family = IndividualQcUtils.getFamilyByIndividualId(studyId, individual.getId(), catalogManager, token);
                if (family != null) {
                    pedigree = FamilyManager.getPedigreeFromFamily(family, individual.getId());
                }

                if (pedigree != null) {
                    if (individual.getFather() != null) {
                        samples.add(individual.getFather().getSamples().get(0).getId());
                    }
                    if (individual.getMother() != null) {
                        samples.add(individual.getMother().getSamples().get(0).getId());
                    }
                    // Create the Exomiser pedigree file
                    createPedigreeFile(family, pedigree, outDir);
                }
            }
        }
        // Create the Exomiser sample file
        createSampleFile(sampleId, individual, hpos, pedigree, outDir);

        // Copy the analysis template file
        Path srcPath = Paths.get(openCgaHome.toAbsolutePath().toString(), ResourceManager.ANALYSIS_DIRNAME, ExomiserWrapperAnalysis.ID,
                exomiserVersion, EXOMISER_ANALYSIS_TEMPLATE_FILENAME);
        Path destPath = outDir.resolve(EXOMISER_ANALYSIS_TEMPLATE_FILENAME);
        try {
            copyFile(srcPath.toFile(), destPath.toFile());
        } catch (IOException e) {
            throw new ToolException("Error copying Exomiser analysis file '" + srcPath + "' to '" + destPath + "'", e);
        }

        // Copy the application.properties and update data according to Exomiser version
        srcPath = Paths.get(openCgaHome.toAbsolutePath().toString(), ResourceManager.ANALYSIS_DIRNAME, ExomiserWrapperAnalysis.ID,
                exomiserVersion, EXOMISER_PROPERTIES_TEMPLATE_FILENAME);
        destPath = outDir.resolve(EXOMISER_PROPERTIES_TEMPLATE_FILENAME);
        try {
            copyFile(srcPath.toFile(), destPath.toFile());
        } catch (IOException e) {
            throw new ToolException("Error copying Exomiser properties file '" + srcPath + "' to '" + destPath + "'", e);
        }

        // Copy the output options
        srcPath = Paths.get(openCgaHome.toAbsolutePath().toString(), ResourceManager.ANALYSIS_DIRNAME, ExomiserWrapperAnalysis.ID,
                exomiserVersion, EXOMISER_OUTPUT_OPTIONS_FILENAME);
        destPath = outDir.resolve(EXOMISER_OUTPUT_OPTIONS_FILENAME);
        try {
            copyFile(srcPath.toFile(), destPath.toFile());
        } catch (IOException e) {
            throw new ToolException("Error copying Exomiser output options file '" + srcPath + "' to '" + destPath + "'", e);
        }
    }

    public static void exportVariants(String sampleId, String studyId, String clinicalAnalysisType, Path outDir,
                                      VariantStorageManager variantStorageManager, String token) throws ToolException {
        CatalogManager catalogManager = variantStorageManager.getCatalogManager();
        Individual individual = IndividualQcUtils.getIndividualBySampleId(studyId, sampleId, catalogManager, token);

        // Set father and mother if necessary (family ?)
        if (individual.getFather() != null && StringUtils.isNotEmpty(individual.getFather().getId())) {
            individual.setFather(IndividualQcUtils.getIndividualById(studyId, individual.getFather().getId(), catalogManager, token));
        }
        if (individual.getMother() != null && StringUtils.isNotEmpty(individual.getMother().getId())) {
            individual.setMother(IndividualQcUtils.getIndividualById(studyId, individual.getMother().getId(), catalogManager, token));
        }

        List<String> samples = new ArrayList<>();
        samples.add(sampleId);

        // Check multi-sample (family) analysis
        if (ClinicalAnalysis.Type.FAMILY.name().equals(clinicalAnalysisType)) {
            if (individual.getMother() != null && individual.getMother().getId() != null
                    && individual.getFather() != null && individual.getFather().getId() != null) {
                Pedigree pedigree = null;
                Family family = IndividualQcUtils.getFamilyByIndividualId(studyId, individual.getId(), catalogManager, token);
                if (family != null) {
                    pedigree = FamilyManager.getPedigreeFromFamily(family, individual.getId());
                }

                if (pedigree != null) {
                    if (individual.getFather() != null) {
                        samples.add(individual.getFather().getSamples().get(0).getId());
                    }
                    if (individual.getMother() != null) {
                        samples.add(individual.getMother().getSamples().get(0).getId());
                    }
                }
            }
        }

        // Export data into VCF file
        Path vcfPath = getVcfPath(sampleId, outDir);

        VariantQuery query = new VariantQuery()
                .study(studyId)
                .sample(sampleId)
                .includeSample(samples)
                .includeSampleData("GT")
                .unknownGenotype("./.")
                .append("includeAllFromSampleIndex", true);

        QueryOptions queryOptions = new QueryOptions(QueryOptions.INCLUDE, "id,studies.samples");

        logger.info("Exomiser exports variants using the query: {}", query.toJson());
        logger.info("Exomiser exports variants using the query options: {}", queryOptions.toJson());

        try {
            variantStorageManager.exportData(vcfPath.toString(), VariantWriterFactory.VariantOutputFormat.VCF_GZ, null, query,
                    queryOptions, token);
        } catch (StorageEngineException | CatalogException e) {
            throw new ToolException(e);
        }
    }

    public static void checkResources(String inputVersion, String study, CatalogManager catalogManager, String token, Path openCgaHome)
            throws ToolException, CatalogException, ResourceException {
        ResourceManager resourceManager = new ResourceManager(openCgaHome);

        // Resources keys (dependent on assembly)
        List<String> resourceKeys = new ArrayList<>();
        resourceKeys.add(PHENOTYPE_RESOURCE_KEY);

        // Check assembly
        checkAssembly(study, catalogManager, token);

        // Check exomiser version
        String exomiserVersion = inputVersion;
        if (StringUtils.isEmpty(exomiserVersion)) {
            // Missing exomiser version use the default one
            exomiserVersion = ConfigurationUtils.getToolDefaultVersion(ExomiserWrapperAnalysis.ID, catalogManager.getConfiguration());
        } else {
            exomiserVersion = inputVersion;
        }

        // Check resources
        for (String resouceKey : resourceKeys) {
            String resource = ConfigurationUtils.getToolResource(ExomiserWrapperAnalysis.ID, exomiserVersion, resouceKey,
                    catalogManager.getConfiguration());
            resourceManager.checkResourcePath(ExomiserWrapperAnalysis.ID, resource);
        }
    }

    public static Path getSamplePath(String sampleId, Path outDir) {
        return outDir.resolve(sampleId + ".yml");
    }

    public static Path getVcfPath(String sampleId, Path outDir) {
        return outDir.resolve(sampleId + ".vcf.gz");
    }

    public static String checkAssembly(String studyId, CatalogManager catalogManager, String token) throws CatalogException, ToolException {
        // Check assembly
        String assembly = IndividualQcUtils.getAssembly(studyId, catalogManager, token);
        if (assembly.equalsIgnoreCase("GRCh38") || assembly.equalsIgnoreCase("HG38")) {
            return "hg38";
        } else if (assembly.equalsIgnoreCase("GRCh37") || assembly.equalsIgnoreCase("HG19")) {
            return "hg19";
        }
        throw new ToolException("Invalid assembly '" + assembly + "'. Supported assemblies are: GRCh38, GRCh37, HG38 and HG19");
    }

    private static File createPedigreeFile(Family family, Pedigree pedigree, Path outDir) throws ToolException {
        Map<String, String> fromIndividualToSample = new HashMap<>();
        for (Individual member : family.getMembers()) {
            if (CollectionUtils.isNotEmpty(member.getSamples())) {
                fromIndividualToSample.put(member.getId(), member.getSamples().get(0).getId());
            }
        }

        File pedigreeFile = outDir.resolve(fromIndividualToSample.get(pedigree.getProband().getId() + ".ped")).toFile();
        try {
            PrintWriter pw = new PrintWriter(pedigreeFile);

            String probandId = fromIndividualToSample.get(pedigree.getProband().getId());

            String fatherId = "0";
            if (pedigree.getProband().getFather() != null) {
                fatherId = fromIndividualToSample.get(pedigree.getProband().getFather().getId());
            }

            String motherId = "0";
            if (pedigree.getProband().getMother() != null) {
                motherId = fromIndividualToSample.get(pedigree.getProband().getMother().getId());
            }

            // Proband
            pw.write(family.getId()
                    + "\t" + probandId
                    + "\t" + fatherId
                    + "\t" + motherId
                    + "\t" + getPedigreeSex(pedigree.getProband())
                    + "\t2\n");

            // Father
            if (!fatherId.equals("0")) {
                pw.write(family.getId() + "\t" + fatherId + "\t0\t0\t1\t0\n");
            }

            // Mother
            if (!motherId.equals("0")) {
                pw.write(family.getId() + "\t" + motherId + "\t0\t0\t2\t0\n");
            }

            // Close file
            pw.close();
        } catch (IOException e) {
            throw new ToolException("Error writing Exomiser pedigree file", e);
        }

        return pedigreeFile;
    }

    private static int getPedigreeSex(Member member) {
        if (member.getSex() != null) {
            if (member.getSex().getSex() == IndividualProperty.Sex.MALE) {
                return 1;
            } if (member.getSex().getSex() == IndividualProperty.Sex.FEMALE) {
                return 2;
            }
        }
        return 0;
    }

    private static File createSampleFile(String sampleId, Individual individual, Set<String> hpos, Pedigree pedigree, Path outDir)
            throws ToolException {
        File sampleFile = getSamplePath(sampleId, outDir).toFile();
        try {
            PrintWriter pw = new PrintWriter(sampleFile);
            pw.write("# a v1 phenopacket describing an individual https://phenopacket-schema.readthedocs.io/en/1.0.0/phenopacket.html\n");
            pw.write("---\n");
            pw.write("id: " + sampleId + "\n");
            String prefix = "";
            if (pedigree != null) {
                prefix = "  ";
                pw.write("proband:\n");
            }
            pw.write(prefix + "subject:\n");
            pw.write(prefix + "  id: " + sampleId + "\n");
            if (individual.getSex() != null) {
                pw.write(prefix + "  sex: " + individual.getSex().getSex().name() + "\n");
            }
            pw.write(prefix + "phenotypicFeatures:\n");
            for (String hpo : hpos) {
                pw.write(prefix + "  - type:\n");
                pw.write(prefix + "      id: " + hpo + "\n");
            }
            if (pedigree != null) {
                pw.write("pedigree:\n");
                pw.write("  persons:\n");

                // Proband
                pw.write("    - individualId: " + sampleId + "\n");
                if (pedigree.getProband().getFather() != null) {
                    pw.write("      paternalId: " + individual.getFather().getSamples().get(0).getId() + "\n");
                }
                if (pedigree.getProband().getMother() != null) {
                    pw.write("      maternalId: " + individual.getMother().getSamples().get(0).getId() + "\n");
                }
                if (pedigree.getProband().getSex() != null) {
                    pw.write("      sex: " + pedigree.getProband().getSex().getSex().name() + "\n");
                }
                pw.write("      affectedStatus: AFFECTED\n");

                // Father
                if (pedigree.getProband().getFather() != null) {
                    pw.write("    - individualId: " + individual.getFather().getSamples().get(0).getId() + "\n");
                    if (pedigree.getProband().getFather().getSex() != null) {
                        pw.write("      sex: " + pedigree.getProband().getFather().getSex().getSex().name() + "\n");
                    }
//                    pw.write("      - affectedStatus:" + AffectationStatus + "\n");
                }

                // Mother
                if (pedigree.getProband().getMother() != null) {
                    pw.write("    - individualId: " + individual.getMother().getSamples().get(0).getId() + "\n");
                    if (pedigree.getProband().getMother().getSex() != null) {
                        pw.write("      sex: " + pedigree.getProband().getMother().getSex().getSex().name() + "\n");
                    }
//                    pw.write("      - affectedStatus:" + AffectationStatus + "\n");
                }
            }

            // Close file
            pw.close();
        } catch (IOException e) {
            throw new ToolException("Error writing Exomiser sample file", e);
        }
        return sampleFile;
    }
}
