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

package org.opencb.opencga.analysis.family;

import org.opencb.opencga.catalog.exceptions.CatalogException;
import org.opencb.opencga.core.common.GitRepositoryState;
import org.opencb.opencga.core.exceptions.ToolException;
import org.opencb.opencga.core.models.individual.Individual;
import org.opencb.opencga.core.tools.annotations.ToolExecutor;
import org.opencb.opencga.core.tools.family.PedigreeGraphAnalysisExecutor;
import org.opencb.opencga.storage.core.exceptions.StorageEngineException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;

@ToolExecutor(id="opencga-local", tool = PedigreeGraphAnalysis.ID,
        framework = ToolExecutor.Framework.LOCAL, source = ToolExecutor.Source.MONGODB)
public class PedigreeGraphLocalAnalysisExecutor extends PedigreeGraphAnalysisExecutor {

    public final static String R_DOCKER_IMAGE = "opencb/opencga-ext-tools:" + GitRepositoryState.get().getBuildVersion();

    private Path opencgaHome;

    private Logger logger = LoggerFactory.getLogger(this.getClass());

    @Override
    public void run() throws ToolException, CatalogException, IOException, StorageEngineException {
        opencgaHome = Paths.get(getExecutorParams().getString("opencgaHome"));
         // Run R script for fitting signature
        computeRScript();
    }

    private void computeRScript() throws IOException, ToolException, CatalogException {
        for (Individual member : getFamily().getMembers()) {
            logger.info("Member: " + member.toString());
        }
//        File cataloguesFile = getOutDir().resolve(CATALOGUES_FILENAME_DEFAULT).toFile();
//        if (!cataloguesFile.exists()) {
//            // Get counts from sample
//            CatalogManager catalogManager = getVariantStorageManager().getCatalogManager();
//            // Check sample
//            String study = catalogManager.getStudyManager().get(getStudy(), QueryOptions.empty(), getToken()).first().getFqn();
//            OpenCGAResult<Sample> sampleResult = catalogManager.getSampleManager().get(study, getSample(), QueryOptions.empty(),
//                    getToken());
//            if (sampleResult.getNumResults() != 1) {
//                throw new ToolException("Unable to compute mutational signature analysis. Sample '" + getSample() + "' not found");
//            }
//            Sample sample = sampleResult.first();
//            logger.info("Searching catalogue counts from quality control for sample " + getSample());
//            if (sample.getQualityControl() != null && sample.getQualityControl().getVariant() != null
//                    && CollectionUtils.isNotEmpty(sample.getQualityControl().getVariant().getSignatures())) {
//                logger.info("Searching in " + sample.getQualityControl().getVariant().getSignatures().size() + " signatures");
//                for (Signature signature : sample.getQualityControl().getVariant().getSignatures()) {
//                    logger.info("Matching ? " + getQueryId() + " vs " + signature.getId());
//                    if (getQueryId().equals(signature.getId())) {
//                        // Write catalogue file
//                        try (PrintWriter pw = new PrintWriter(cataloguesFile)) {
//                            pw.println(getSample());
//                            for (Signature.GenomeContextCount count : signature.getCounts()) {
//                                pw.println(count.getContext() + "\t" + count.getTotal());
//                            }
//                            pw.close();
//                        } catch (Exception e) {
//                            throw new ToolException("Error writing catalogue output file: " + cataloguesFile.getName(), e);
//                        }
//                        logger.info("Found catalogue {} and written in {}", signature.getId(), cataloguesFile.getAbsolutePath());
//                        break;
//                    }
//                }
//            }
//            if (!cataloguesFile.exists()) {
//                throw new ToolException("Could not find mutational signagure catalogue (counts) file: " + cataloguesFile.getName());
//            }
//        }
//
//        List<AbstractMap.SimpleEntry<String, String>> inputBindings = new ArrayList<>();
//        inputBindings.add(new AbstractMap.SimpleEntry<>(getOutDir().toAbsolutePath().toString(), "/data/input"));
//        if (StringUtils.isNotEmpty(getSignaturesFile())) {
//            File signaturesFile = new File(getSignaturesFile());
//            if (signaturesFile.exists()) {
//                inputBindings.add(new AbstractMap.SimpleEntry<>(signaturesFile.getParent(), "/data/signaturesFile"));
//            }
//        }
//        if (StringUtils.isNotEmpty(getRareSignaturesFile())) {
//            File rareSignaturesFile = new File(getRareSignaturesFile());
//            if (rareSignaturesFile.exists()) {
//                inputBindings.add(new AbstractMap.SimpleEntry<>(rareSignaturesFile.getParent(), "/data/rareSignaturesFile"));
//            }
//        }
//        AbstractMap.SimpleEntry<String, String> outputBinding = new AbstractMap.SimpleEntry<>(getOutDir()
//                .toAbsolutePath().toString(), "/data/output");
//        StringBuilder scriptParams = new StringBuilder("R CMD Rscript --vanilla ")
//                .append("/opt/opencga/signature.tools.lib/scripts/signatureFit")
//                .append(" --catalogues=/data/input/").append(cataloguesFile.getName())
//                .append(" --outdir=/data/output");
//        if (StringUtils.isNotEmpty(getFitMethod())) {
//            scriptParams.append(" --fitmethod=").append(getFitMethod());
//        }
//        if (StringUtils.isNotEmpty(getSigVersion())) {
//            scriptParams.append(" --sigversion=").append(getSigVersion());
//        }
//        if (StringUtils.isNotEmpty(getOrgan())) {
//            scriptParams.append(" --organ=").append(getOrgan());
//        }
//        if (getThresholdPerc() != null) {
//            scriptParams.append(" --thresholdperc=").append(getThresholdPerc());
//        }
//        if (getThresholdPval() != null) {
//            scriptParams.append(" --thresholdpval=").append(getThresholdPval());
//        }
//        if (getMaxRareSigs() != null) {
//            scriptParams.append(" --maxraresigs=").append(getMaxRareSigs());
//        }
//        if (getnBoot() != null) {
//            scriptParams.append(" -b --nboot=").append(getnBoot());
//        }
//        if (StringUtils.isNotEmpty(getSignaturesFile()) && new File(getSignaturesFile()).exists()) {
//            scriptParams.append(" --signaturesfile=/data/signaturesFile/").append(new File(getSignaturesFile()).getName());
//        }
//        if (StringUtils.isNotEmpty(getRareSignaturesFile()) && new File(getRareSignaturesFile()).exists()) {
//            scriptParams.append(" --raresignaturesfile=/data/rareSignaturesFile/").append(new File(getRareSignaturesFile()).getName());
//        }
//        switch (getAssembly()) {
//            case "GRCh37": {
//                scriptParams.append(" --genomev=hg19");
//                break;
//            }
//            case "GRCh38": {
//                scriptParams.append(" --genomev=hg38");
//                break;
//            }
//        }
//
//        String cmdline = DockerUtils.run(R_DOCKER_IMAGE, inputBindings, outputBinding, scriptParams.toString(),
//                null);
//        logger.info("Docker command line: " + cmdline);
//
//        // Check fitting file before parsing and creating the mutational signature fitting data model
//        File signatureCoeffsFile = getOutDir().resolve(SIGNATURE_COEFFS_FILENAME).toFile();
//        if (!signatureCoeffsFile.exists()) {
//            throw new ToolExecutorException("Something wrong happened: signature coeffs. file " + SIGNATURE_COEFFS_FILENAME + " could not"
//                    + " be generated");
//        }
//        SignatureFitting signatureFitting = parseFittingResults(getOutDir(), getFitId(), getFitMethod(), getSigVersion(), getnBoot(),
//                getOrgan(), getThresholdPerc(), getThresholdPval(), getMaxRareSigs());
//        JacksonUtils.getDefaultObjectMapper().writerFor(SignatureFitting.class).writeValue(getOutDir()
//                .resolve(MutationalSignatureAnalysis.MUTATIONAL_SIGNATURE_FITTING_DATA_MODEL_FILENAME).toFile(), signatureFitting);
    }
}
