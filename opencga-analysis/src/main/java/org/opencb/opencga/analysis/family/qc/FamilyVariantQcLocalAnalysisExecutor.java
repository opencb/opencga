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

import com.fasterxml.jackson.databind.ObjectWriter;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang.StringUtils;
import org.opencb.biodata.models.clinical.qc.RelatednessReport;
import org.opencb.commons.datastore.core.Query;
import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.commons.utils.DockerUtils;
import org.opencb.opencga.analysis.StorageToolExecutor;
import org.opencb.opencga.analysis.variant.mutationalSignature.MutationalSignatureAnalysis;
import org.opencb.opencga.analysis.variant.mutationalSignature.MutationalSignatureLocalAnalysisExecutor;
import org.opencb.opencga.catalog.db.api.IndividualDBAdaptor;
import org.opencb.opencga.catalog.exceptions.CatalogException;
import org.opencb.opencga.catalog.managers.CatalogManager;
import org.opencb.opencga.core.common.GitRepositoryState;
import org.opencb.opencga.core.common.JacksonUtils;
import org.opencb.opencga.core.exceptions.ToolException;
import org.opencb.opencga.core.exceptions.ToolExecutorException;
import org.opencb.opencga.core.models.individual.Individual;
import org.opencb.opencga.core.models.sample.Sample;
import org.opencb.opencga.core.models.variant.FamilyQcAnalysisParams;
import org.opencb.opencga.core.response.OpenCGAResult;
import org.opencb.opencga.core.tools.ToolParams;
import org.opencb.opencga.core.tools.annotations.ToolExecutor;
import org.opencb.opencga.core.tools.variant.FamilyQcAnalysisExecutor;
import org.opencb.opencga.core.tools.variant.FamilyVariantQcAnalysisExecutor;

import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

@ToolExecutor(id="opencga-local", tool = FamilyVariantQcAnalysis.ID, framework = ToolExecutor.Framework.LOCAL,
        source = ToolExecutor.Source.STORAGE)
public class FamilyVariantQcLocalAnalysisExecutor extends FamilyVariantQcAnalysisExecutor implements StorageToolExecutor {

    @Override
    public void run() throws ToolExecutorException {
        // Run the Python script responsible for performing the family QC analyses
        //    variant_qc.main.py --vcf-file xxx --info-json xxx --bam-file xxx --qc-type xxx --config xxx --output-dir xxx

        // Build command line to run Python script via docker image
        Path opencgaHome = getOpencgaHome();

        try {
            // Input binding
            List<AbstractMap.SimpleEntry<String, String>> inputBindings = new ArrayList<>();
            inputBindings.add(new AbstractMap.SimpleEntry<>(opencgaHome.resolve("analysis/variant-qc").toAbsolutePath().toString(),
                    "/script"));

            // Output binding
            AbstractMap.SimpleEntry<String, String> outputBinding = new AbstractMap.SimpleEntry<>(getOutDir().toAbsolutePath().toString(),
                    "/jobdir");

            String configFilename = "config.json";
            ObjectWriter objectWriter = JacksonUtils.getDefaultObjectMapper().writerFor(FamilyQcAnalysisParams.class);
            objectWriter.writeValue(getOutDir().resolve(configFilename).toFile(), getQcParams());

            String params = "python3 /script/variant_qc.main.py"
                    + " --vcf-file " + StringUtils.join(getVcfPaths().stream().map(p -> p.toAbsolutePath().toString().replace(
                    getOutDir().toAbsolutePath().toString(), "/jobdir")).collect(Collectors.toList()), ",")
                    + " --info-json " + StringUtils.join(getJsonPaths().stream().map(p -> p.toAbsolutePath().toString().replace(
                    getOutDir().toAbsolutePath().toString(), "/jobdir")).collect(Collectors.toList()), ",")
                    + " --qc-type family"
                    + " --config /jobdir/" + configFilename
                    + " --output-dir /jobdir";


            // Execute Pythong script in docker
            String dockerImage = "opencb/opencga-ext-tools:" + GitRepositoryState.getInstance().getBuildVersion();

            //DockerUtils.run(dockerImage, inputBindings, outputBinding, params, null);
        } catch (IOException e) {
            throw new ToolExecutorException(e);
        }
    }
}
