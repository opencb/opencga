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

package org.opencb.opencga.analysis.variant.hrdetect;

import htsjdk.samtools.reference.BlockCompressedIndexedFastaSequenceFile;
import htsjdk.samtools.reference.FastaSequenceIndex;
import htsjdk.samtools.reference.ReferenceSequence;
import htsjdk.samtools.util.GZIIndex;
import org.apache.commons.lang3.StringUtils;
import org.opencb.biodata.models.variant.Variant;
import org.opencb.biodata.models.variant.avro.VariantType;
import org.opencb.commons.datastore.core.Query;
import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.commons.utils.DockerUtils;
import org.opencb.opencga.analysis.ResourceUtils;
import org.opencb.opencga.analysis.StorageToolExecutor;
import org.opencb.opencga.catalog.exceptions.CatalogException;
import org.opencb.opencga.core.common.GitRepositoryState;
import org.opencb.opencga.core.exceptions.ToolException;
import org.opencb.opencga.core.exceptions.ToolExecutorException;
import org.opencb.opencga.core.response.OpenCGAResult;
import org.opencb.opencga.core.response.VariantQueryResult;
import org.opencb.opencga.core.tools.annotations.ToolExecutor;
import org.opencb.opencga.core.tools.variant.HRDetectAnalysisExecutor;
import org.opencb.opencga.core.tools.variant.MutationalSignatureAnalysisExecutor;
import org.opencb.opencga.storage.core.exceptions.StorageEngineException;
import org.opencb.opencga.storage.core.variant.adaptors.VariantQueryParam;
import org.opencb.opencga.storage.core.variant.adaptors.iterators.VariantDBIterator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.*;

import static org.opencb.opencga.analysis.variant.mutationalSignature.MutationalSignatureAnalysis.CATALOGUES_FILENAME_DEFAULT;

@ToolExecutor(id="opencga-local", tool = HRDetectAnalysis.ID,
        framework = ToolExecutor.Framework.LOCAL, source = ToolExecutor.Source.STORAGE)
public class HRDetectLocalAnalysisExecutor extends HRDetectAnalysisExecutor
        implements StorageToolExecutor {

    public final static String R_DOCKER_IMAGE = "opencb/opencga-ext-tools:" + GitRepositoryState.get().getBuildVersion();

    private final static String CNV_FILENAME = "cnv.bed";
    private final static String INDEL_FILENAME = "indel.bed";

    private Path opencgaHome;

    private Logger logger = LoggerFactory.getLogger(this.getClass());

    @Override
    public void run() throws ToolException, CatalogException, IOException, StorageEngineException {
        opencgaHome = Paths.get(getExecutorParams().getString("opencgaHome"));

        // Prepare CNV data
        prepareCNVData();

        // Prepare INDEL data
        prepareINDELData();

        // Run R script for fitting signature
        executeRScript();
    }

    private void prepareCNVData() throws ToolExecutorException, StorageEngineException, CatalogException {
        QueryOptions queryOptions = new QueryOptions();
        queryOptions.append(QueryOptions.INCLUDE, "id");
        VariantDBIterator iterator = getVariantStorageManager().iterator(new Query(getCnvQuery()), queryOptions, getToken());
        while (iterator.hasNext()) {
            Variant variant = iterator.next();
        }
    }

    private  void prepareINDELData() throws ToolExecutorException, StorageEngineException, CatalogException {
        QueryOptions queryOptions = new QueryOptions();
        queryOptions.append(QueryOptions.INCLUDE, "id");
        VariantDBIterator iterator = getVariantStorageManager().iterator(new Query(getIndelQuery()), queryOptions, getToken());
        while (iterator.hasNext()) {
            Variant variant = iterator.next();
        }
    }

    private void executeRScript() throws IOException {
        // Input
        List<AbstractMap.SimpleEntry<String, String>> inputBindings = new ArrayList<>();
        inputBindings.add(new AbstractMap.SimpleEntry<>(getSnvRDataPath().toFile().getParent(), "/snv"));
        inputBindings.add(new AbstractMap.SimpleEntry<>(getSvRDataPath().toFile().getParent(), "/sv"));

        // Output
        AbstractMap.SimpleEntry<String, String> outputBinding = new AbstractMap.SimpleEntry<>(getOutDir()
                .toAbsolutePath().toString(), "/data");

        // Command
        StringBuilder scriptParams = new StringBuilder("R CMD Rscript --vanilla ")
                .append("/opt/opencga/signature.tools.lib/scripts/signatureFit")
                .append(" --snv=/snv/").append(getSnvRDataPath().toFile().getName())
                .append(" --sv=/sv/").append(getSvRDataPath().toFile().getName())
                .append(" --cnv=/data/").append(CNV_FILENAME)
                .append(" --indel=/data/").append(INDEL_FILENAME)
                .append(" --outdir=/data");

        String cmdline = DockerUtils.run(R_DOCKER_IMAGE, inputBindings, outputBinding, scriptParams.toString(), null);
        logger.info("Docker command line: " + cmdline);
    }
}
