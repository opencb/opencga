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

package org.opencb.opencga.analysis.wrappers.roh;

import org.apache.commons.collections4.MapUtils;
import org.apache.commons.lang3.StringUtils;
import org.opencb.commons.datastore.core.Query;
import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.commons.utils.DockerUtils;
import org.opencb.opencga.analysis.AnalysisUtils;
import org.opencb.opencga.analysis.tools.OpenCgaToolScopeStudy;
import org.opencb.opencga.analysis.wrappers.samtools.SamtoolsWrapperAnalysisExecutor;
import org.opencb.opencga.catalog.db.api.FileDBAdaptor;
import org.opencb.opencga.catalog.exceptions.CatalogException;
import org.opencb.opencga.catalog.managers.FileManager;
import org.opencb.opencga.core.exceptions.ToolException;
import org.opencb.opencga.core.models.alignment.RohWrapperParams;
import org.opencb.opencga.core.models.common.Enums;
import org.opencb.opencga.core.models.file.File;
import org.opencb.opencga.core.response.OpenCGAResult;
import org.opencb.opencga.core.tools.annotations.Tool;
import org.opencb.opencga.core.tools.annotations.ToolParams;
import org.opencb.opencga.storage.core.exceptions.StorageEngineException;
import org.opencb.opencga.storage.core.variant.adaptors.VariantQuery;
import org.opencb.opencga.storage.core.variant.io.VariantWriterFactory;

import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.*;

import static org.opencb.opencga.core.api.ParamConstants.SAMTOOLS_COMMANDS_SUPPORTED;
import static org.opencb.opencga.core.api.ParamConstants.SAMTOOLS_COMMAND_DESCRIPTION;

@Tool(id = RohWrapperAnalysis.ID, resource = Enums.Resource.SAMPLE, description = RohWrapperAnalysis.DESCRIPTION)
public class RohWrapperAnalysis extends OpenCgaToolScopeStudy {

    public static final String ID = "roh";
    public static final String DESCRIPTION = "Analysis to calculate and visualise regions of homozygosity (ROHs) for a given sample";

    @ToolParams
    protected final RohWrapperParams rohParams = new RohWrapperParams();

    @Override
    protected void check() throws Exception {
        super.check();

        // Check sample
        if (StringUtils.isEmpty(rohParams.getSampleId())) {
            throw new ToolException("Missing sample ID");
        }

        // Check chromosome
        if (StringUtils.isEmpty(rohParams.getChromosome())) {
            throw new ToolException("Missing chromosome");
        }

    }

    @Override
    protected void run() throws Exception {
        setUpStorageEngineExecutor(study);

        step(getId(), () -> {
            // Get VCF file
            Path vcfPath = null;
            boolean requireExport = true;

            // Get file
            if (StringUtils.isNotEmpty(rohParams.getFileId())) {
                OpenCGAResult<File> fileResult;
                Query query = new Query(FileDBAdaptor.QueryParams.FORMAT.key(), File.Format.VCF)
                        .append(FileDBAdaptor.QueryParams.SAMPLE_IDS.key(), rohParams.getSampleId());
                try {
                    fileResult = getCatalogManager().getFileManager().search(study, query, QueryOptions.empty(), token);
                } catch (CatalogException e) {
                    throw new ToolException(e);
                }
                boolean found = false;
                for (File file : fileResult.getResults()) {
                    if (file.getId().equals(rohParams.getFileId())) {
                        vcfPath = Paths.get(fileResult.first().getUri().getPath());
                        if (Files.exists(vcfPath) && vcfPath.toFile().canRead()) {
                            found = true;
                            break;
                        }
                    }
                }
                if (found) {
                    requireExport = false;
                } else {
                    logger.info("VCF file ID {} not found, so variant export will be performed for ROH analysis", rohParams.getFileId());
                }
            }

            if (requireExport) {
                // Export variants to VCF file
                vcfPath = getOutDir().resolve(rohParams.getSampleId() + "." + getJobId() + ".vcf.gz");

                VariantQuery variantQuery = new VariantQuery()
                        .study(study)
                        .region(rohParams.getChromosome())
                        .sample(rohParams.getSampleId())
                        .includeSampleData("GT")
                        .unknownGenotype("./.");

                QueryOptions queryOptions = QueryOptions.empty();

                logger.info("Export variants for sample {} to the file {}", rohParams.getSampleId(), vcfPath);
                logger.info("Export query: {}", variantQuery.toJson());
                logger.info("Export query options: {}", queryOptions.toJson());

                try {
                    getVariantStorageManager().exportData(vcfPath.toString(), VariantWriterFactory.VariantOutputFormat.VCF_GZ, null,
                            variantQuery, queryOptions, token);
                } catch (StorageEngineException | CatalogException e) {
                    throw new ToolException(e);
                }
            }

            // Get he ROH analysis executor and execute !!!
            boolean skipGenotypeQuality;
            if (requireExport || rohParams.getSkipGenotypeQuality() == null) {
                skipGenotypeQuality = true;
            } else {
                skipGenotypeQuality = rohParams.getSkipGenotypeQuality();
            }
            getToolExecutor(RohWrapperAnalysisExecutor.class)
                    .setSampleId(rohParams.getSampleId())
                    .setChromosome(rohParams.getChromosome())
                    .setVcfPath(vcfPath)
                    .setFilter(rohParams.getFilter())
                    .setGenotypeQuality(rohParams.getGenotypeQuality())
                    .setSkipGenotypeQuality(skipGenotypeQuality)
                    .setHomozygWindowSnp(rohParams.getHomozygWindowSnp())
                    .setHomozygWindowHet(rohParams.getHomozygWindowHet())
                    .setHomozygWindowMissing(rohParams.getHomozygWindowMissing())
                    .setHomozygWindowThreshold(rohParams.getHomozygWindowThreshold())
                    .setHomozygKb(rohParams.getHomozygKb())
                    .setHomozygSnp(rohParams.getHomozygSnp())
                    .setHomozygHet(rohParams.getHomozygHet())
                    .setHomozygDensity(rohParams.getHomozygDensity())
                    .setHomozygGap(rohParams.getHomozygGap())
                    .execute();
        });
    }
}