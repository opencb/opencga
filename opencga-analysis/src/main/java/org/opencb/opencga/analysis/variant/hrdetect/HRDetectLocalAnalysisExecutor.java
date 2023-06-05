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
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.opencb.biodata.models.variant.StudyEntry;
import org.opencb.biodata.models.variant.Variant;
import org.opencb.biodata.models.variant.avro.VariantType;
import org.opencb.commons.datastore.core.Query;
import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.commons.datastore.core.QueryResultWriter;
import org.opencb.commons.exec.Command;
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
import org.opencb.opencga.storage.core.variant.io.VariantWriterFactory;
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

    private final static String CNV_FILENAME = "cnv.tsv";
    private final static String INDEL_FILENAME = "indel.vcf";
    private final static String INDEL_SORTED_FILENAME = "indel.sorted.vcf";
    private final static String INDEL_GZ_FILENAME = "indel.sorted.vcf.gz";
    private final static String INPUT_TABLE_FILENAME = "inputTable.tsv";

    private final static String VIRTUAL_VOLUMEN_DATA = "/data/";
    private final static String VIRTUAL_VOLUMEN_SNV = "/snv/";
    private final static String VIRTUAL_VOLUMEN_SV = "/sv/";

    private Path opencgaHome;

    private Logger logger = LoggerFactory.getLogger(this.getClass());

    @Override
    public void run() throws ToolException, CatalogException, IOException, StorageEngineException {
        opencgaHome = Paths.get(getExecutorParams().getString("opencgaHome"));

        // Prepare CNV data
        prepareCNVData();

        // Prepare INDEL data
        prepareINDELData();

        // Prepare input table
        prepareInputTable();

        // Run R script for fitting signature
        executeRScript();
    }

    private void prepareCNVData() throws ToolExecutorException, StorageEngineException, CatalogException, FileNotFoundException {
        Query query = new Query(getCnvQuery());
        query.put(VariantQueryParam.STUDY.key(), getStudy());
        query.put(VariantQueryParam.SAMPLE.key(), getSomaticSample() + "," + getGermlineSample());

        QueryOptions queryOptions = new QueryOptions();
        queryOptions.append(QueryOptions.INCLUDE, "id,studies");

        logger.info("CNV query: {}", query);
        logger.info("CNV query options: {}", queryOptions);

        PrintWriter pwOut = new PrintWriter(getOutDir().resolve("cnvs.discarded").toFile());

        PrintWriter pw = new PrintWriter(getOutDir().resolve(CNV_FILENAME).toAbsolutePath().toString());
        pw.println("seg_no\tChromosome\tchromStart\tchromEnd\ttotal.copy.number.inNormal\tminor.copy.number.inNormal\t"
                + "total.copy.number.inTumour\tminor.copy.number.inTumour");

        VariantDBIterator iterator = getVariantStorageManager().iterator(query, queryOptions, getToken());
        int count = 0;
        while (iterator.hasNext()) {
            Variant variant = iterator.next();

            if (CollectionUtils.isEmpty(variant.getStudies())) {
                pwOut.println(variant.toStringSimple() + "\tStudies is empty");
            } else {
                StudyEntry studyEntry = variant.getStudies().get(0);
                try {
                    StringBuilder sb = new StringBuilder(++count)
                            .append("\t").append(variant.getChromosome())
                            .append("\t").append(variant.getStart())
                            .append("\t").append(variant.getEnd())
                            .append("\t").append(Integer.parseInt(studyEntry.getSampleData(getGermlineSample(), "TCN")))
                            .append("\t").append(Integer.parseInt(studyEntry.getSampleData(getGermlineSample(), "MCN")))
                            .append("\t").append(Integer.parseInt(studyEntry.getSampleData(getSomaticSample(), "TCN")))
                            .append("\t").append(Integer.parseInt(studyEntry.getSampleData(getSomaticSample(), "MCN")));

                    pw.println(sb);
                } catch (NumberFormatException e) {
                    pwOut.println(variant.toStringSimple() + "\tError parsing TCN/MCN values: " + e.getMessage());
                }
            }
        }

        pw.close();
        pwOut.close();
    }

    private void prepareINDELData() throws ToolExecutorException, StorageEngineException, CatalogException, IOException {
        Query query = new Query(getIndelQuery());
        query.put(VariantQueryParam.STUDY.key(), getStudy());

        QueryOptions queryOptions = new QueryOptions();
        queryOptions.append(QueryOptions.INCLUDE, "id,studies")
                .append(QueryOptions.SORT, true);

        logger.info("INDEL query: {}", query);
        logger.info("INDEL query options: {}", queryOptions);

        getVariantStorageManager().exportData(getOutDir().resolve(INDEL_FILENAME).toAbsolutePath().toString(),
                VariantWriterFactory.VariantOutputFormat.VCF, null, query, queryOptions, getToken());

        if (!getOutDir().resolve(INDEL_FILENAME).toFile().exists()) {
            new ToolExecutorException("Error exporting VCF file with INDEL variants");
        }

        // Workaround to sort, waiting for exporting to do it
        File sortVcfFile = getOutDir().resolve("sort_vcf.sh").toFile();
        PrintWriter pw = new PrintWriter(sortVcfFile);
        pw.println("#!/bin/sh");
        pw.println("cat $1 | awk '$1 ~ /^#/ {print $0;next} {print $0 | \"sort -k1,1 -k2,2n\"}' > $2");
        pw.close();
        new Command("bash " + sortVcfFile.getAbsolutePath()
                + " " + getOutDir().resolve(INDEL_FILENAME).toAbsolutePath()
                + " " + getOutDir().resolve(INDEL_SORTED_FILENAME).toFile())
                .run();
        sortVcfFile.delete();

        // BGZIP
        AbstractMap.SimpleEntry<String, String> outputBinding = new AbstractMap.SimpleEntry<>(getOutDir()
                .toAbsolutePath().toString(), VIRTUAL_VOLUMEN_DATA);
        String cmdline = DockerUtils.run(R_DOCKER_IMAGE, null, outputBinding, "bgzip " + VIRTUAL_VOLUMEN_DATA + INDEL_SORTED_FILENAME,
                null);
        logger.info("Docker command line: " + cmdline);

        // TABIX
        cmdline = DockerUtils.run(R_DOCKER_IMAGE, null, outputBinding, "tabix -p vcf " + VIRTUAL_VOLUMEN_DATA + INDEL_GZ_FILENAME, null);
        logger.info("Docker command line: " + cmdline);
    }

    private void prepareInputTable() throws FileNotFoundException {
        PrintWriter pw = new PrintWriter(getOutDir().resolve(INPUT_TABLE_FILENAME).toAbsolutePath().toString());
        pw.println("sample\tIndels_vcf_files\tCNV_tab_files");
        pw.println(getSomaticSample() + "\t" + VIRTUAL_VOLUMEN_DATA + INDEL_GZ_FILENAME + "\t" + VIRTUAL_VOLUMEN_DATA + CNV_FILENAME);
        pw.close();
    }

    private void executeRScript() throws IOException {
        // Input
        List<AbstractMap.SimpleEntry<String, String>> inputBindings = new ArrayList<>();
        inputBindings.add(new AbstractMap.SimpleEntry<>(getSnvRDataPath().toFile().getParent(), VIRTUAL_VOLUMEN_SNV));
        inputBindings.add(new AbstractMap.SimpleEntry<>(getSvRDataPath().toFile().getParent(), VIRTUAL_VOLUMEN_SV));

        // Output
        AbstractMap.SimpleEntry<String, String> outputBinding = new AbstractMap.SimpleEntry<>(getOutDir()
                .toAbsolutePath().toString(), VIRTUAL_VOLUMEN_DATA);

        // Command
        StringBuilder scriptParams = new StringBuilder("R CMD Rscript --vanilla ")
                .append("/opt/opencga/signature.tools.lib/scripts/hrDetect")
                .append(" -x ").append(VIRTUAL_VOLUMEN_SNV).append(getSnvRDataPath().toFile().getName())
                .append(" -X ").append(VIRTUAL_VOLUMEN_SV).append(getSvRDataPath().toFile().getName())
                .append(" -i ").append(VIRTUAL_VOLUMEN_DATA).append(INPUT_TABLE_FILENAME)
                .append(" -o ").append(VIRTUAL_VOLUMEN_DATA);

        if (StringUtils.isNotEmpty(getSnv3CustomName())) {
            scriptParams.append(" -y ").append(getSnv3CustomName());
        }
        if (StringUtils.isNotEmpty(getSnv8CustomName())) {
            scriptParams.append(" -z ").append(getSnv8CustomName());
        }
        if (StringUtils.isNotEmpty(getSv3CustomName())) {
            scriptParams.append(" -Y ").append(getSv3CustomName());
        }
        if (StringUtils.isNotEmpty(getSv8CustomName())) {
            scriptParams.append(" -Z ").append(getSv3CustomName());
        }
        if (getBootstrap() != null) {
            scriptParams.append(" -b");
        }

        switch (getAssembly()) {
            case "GRCh37": {
                scriptParams.append(" --genomev=hg19");
                break;
            }
            case "GRCh38": {
                scriptParams.append(" --genomev=hg38");
                break;
            }
        }

        String cmdline = DockerUtils.run(R_DOCKER_IMAGE, inputBindings, outputBinding, scriptParams.toString(), null);
        logger.info("Docker command line: " + cmdline);
    }
}
