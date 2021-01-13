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

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.opencb.biodata.models.clinical.ClinicalProperty;
import org.opencb.commons.datastore.core.ObjectMap;
import org.opencb.commons.datastore.core.Query;
import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.opencga.analysis.clinical.rga.RgaAnalysis;
import org.opencb.opencga.analysis.clinical.team.TeamInterpretationAnalysis;
import org.opencb.opencga.analysis.clinical.team.TeamInterpretationConfiguration;
import org.opencb.opencga.analysis.clinical.tiering.CancerTieringInterpretationAnalysis;
import org.opencb.opencga.analysis.clinical.tiering.CancerTieringInterpretationConfiguration;
import org.opencb.opencga.analysis.clinical.tiering.TieringInterpretationAnalysis;
import org.opencb.opencga.analysis.clinical.tiering.TieringInterpretationConfiguration;
import org.opencb.opencga.analysis.clinical.zetta.ZettaInterpretationAnalysis;
import org.opencb.opencga.analysis.clinical.zetta.ZettaInterpretationConfiguration;
import org.opencb.opencga.app.cli.internal.options.ClinicalCommandOptions;
import org.opencb.opencga.core.api.ParamConstants;
import org.opencb.opencga.core.exceptions.ToolException;

import java.io.FileInputStream;
import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;

import static org.opencb.opencga.app.cli.internal.options.ClinicalCommandOptions.RgaSecondaryIndexCommandOptions.RGA_INDEX_RUN_COMMAND;
import static org.opencb.opencga.app.cli.main.options.ClinicalCommandOptions.InterpretationCancerTieringCommandOptions.CANCER_TIERING_RUN_COMMAND;
import static org.opencb.opencga.app.cli.main.options.ClinicalCommandOptions.InterpretationTeamCommandOptions.TEAM_RUN_COMMAND;
import static org.opencb.opencga.app.cli.main.options.ClinicalCommandOptions.InterpretationTieringCommandOptions.TIERING_RUN_COMMAND;
import static org.opencb.opencga.app.cli.main.options.ClinicalCommandOptions.InterpretationZettaCommandOptions.ZETTA_RUN_COMMAND;

/**
 * Created on 01/04/20
 *
 * @author Joaquin Tarraga Gimenez &lt;joaquintarraga@gmail.com&gt;
 */
public class ClinicalCommandExecutor extends InternalCommandExecutor {

    private final ClinicalCommandOptions clinicalCommandOptions;

    public ClinicalCommandExecutor(ClinicalCommandOptions options) {
        super(options.commonCommandOptions);
        clinicalCommandOptions = options;
    }

    @Override
    public void execute() throws Exception {
        logger.debug("Executing clinical command line");

        String subCommandString = getParsedSubCommand(clinicalCommandOptions.jCommander);
        configure();
        switch (subCommandString) {

            case TIERING_RUN_COMMAND:
                tiering();
                break;

            case TEAM_RUN_COMMAND:
                team();
                break;

            case ZETTA_RUN_COMMAND:
                zetta();
                break;

            case CANCER_TIERING_RUN_COMMAND:
                cancerTiering();
                break;

            case RGA_INDEX_RUN_COMMAND:
                rgaIndex();
                break;

            default:
                logger.error("Subcommand not valid");
                break;

        }
    }

    private void rgaIndex() throws ToolException {
        ClinicalCommandOptions.RgaSecondaryIndexCommandOptions options = clinicalCommandOptions.rgaSecondaryIndexCommandOptions;
        Path outDir = Paths.get(options.outdir);
        ObjectMap params = new ObjectMap()
                .append(RgaAnalysis.STUDY_PARAM, options.study)
                .append(RgaAnalysis.FILE_PARAM, options.file);
        toolRunner.execute(RgaAnalysis.class, params, outDir, options.jobOptions.jobId, options.commonOptions.token);
    }


    private void tiering() throws Exception {
        ClinicalCommandOptions.TieringCommandOptions cliOptions = clinicalCommandOptions.tieringCommandOptions;
        ObjectMap params = new ObjectMap();
        params.putAll(cliOptions.commonOptions.params);

        // Prepare analysis parameters and config
        String token = cliOptions.commonOptions.token;

        String studyId = cliOptions.study;
        String clinicalAnalysisId = cliOptions.clinicalAnalysis;

        if (CollectionUtils.isEmpty(cliOptions.panels)) {
            throw new ToolException("Missing disease panel IDs");
        }
//        List<String> panelList = Arrays.asList(StringUtils.split(options.panelIds, ","));

        ClinicalProperty.Penetrance penetrance = cliOptions.penetrance;

        TieringInterpretationConfiguration config = new TieringInterpretationConfiguration();
//        config.setSkipUntieredVariants(!cliOptions.includeUntieredVariants);

        Path outDir = Paths.get(cliOptions.outdir);
        Path opencgaHome = Paths.get(configuration.getWorkspace()).getParent();

        // Execute tiering analysis
        TieringInterpretationAnalysis tieringAnalysis = new TieringInterpretationAnalysis();
        tieringAnalysis.setUp(opencgaHome.toString(), new ObjectMap(), outDir, token);
        tieringAnalysis.setStudyId(studyId)
                .setClinicalAnalysisId(clinicalAnalysisId)
                .setDiseasePanelIds(cliOptions.panels)
                .setPenetrance(penetrance)
                .setConfig(config);
        tieringAnalysis.setPrimary(cliOptions.primary);
        tieringAnalysis.start();
    }

    private void team() throws Exception {
        ClinicalCommandOptions.TeamCommandOptions cliOptions = clinicalCommandOptions.teamCommandOptions;
        ObjectMap params = new ObjectMap();
        params.putAll(cliOptions.commonOptions.params);

        // Prepare analysis parameters and config
        String token = cliOptions.commonOptions.token;

        if (CollectionUtils.isEmpty(cliOptions.panels)) {
            throw new ToolException("Missing disease panel IDs");
        }

        ClinicalProperty.ModeOfInheritance moi = null;
        if (StringUtils.isNotEmpty(cliOptions.familySeggregation)) {
            moi = ClinicalProperty.ModeOfInheritance.valueOf(cliOptions.familySeggregation);
        }
//        List<String> panelList = Arrays.asList(StringUtils.split(options.panelIds, ","));

        TeamInterpretationConfiguration config = new TeamInterpretationConfiguration();
//        config.setSkipUntieredVariants(!cliOptions.includeUntieredVariants);

        Path outDir = Paths.get(cliOptions.outdir);
        Path opencgaHome = Paths.get(configuration.getWorkspace()).getParent();

        // Execute tiering analysis
        TeamInterpretationAnalysis teamAnalysis = new TeamInterpretationAnalysis();
        teamAnalysis.setUp(opencgaHome.toString(), new ObjectMap(), outDir, token);
        teamAnalysis.setStudyId(cliOptions.study)
                .setClinicalAnalysisId(cliOptions.clinicalAnalysis)
                .setDiseasePanelIds(cliOptions.panels)
                .setMoi(moi)
                .setConfig(config);
        teamAnalysis.setPrimary(cliOptions.primary);
        teamAnalysis.start();
    }

    private void zetta() throws ToolException, IOException {
        ClinicalCommandOptions.ZettaCommandOptions cliOptions = clinicalCommandOptions.zettaCommandOptions;

        Query query = new Query();

        // Variant filters
        query.putIfNotNull("id", cliOptions.basicQueryOptions.id);
        query.putIfNotNull("region", cliOptions.basicQueryOptions.region);
        query.putIfNotNull("type", cliOptions.basicQueryOptions.type);

        // Study filters
        query.putIfNotNull(ParamConstants.STUDY_PARAM, cliOptions.study);
        query.putIfNotNull("file", cliOptions.file);
        query.putIfNotNull("filter", cliOptions.filter);
        query.putIfNotNull("qual", cliOptions.qual);
        query.putIfNotNull("fileData", cliOptions.fileData);

        query.putIfNotNull("sample", cliOptions.samples);
        query.putIfNotNull("sampleData", cliOptions.sampleData);
        query.putIfNotNull("sampleAnnotation", cliOptions.sampleAnnotation);

        query.putIfNotNull("cohort", cliOptions.cohort);
        query.putIfNotNull("cohortStatsRef", cliOptions.basicQueryOptions.rf);
        query.putIfNotNull("cohortStatsAlt", cliOptions.basicQueryOptions.af);
        query.putIfNotNull("cohortStatsMaf", cliOptions.basicQueryOptions.maf);
        query.putIfNotNull("cohortStatsMgf", cliOptions.mgf);
        query.putIfNotNull("cohortStatsPass", cliOptions.cohortStatsPass);
        query.putIfNotNull("score", cliOptions.score);

        // Annotation filters
        query.putIfNotNull("gene", cliOptions.basicQueryOptions.gene);
        query.putIfNotNull("ct", cliOptions.basicQueryOptions.consequenceType);
        query.putIfNotNull("xref", cliOptions.xref);
        query.putIfNotNull("biotype", cliOptions.geneBiotype);
        query.putIfNotNull("proteinSubstitution", cliOptions.basicQueryOptions.proteinSubstitution);
        query.putIfNotNull("conservation", cliOptions.basicQueryOptions.conservation);
        query.putIfNotNull("populationFrequencyAlt", cliOptions.basicQueryOptions.populationFreqAlt);
        query.putIfNotNull("populationFrequencyRef", cliOptions.populationFreqRef);
        query.putIfNotNull("populationFrequencyMaf", cliOptions.populationFreqMaf);
        query.putIfNotNull("transcriptFlag", cliOptions.flags);
        query.putIfNotNull("geneTraitId", cliOptions.geneTraitId);
        query.putIfNotNull("go", cliOptions.go);
        query.putIfNotNull("expression", cliOptions.expression);
        query.putIfNotNull("proteinKeyword", cliOptions.proteinKeywords);
        query.putIfNotNull("drug", cliOptions.drugs);
        query.putIfNotNull("functionalScore", cliOptions.basicQueryOptions.functionalScore);
        query.putIfNotNull("clinicalSignificance", cliOptions.clinicalSignificance);
        query.putIfNotNull("customAnnotation", cliOptions.annotations);

        query.putIfNotNull("panel", cliOptions.panel);

        query.putIfNotNull("trait", cliOptions.trait);

        Path outDir = Paths.get(cliOptions.outdir);
        Path opencgaHome = Paths.get(configuration.getWorkspace()).getParent();

        ZettaInterpretationConfiguration config = new ZettaInterpretationConfiguration();
        ObjectMapper objectMapper = new ObjectMapper(new YAMLFactory());
        Path configPath = opencgaHome.resolve("analysis/" + ZettaInterpretationAnalysis.ID + "/config.yml");
        if (configPath.toFile().exists()) {
            FileInputStream fis = new FileInputStream(configPath.toFile());
            config = objectMapper.readValue(fis, ZettaInterpretationConfiguration.class);
        }

        // Execute tiering analysis
        ZettaInterpretationAnalysis zettaAnalysis = new ZettaInterpretationAnalysis();
        zettaAnalysis.setUp(opencgaHome.toString(), new ObjectMap(), outDir, token);
        zettaAnalysis.setStudyId(cliOptions.study)
                .setClinicalAnalysisId(cliOptions.clinicalAnalysis)
                .setQuery(query)
                .setQueryOptions(QueryOptions.empty())
                .setConfig(config);
        zettaAnalysis.setPrimary(cliOptions.primary);
        zettaAnalysis.start();
    }

    private void cancerTiering() throws Exception {
        ClinicalCommandOptions.CancerTieringCommandOptions cliOptions = clinicalCommandOptions.cancerTieringCommandOptions;
        ObjectMap params = new ObjectMap();
        params.putAll(cliOptions.commonOptions.params);

        // Prepare analysis parameters and config
        String token = cliOptions.commonOptions.token;

        CancerTieringInterpretationConfiguration config = new CancerTieringInterpretationConfiguration();
//        config.setSkipUntieredVariants(!cliOptions.includeUntieredVariants);

        Path outDir = Paths.get(cliOptions.outdir);
        Path opencgaHome = Paths.get(configuration.getWorkspace()).getParent();

        // Execute cancer tiering analysis
        CancerTieringInterpretationAnalysis cancerTieringAnalysis = new CancerTieringInterpretationAnalysis();
        cancerTieringAnalysis.setUp(opencgaHome.toString(), new ObjectMap(), outDir, token);
        cancerTieringAnalysis.setStudyId(cliOptions.study)
                .setClinicalAnalysisId(cliOptions.clinicalAnalysis)
                .setVariantIdsToDiscard(cliOptions.discardedVariants)
                .setConfig(config);
        cancerTieringAnalysis.setPrimary(cliOptions.primary);
        cancerTieringAnalysis.start();
    }
}
