/*
 * Copyright 2015-2016 OpenCB
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

package org.opencb.opencga.app.cli.main.executors.analysis;

import org.apache.commons.lang3.StringUtils;
import org.opencb.commons.datastore.core.ObjectMap;
import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.commons.datastore.core.QueryResponse;
import org.opencb.opencga.app.cli.analysis.options.VariantCommandOptions;
import org.opencb.opencga.app.cli.main.OpencgaCommandExecutor;
import org.opencb.opencga.catalog.exceptions.CatalogException;
import org.opencb.opencga.storage.core.variant.VariantStorageEngine;
import org.opencb.opencga.storage.core.variant.adaptors.VariantDBAdaptor;

import java.io.IOException;

/**
 * Created by pfurio on 15/08/16.
 */
public class VariantCommandExecutor extends OpencgaCommandExecutor {

    private VariantCommandOptions variantCommandOptions;

    public VariantCommandExecutor(VariantCommandOptions variantCommandOptions) {
        super(variantCommandOptions.commonCommandOptions);
        this.variantCommandOptions = variantCommandOptions;
    }


    @Override
    public void execute() throws Exception {
        logger.debug("Executing variant command line");

        String subCommandString = getParsedSubCommand(variantCommandOptions.jCommander);
        QueryResponse queryResponse = null;
        switch (subCommandString) {
            case "index":
                queryResponse = index();
                break;
            case "query":
                queryResponse = query();
                break;
            default:
                logger.error("Subcommand not valid");
                break;
        }

        createOutput(queryResponse);

    }

    private QueryResponse index() throws CatalogException, IOException {
        logger.debug("Indexing variant(s)");

        String fileIds = variantCommandOptions.indexCommandOptions.fileIds;

        ObjectMap o = new ObjectMap();
        o.putIfNotNull(VariantStorageEngine.Options.STUDY_ID.key(), variantCommandOptions.indexCommandOptions.study);
        o.putIfNotNull("outDir", variantCommandOptions.indexCommandOptions.outdirId);
        o.putIfNotNull("transform", variantCommandOptions.indexCommandOptions.transform);
        o.putIfNotNull("load", variantCommandOptions.indexCommandOptions.load);
        o.putIfNotNull(VariantStorageEngine.Options.EXCLUDE_GENOTYPES.key(), variantCommandOptions.indexCommandOptions.excludeGenotype);
        o.putIfNotNull("includeExtraFields", variantCommandOptions.indexCommandOptions.extraFields);
        o.putIfNotNull("aggregated", variantCommandOptions.indexCommandOptions.aggregated);
        o.putIfNotNull(VariantStorageEngine.Options.CALCULATE_STATS.key(), variantCommandOptions.indexCommandOptions.calculateStats);
        o.putIfNotNull(VariantStorageEngine.Options.ANNOTATE.key(), variantCommandOptions.indexCommandOptions.annotate);
        o.putIfNotNull(VariantStorageEngine.Options.RESUME.key(), variantCommandOptions.indexCommandOptions.resume);
//        o.putIfNotNull("overwrite", variantCommandOptions.indexCommandOptions.overwriteAnnotations);
        o.putAll(variantCommandOptions.commonCommandOptions.params);

//        return openCGAClient.getFileClient().index(fileIds, o);
        return openCGAClient.getVariantClient().index(fileIds, o);
    }

    private QueryResponse query() throws CatalogException, IOException {
        logger.debug("Listing variants of a study.");

        VariantCommandOptions.QueryVariantCommandOptions queryCommandOptions = variantCommandOptions.queryCommandOptions;

        ObjectMap params = new ObjectMap();
        params.putAll(variantCommandOptions.commonCommandOptions.params);

        params.putIfNotEmpty(VariantDBAdaptor.VariantQueryParams.ID.key(), queryCommandOptions.ids);
        params.putIfNotEmpty(VariantDBAdaptor.VariantQueryParams.REGION.key(), queryCommandOptions.region);
        params.putIfNotEmpty(VariantDBAdaptor.VariantQueryParams.CHROMOSOME.key(),
                queryCommandOptions.chromosome);
        params.putIfNotEmpty(VariantDBAdaptor.VariantQueryParams.GENE.key(), queryCommandOptions.gene);
        params.putIfNotNull(VariantDBAdaptor.VariantQueryParams.TYPE.key(), queryCommandOptions.type);
        params.putIfNotEmpty(VariantDBAdaptor.VariantQueryParams.REFERENCE.key(), queryCommandOptions.reference);
        params.putIfNotEmpty(VariantDBAdaptor.VariantQueryParams.ALTERNATE.key(), queryCommandOptions.alternate);
        params.putIfNotEmpty(VariantDBAdaptor.VariantQueryParams.RETURNED_STUDIES.key(), queryCommandOptions.returnedStudies);
        params.putIfNotEmpty(VariantDBAdaptor.VariantQueryParams.RETURNED_SAMPLES.key(), queryCommandOptions.returnedSamples);
        params.putIfNotEmpty(VariantDBAdaptor.VariantQueryParams.RETURNED_FILES.key(), queryCommandOptions.returnedFiles);
        params.putIfNotEmpty(VariantDBAdaptor.VariantQueryParams.STUDIES.key(), queryCommandOptions.studies);
        params.putIfNotEmpty(VariantDBAdaptor.VariantQueryParams.FILES.key(), queryCommandOptions.files);
        params.putIfNotEmpty(VariantDBAdaptor.VariantQueryParams.STATS_MAF.key(), queryCommandOptions.maf);
        params.putIfNotEmpty(VariantDBAdaptor.VariantQueryParams.STATS_MGF.key(), queryCommandOptions.mgf);
        params.putIfNotEmpty(VariantDBAdaptor.VariantQueryParams.MISSING_ALLELES.key(), queryCommandOptions.missingAlleles);
        params.putIfNotEmpty(VariantDBAdaptor.VariantQueryParams.MISSING_GENOTYPES.key(),
                queryCommandOptions.missingGenotypes);
//        queryOptions.put(VariantDBAdaptor.VariantQueryParams.ANNOTATION_EXISTS.key(),
//                queryCommandOptions.annotationExists);
        params.putIfNotEmpty(VariantDBAdaptor.VariantQueryParams.GENOTYPE.key(), queryCommandOptions.genotype);
        params.putIfNotEmpty(VariantDBAdaptor.VariantQueryParams.ANNOT_CONSEQUENCE_TYPE.key(), queryCommandOptions.annot_ct);
        params.putIfNotEmpty(VariantDBAdaptor.VariantQueryParams.ANNOT_XREF.key(), queryCommandOptions.annot_xref);
        params.putIfNotEmpty(VariantDBAdaptor.VariantQueryParams.ANNOT_BIOTYPE.key(), queryCommandOptions.annot_biotype);
        params.putIfNotEmpty(VariantDBAdaptor.VariantQueryParams.ANNOT_POLYPHEN.key(), queryCommandOptions.polyphen);
        params.putIfNotEmpty(VariantDBAdaptor.VariantQueryParams.ANNOT_SIFT.key(), queryCommandOptions.sift);
        params.putIfNotEmpty(VariantDBAdaptor.VariantQueryParams.ANNOT_CONSERVATION.key(), queryCommandOptions.conservation);
        params.putIfNotEmpty(VariantDBAdaptor.VariantQueryParams.ANNOT_POPULATION_MINOR_ALLELE_FREQUENCY.key(),
                queryCommandOptions.annotPopulationMaf);
        params.putIfNotEmpty(VariantDBAdaptor.VariantQueryParams.ANNOT_POPULATION_ALTERNATE_FREQUENCY.key(),
                queryCommandOptions.alternate_frequency);
        params.putIfNotEmpty(VariantDBAdaptor.VariantQueryParams.ANNOT_POPULATION_REFERENCE_FREQUENCY.key(),
                queryCommandOptions.reference_frequency);
        params.putIfNotEmpty(VariantDBAdaptor.VariantQueryParams.ANNOT_TRANSCRIPTION_FLAGS.key(),
                queryCommandOptions.transcriptionFlags);
        params.putIfNotEmpty(VariantDBAdaptor.VariantQueryParams.ANNOT_GENE_TRAITS_ID.key(), queryCommandOptions.geneTraitId);
        params.putIfNotEmpty(VariantDBAdaptor.VariantQueryParams.ANNOT_GENE_TRAITS_NAME.key(),
                queryCommandOptions.geneTraitName);
        params.putIfNotEmpty(VariantDBAdaptor.VariantQueryParams.ANNOT_HPO.key(), queryCommandOptions.hpo);
        params.putIfNotEmpty(VariantDBAdaptor.VariantQueryParams.ANNOT_GO.key(), queryCommandOptions.go);
        params.putIfNotEmpty(VariantDBAdaptor.VariantQueryParams.ANNOT_EXPRESSION.key(), queryCommandOptions.expression);
        params.putIfNotEmpty(VariantDBAdaptor.VariantQueryParams.ANNOT_PROTEIN_KEYWORDS.key(),
                queryCommandOptions.proteinKeyword);
        params.putIfNotEmpty(VariantDBAdaptor.VariantQueryParams.ANNOT_DRUG.key(), queryCommandOptions.drug);
        params.putIfNotEmpty(VariantDBAdaptor.VariantQueryParams.ANNOT_FUNCTIONAL_SCORE.key(),
                queryCommandOptions.functionalScore);
        params.putIfNotEmpty(VariantDBAdaptor.VariantQueryParams.UNKNOWN_GENOTYPE.key(), queryCommandOptions.unknownGenotype);
        params.put(QueryOptions.SORT, queryCommandOptions.sort);
//        queryOptions.putIfNotEmpty("merge", queryCommandOptions.merge);

        QueryOptions options = new QueryOptions();
        options.putIfNotEmpty(QueryOptions.INCLUDE, queryCommandOptions.include);
        options.putIfNotEmpty(QueryOptions.EXCLUDE, queryCommandOptions.exclude);
        options.putIfNotEmpty(QueryOptions.LIMIT, queryCommandOptions.limit);
        options.putIfNotEmpty(QueryOptions.SKIP, queryCommandOptions.skip);
        options.put("count", queryCommandOptions.count);

        params.put("samplesMetadata", queryCommandOptions.samplesMetadata);
        params.putIfNotEmpty("groupBy", queryCommandOptions.groupBy);
        params.put("histogram", queryCommandOptions.histogram);
        params.putIfNotEmpty("interval", queryCommandOptions.interval);

        if (queryCommandOptions.count) {
            return openCGAClient.getVariantClient().count(params, options);
        } else if (queryCommandOptions.samplesMetadata || StringUtils.isNoneEmpty(queryCommandOptions.groupBy)
                || queryCommandOptions.histogram) {
            return openCGAClient.getVariantClient().genericQuery(params, options);
        } else {
            return openCGAClient.getVariantClient().query(params, options);
        }
    }

}
