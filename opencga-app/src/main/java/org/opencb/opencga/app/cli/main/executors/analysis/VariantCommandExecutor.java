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

        QueryOptions queryOptions = new QueryOptions();
        queryOptions.putAll(variantCommandOptions.commonCommandOptions.params);

        queryOptions.putIfNotEmpty(VariantDBAdaptor.VariantQueryParams.ID.key(), queryCommandOptions.ids);
        queryOptions.putIfNotEmpty(VariantDBAdaptor.VariantQueryParams.REGION.key(), queryCommandOptions.region);
        queryOptions.putIfNotEmpty(VariantDBAdaptor.VariantQueryParams.CHROMOSOME.key(),
                queryCommandOptions.chromosome);
        queryOptions.putIfNotEmpty(VariantDBAdaptor.VariantQueryParams.GENE.key(), queryCommandOptions.gene);
        queryOptions.putIfNotNull(VariantDBAdaptor.VariantQueryParams.TYPE.key(), queryCommandOptions.type);
        queryOptions.putIfNotEmpty(VariantDBAdaptor.VariantQueryParams.REFERENCE.key(), queryCommandOptions.reference);
        queryOptions.putIfNotEmpty(VariantDBAdaptor.VariantQueryParams.ALTERNATE.key(), queryCommandOptions.alternate);
        queryOptions.putIfNotEmpty(VariantDBAdaptor.VariantQueryParams.RETURNED_STUDIES.key(), queryCommandOptions.returnedStudies);
        queryOptions.putIfNotEmpty(VariantDBAdaptor.VariantQueryParams.RETURNED_SAMPLES.key(), queryCommandOptions.returnedSamples);
        queryOptions.putIfNotEmpty(VariantDBAdaptor.VariantQueryParams.RETURNED_FILES.key(), queryCommandOptions.returnedFiles);
        queryOptions.putIfNotEmpty(VariantDBAdaptor.VariantQueryParams.STUDIES.key(), queryCommandOptions.studies);
        queryOptions.putIfNotEmpty(VariantDBAdaptor.VariantQueryParams.FILES.key(), queryCommandOptions.files);
        queryOptions.putIfNotEmpty(VariantDBAdaptor.VariantQueryParams.STATS_MAF.key(), queryCommandOptions.maf);
        queryOptions.putIfNotEmpty(VariantDBAdaptor.VariantQueryParams.STATS_MGF.key(), queryCommandOptions.mgf);
        queryOptions.putIfNotEmpty(VariantDBAdaptor.VariantQueryParams.MISSING_ALLELES.key(), queryCommandOptions.missingAlleles);
        queryOptions.putIfNotEmpty(VariantDBAdaptor.VariantQueryParams.MISSING_GENOTYPES.key(),
                queryCommandOptions.missingGenotypes);
//        queryOptions.put(VariantDBAdaptor.VariantQueryParams.ANNOTATION_EXISTS.key(),
//                queryCommandOptions.annotationExists);
        queryOptions.putIfNotEmpty(VariantDBAdaptor.VariantQueryParams.GENOTYPE.key(), queryCommandOptions.genotype);
        queryOptions.putIfNotEmpty(VariantDBAdaptor.VariantQueryParams.ANNOT_CONSEQUENCE_TYPE.key(), queryCommandOptions.annot_ct);
        queryOptions.putIfNotEmpty(VariantDBAdaptor.VariantQueryParams.ANNOT_XREF.key(), queryCommandOptions.annot_xref);
        queryOptions.putIfNotEmpty(VariantDBAdaptor.VariantQueryParams.ANNOT_BIOTYPE.key(), queryCommandOptions.annot_biotype);
        queryOptions.putIfNotEmpty(VariantDBAdaptor.VariantQueryParams.ANNOT_POLYPHEN.key(), queryCommandOptions.polyphen);
        queryOptions.putIfNotEmpty(VariantDBAdaptor.VariantQueryParams.ANNOT_SIFT.key(), queryCommandOptions.sift);
        queryOptions.putIfNotEmpty(VariantDBAdaptor.VariantQueryParams.ANNOT_CONSERVATION.key(), queryCommandOptions.conservation);
        queryOptions.putIfNotEmpty(VariantDBAdaptor.VariantQueryParams.ANNOT_POPULATION_MINOR_ALLELE_FREQUENCY.key(),
                queryCommandOptions.annotPopulationMaf);
        queryOptions.putIfNotEmpty(VariantDBAdaptor.VariantQueryParams.ANNOT_POPULATION_ALTERNATE_FREQUENCY.key(),
                queryCommandOptions.alternate_frequency);
        queryOptions.putIfNotEmpty(VariantDBAdaptor.VariantQueryParams.ANNOT_POPULATION_REFERENCE_FREQUENCY.key(),
                queryCommandOptions.reference_frequency);
        queryOptions.putIfNotEmpty(VariantDBAdaptor.VariantQueryParams.ANNOT_TRANSCRIPTION_FLAGS.key(),
                queryCommandOptions.transcriptionFlags);
        queryOptions.putIfNotEmpty(VariantDBAdaptor.VariantQueryParams.ANNOT_GENE_TRAITS_ID.key(), queryCommandOptions.geneTraitId);
        queryOptions.putIfNotEmpty(VariantDBAdaptor.VariantQueryParams.ANNOT_GENE_TRAITS_NAME.key(),
                queryCommandOptions.geneTraitName);
        queryOptions.putIfNotEmpty(VariantDBAdaptor.VariantQueryParams.ANNOT_HPO.key(), queryCommandOptions.hpo);
        queryOptions.putIfNotEmpty(VariantDBAdaptor.VariantQueryParams.ANNOT_GO.key(), queryCommandOptions.go);
        queryOptions.putIfNotEmpty(VariantDBAdaptor.VariantQueryParams.ANNOT_EXPRESSION.key(), queryCommandOptions.expression);
        queryOptions.putIfNotEmpty(VariantDBAdaptor.VariantQueryParams.ANNOT_PROTEIN_KEYWORDS.key(),
                queryCommandOptions.proteinKeyword);
        queryOptions.putIfNotEmpty(VariantDBAdaptor.VariantQueryParams.ANNOT_DRUG.key(), queryCommandOptions.drug);
        queryOptions.putIfNotEmpty(VariantDBAdaptor.VariantQueryParams.ANNOT_FUNCTIONAL_SCORE.key(),
                queryCommandOptions.functionalScore);
        queryOptions.putIfNotEmpty(VariantDBAdaptor.VariantQueryParams.UNKNOWN_GENOTYPE.key(), queryCommandOptions.unknownGenotype);
        queryOptions.put(QueryOptions.SORT, queryCommandOptions.sort);
//        queryOptions.putIfNotEmpty("merge", queryCommandOptions.merge);

        queryOptions.putIfNotEmpty(QueryOptions.INCLUDE, queryCommandOptions.include);
        queryOptions.putIfNotEmpty(QueryOptions.EXCLUDE, queryCommandOptions.exclude);
        queryOptions.putIfNotEmpty(QueryOptions.LIMIT, queryCommandOptions.limit);
        queryOptions.putIfNotEmpty(QueryOptions.SKIP, queryCommandOptions.skip);

        queryOptions.put("samplesMetadata", queryCommandOptions.samplesMetadata);
        queryOptions.putIfNotEmpty("groupBy", queryCommandOptions.groupBy);
        queryOptions.put("histogram", queryCommandOptions.histogram);
        queryOptions.putIfNotEmpty("interval", queryCommandOptions.interval);
        queryOptions.put("count", queryCommandOptions.count);

        if (queryCommandOptions.count) {
            return openCGAClient.getVariantClient().count(queryOptions);
        } else if (queryCommandOptions.samplesMetadata || StringUtils.isNoneEmpty(queryCommandOptions.groupBy) || queryCommandOptions.histogram) {
            return openCGAClient.getVariantClient().genericQuery(queryOptions);
        } else {
            return openCGAClient.getVariantClient().query(queryOptions);
        }
    }

}
