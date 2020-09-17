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

package org.opencb.opencga.storage.core.variant.annotation.annotators;

import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.time.StopWatch;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opencb.biodata.models.variant.Variant;
import org.opencb.biodata.models.variant.avro.AdditionalAttribute;
import org.opencb.biodata.models.variant.avro.VariantAnnotation;
import org.opencb.commons.datastore.core.ObjectMap;
import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.commons.datastore.core.QueryResult;
import org.opencb.opencga.core.common.TimeUtils;
import org.opencb.opencga.storage.core.config.StorageConfiguration;
import org.opencb.opencga.storage.core.metadata.models.ProjectMetadata;
import org.opencb.opencga.storage.core.variant.VariantStorageOptions;
import org.opencb.opencga.storage.core.variant.annotation.VariantAnnotatorException;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.TimeUnit;

import static org.opencb.opencga.storage.core.variant.adaptors.VariantField.AdditionalAttributes.GROUP_NAME;
import static org.opencb.opencga.storage.core.variant.adaptors.VariantField.AdditionalAttributes.VARIANT_ID;

/**
 * Created by jacobo on 9/01/15.
 */
public abstract class AbstractCellBaseVariantAnnotator extends VariantAnnotator {


    public static final int SLOW_CELLBASE_SECONDS = 60;
    protected static Logger logger = LogManager.getLogger(AbstractCellBaseVariantAnnotator.class);
    protected final String species;
    protected final String assembly;
    protected final String cellbaseVersion;
    protected final QueryOptions queryOptions;
    protected final boolean impreciseVariants;
    protected final int variantLengthThreshold;

    public AbstractCellBaseVariantAnnotator(StorageConfiguration storageConfiguration, ProjectMetadata projectMetadata, ObjectMap params)
            throws VariantAnnotatorException {
        super(storageConfiguration, projectMetadata, params);
//        species = toCellBaseSpeciesName(params.getString(VariantAnnotationManager.SPECIES));
//        assembly = params.getString(VariantAnnotationManager.ASSEMBLY);
        species = projectMetadata.getSpecies();
        assembly = projectMetadata.getAssembly();
        cellbaseVersion = storageConfiguration.getCellbase().getVersion();

        queryOptions = new QueryOptions();
        if (StringUtils.isNotEmpty(params.getString(VariantStorageOptions.ANNOTATOR_CELLBASE_INCLUDE.key()))) {
            queryOptions.put(QueryOptions.INCLUDE, params.getString(VariantStorageOptions.ANNOTATOR_CELLBASE_INCLUDE.key()));
        } else if (StringUtils.isNotEmpty(params.getString(VariantStorageOptions.ANNOTATOR_CELLBASE_EXCLUDE.key()))) {
            queryOptions.put(QueryOptions.EXCLUDE, params.getString(VariantStorageOptions.ANNOTATOR_CELLBASE_EXCLUDE.key()));
        }
        if (!params.getBoolean(VariantStorageOptions.ANNOTATOR_CELLBASE_USE_CACHE.key())) {
            queryOptions.append("useCache", false);
        }
        variantLengthThreshold = params.getInt(
                VariantStorageOptions.ANNOTATOR_CELLBASE_VARIANT_LENGTH_THRESHOLD.key(),
                VariantStorageOptions.ANNOTATOR_CELLBASE_VARIANT_LENGTH_THRESHOLD.defaultValue());
        impreciseVariants = params.getBoolean(VariantStorageOptions.ANNOTATOR_CELLBASE_IMPRECISE_VARIANTS.key(), true);

        checkNotNull(cellbaseVersion, "cellbase version");
        checkNotNull(species, "species");
        checkNotNull(assembly, "assembly");

    }

    protected static void checkNotNull(String value, String name) throws VariantAnnotatorException {
        if (value == null || value.isEmpty()) {
            throw new VariantAnnotatorException("Missing defaultValue: " + name);
        }
    }

    public static String toCellBaseSpeciesName(String scientificName) {
        if (scientificName != null && scientificName.contains(" ")) {
            String[] split = scientificName.split(" ", 2);
            scientificName = (split[0].charAt(0) + split[1]).toLowerCase();
        }
        return scientificName;
    }


    @Override
    public final List<VariantAnnotation> annotate(List<Variant> variants) throws VariantAnnotatorException {
        List<Variant> nonStructuralVariations = filterStructuralVariants(variants);
        StopWatch stopWatch = StopWatch.createStarted();
        List<QueryResult<VariantAnnotation>> queryResults = annotateFiltered(nonStructuralVariations);
        stopWatch.stop();
        if (stopWatch.getTime(TimeUnit.SECONDS) > SLOW_CELLBASE_SECONDS) {
            logger.warn("Slow annotation from CellBase."
                    + " Annotating " + variants.size() + " variants took " + TimeUtils.durationToString(stopWatch));
        }
        return getVariantAnnotationList(nonStructuralVariations, queryResults);
    }

    protected abstract List<QueryResult<VariantAnnotation>> annotateFiltered(List<Variant> variants) throws VariantAnnotatorException;

    private List<Variant> filterStructuralVariants(List<Variant> variants) {
        List<Variant> nonStructuralVariants = new ArrayList<>(variants.size());
        for (Variant variant : variants) {
            // If Variant is SV some work is needed
            // TODO:Manage larger SV variants
            if (variant.getAlternate().length() + variant.getReference().length() > variantLengthThreshold) {
//                logger.info("Skip variant! {}", genomicVariant);
                logger.info("Skip variant! {}", variant.getChromosome() + ":" + variant.getStart() + ":"
                        + (variant.getReference().length() > 10
                        ? variant.getReference().substring(0, 10) + "...[" + variant.getReference().length() + "]"
                        : variant.getReference()) + ":"
                        + (variant.getAlternate().length() > 10
                        ? variant.getAlternate().substring(0, 10) + "...[" + variant.getAlternate().length() + "]"
                        : variant.getAlternate())
                );
                logger.debug("Skip variant! {}", variant);
            } else {
                nonStructuralVariants.add(variant);
            }
        }
        return nonStructuralVariants;
    }

    protected List<VariantAnnotation> getVariantAnnotationList(List<Variant> variants, List<QueryResult<VariantAnnotation>> queryResults) {
        List<VariantAnnotation> variantAnnotationList = new ArrayList<>(variants.size());
        Iterator<Variant> iterator = variants.iterator();
        if (queryResults != null) {
            for (QueryResult<VariantAnnotation> queryResult : queryResults) {
                // If the QueryResult is empty, assume that the variant was skipped
                // Check that the skipped variant matches with the expected variant
                if (queryResult.getResult().isEmpty()) {
                    Variant variant = iterator.next();
                    if (variant.toString().equals(queryResult.getId()) || variant.toStringSimple().equals(queryResult.getId())) {
                        logger.warn("Skip annotation for variant " + variant);
                    } else {
                        Variant variantId = new Variant(queryResult.getId());
                        if (!variant.getChromosome().equals(variantId.getChromosome())
                                || !variant.getStart().equals(variantId.getStart())
                                || !variant.getReference().equals(variantId.getReference())
                                || !variant.getAlternate().equals(variantId.getAlternate())) {
                            throw unexpectedVariantOrderException(variant, variantId);
                        } else {
                            logger.warn("Skip annotation for variant " + variant);
                        }
                    }
                }
                for (VariantAnnotation variantAnnotation : queryResult.getResult()) {
                    Variant variant = iterator.next();
                    if (!variant.getChromosome().equals(variantAnnotation.getChromosome())
                            || !variant.getStart().equals(variantAnnotation.getStart())
                            || !variant.getReference().equals(variantAnnotation.getReference())
                            || !variant.getAlternate().equals(variantAnnotation.getAlternate())) {
                        throw unexpectedVariantOrderException(variant, variantAnnotation.getChromosome() + ':'
                                + variantAnnotation.getStart() + ':'
                                + variantAnnotation.getReference() + ':'
                                + variantAnnotation.getAlternate());
                    }
                    if (variant.isSV() || variant.getSv() != null) {
                        // Variant annotation class does not have information about Structural Variations.
                        // Store the original Variant.toString as an additional attribute.
                        AdditionalAttribute additionalAttribute =
                                new AdditionalAttribute(Collections.singletonMap(VARIANT_ID.key(), variant.toString()));
                        if (variantAnnotation.getAdditionalAttributes() == null) {
                            variantAnnotation
                                    .setAdditionalAttributes(Collections.singletonMap(GROUP_NAME.key(), additionalAttribute));
                        } else {
                            variantAnnotation.getAdditionalAttributes().put(GROUP_NAME.key(), additionalAttribute);
                        }
                    }
                    variantAnnotationList.add(variantAnnotation);
                }
            }
        }
        return variantAnnotationList;
    }

    static RuntimeException unexpectedVariantOrderException(Object expected, Object actual) {
        return new IllegalArgumentException("Variants not in the expected order! "
                + "Expected '" + expected + "', " + "but got '" + actual + "'.");
    }

}
