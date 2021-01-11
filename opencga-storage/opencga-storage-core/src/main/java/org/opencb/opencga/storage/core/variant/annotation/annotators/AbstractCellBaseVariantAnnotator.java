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
import org.opencb.biodata.models.variant.Variant;
import org.opencb.biodata.models.variant.VariantBuilder;
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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;

import static org.opencb.opencga.storage.core.variant.adaptors.VariantField.AdditionalAttributes.GROUP_NAME;
import static org.opencb.opencga.storage.core.variant.adaptors.VariantField.AdditionalAttributes.VARIANT_ID;

/**
 * Created by jacobo on 9/01/15.
 */
public abstract class AbstractCellBaseVariantAnnotator extends VariantAnnotator {


    public static final int SLOW_CELLBASE_SECONDS = 60;
    protected static Logger logger = LoggerFactory.getLogger(AbstractCellBaseVariantAnnotator.class);
    protected final String species;
    protected final String assembly;
    protected final String cellbaseVersion;
    protected final QueryOptions queryOptions;
    protected final boolean supportImpreciseVariants;
    protected final boolean supportStarAlternate;
    protected final int variantLengthThreshold;
    protected final Function<Variant, String> variantSerializer;

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
        supportImpreciseVariants = params.getBoolean(VariantStorageOptions.ANNOTATOR_CELLBASE_IMPRECISE_VARIANTS.key(),
                VariantStorageOptions.ANNOTATOR_CELLBASE_IMPRECISE_VARIANTS.defaultValue());
        supportStarAlternate = params.getBoolean(VariantStorageOptions.ANNOTATOR_CELLBASE_STAR_ALTERNATE.key(),
                VariantStorageOptions.ANNOTATOR_CELLBASE_STAR_ALTERNATE.defaultValue());

        if (supportImpreciseVariants) {
            // If the cellbase sever supports imprecise variants, use the original toString, which adds the CIPOS and CIEND
            variantSerializer = Variant::toString;
        } else {
            variantSerializer = variant -> variant.getChromosome()
                    + ':' + variant.getStart()
                    + ':' + (variant.getReference().isEmpty() ? "-" : variant.getReference())
                    + ':' + (variant.getAlternate().isEmpty() ? "-" : variant.getAlternate());
        }
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
        List<Variant> filteredVariants = filterVariants(variants);
        StopWatch stopWatch = StopWatch.createStarted();
        List<QueryResult<VariantAnnotation>> queryResults = annotateFiltered(filteredVariants);
        stopWatch.stop();
        if (stopWatch.getTime(TimeUnit.SECONDS) > SLOW_CELLBASE_SECONDS) {
            logger.warn("Slow annotation from CellBase."
                    + " Annotating " + variants.size() + " variants took " + TimeUtils.durationToString(stopWatch));
        }
        return getVariantAnnotationList(filteredVariants, queryResults);
    }

    protected abstract List<QueryResult<VariantAnnotation>> annotateFiltered(List<Variant> variants) throws VariantAnnotatorException;

    private List<Variant> filterVariants(List<Variant> variants) {
        List<Variant> nonStructuralVariants = new ArrayList<>(variants.size());
        for (Variant variant : variants) {
            // If Variant is SV some work is needed
            // TODO:Manage larger SV variants
            int length = Math.max(variant.getLength(), variant.getAlternate().length() + variant.getReference().length());
            boolean skipLength = length > variantLengthThreshold;
            boolean skipStarAlternate = !supportStarAlternate && variant.getAlternate().equals("*");
            if (skipLength) {
//                logger.info("Skip variant! {}", genomicVariant);
                logger.info("Skip variant! {}", variant.getChromosome() + ":" + variant.getStart() + "-" + variant.getEnd() + ":"
                        + (variant.getReference().length() > 10
                        ? variant.getReference().substring(0, 10) + "...[" + variant.getReference().length() + "]"
                        : variant.getReference()) + ":"
                        + (variant.getAlternate().length() > 10
                        ? variant.getAlternate().substring(0, 10) + "...[" + variant.getAlternate().length() + "]"
                        : variant.getAlternate())
                );
                logger.debug("Skip variant! {}", variant);
            } else if (skipStarAlternate) {
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
                    if (variantSerializer.apply(variant).equals(queryResult.getId())
                            || variant.toString().equals(queryResult.getId())
                            || variant.toStringSimple().equals(queryResult.getId())) {
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
                    String annotationAlternate = variantAnnotation.getAlternate();
                    if (annotationAlternate.equals(VariantBuilder.DUP_ALT)
                            && variant.getAlternate().equals(VariantBuilder.DUP_TANDEM_ALT)) {
                        // Annotator might remove the ":TANDEM". Put it back
                        annotationAlternate = VariantBuilder.DUP_TANDEM_ALT;
                    }
                    if (!variant.getChromosome().equals(variantAnnotation.getChromosome())
                            || !variant.getStart().equals(variantAnnotation.getStart())
                            || !variant.getReference().equals(variantAnnotation.getReference())
                            || !variant.getAlternate().equals(annotationAlternate)) {
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
