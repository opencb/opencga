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
import org.opencb.biodata.models.variant.Variant;
import org.opencb.biodata.models.variant.avro.VariantAnnotation;
import org.opencb.commons.datastore.core.ObjectMap;
import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.commons.datastore.core.QueryResult;
import org.opencb.opencga.storage.core.config.StorageConfiguration;
import org.opencb.opencga.storage.core.variant.annotation.VariantAnnotationManager;
import org.opencb.opencga.storage.core.variant.annotation.VariantAnnotatorException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by jacobo on 9/01/15.
 */
public abstract class AbstractCellBaseVariantAnnotator extends VariantAnnotator {

    public static final String ANNOTATOR_CELLBASE_USE_CACHE = "annotator.cellbase.use_cache";
    public static final String ANNOTATOR_CELLBASE_INCLUDE = "annotator.cellbase.include";
    public static final String ANNOTATOR_CELLBASE_EXCLUDE = "annotator.cellbase.exclude";
    public static final int CELLBASE_VARIANT_THRESHOLD = 5000;

    protected static Logger logger = LoggerFactory.getLogger(AbstractCellBaseVariantAnnotator.class);
    protected final String species;
    protected final String assembly;
    protected final String cellbaseVersion;
    protected final QueryOptions queryOptions;

    public AbstractCellBaseVariantAnnotator(StorageConfiguration storageConfiguration, ObjectMap params) throws VariantAnnotatorException {
        super(storageConfiguration, params);

        species = toCellBaseSpeciesName(params.getString(VariantAnnotationManager.SPECIES));
        assembly = params.getString(VariantAnnotationManager.ASSEMBLY);
        cellbaseVersion = storageConfiguration.getCellbase().getVersion();

        queryOptions = new QueryOptions();
        if (StringUtils.isNotEmpty(params.getString(ANNOTATOR_CELLBASE_INCLUDE))) {
            queryOptions.put(QueryOptions.INCLUDE, params.getString(ANNOTATOR_CELLBASE_INCLUDE));
        } else if (StringUtils.isNotEmpty(params.getString(ANNOTATOR_CELLBASE_EXCLUDE))) {
            queryOptions.put(QueryOptions.EXCLUDE, params.getString(ANNOTATOR_CELLBASE_EXCLUDE));
        }
        if (!params.getBoolean(ANNOTATOR_CELLBASE_USE_CACHE)) {
            queryOptions.append("useCache", false);
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
        List<Variant> nonStructuralVariations = filterStructuralVariants(variants);
        return annotateFiltered(nonStructuralVariations);
    }

    protected abstract List<VariantAnnotation> annotateFiltered(List<Variant> variants) throws VariantAnnotatorException;

    private List<Variant> filterStructuralVariants(List<Variant> variants) {
        List<Variant> nonStructuralVariants = new ArrayList<>(variants.size());
        for (Variant variant : variants) {
            // If Variant is SV some work is needed
            // TODO:Manage larger SV variants
            if (variant.getAlternate().length() + variant.getReference().length() > CELLBASE_VARIANT_THRESHOLD) {
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
        if (queryResults != null) {
            for (QueryResult<VariantAnnotation> queryResult : queryResults) {
                variantAnnotationList.addAll(queryResult.getResult());
            }
        }
        return variantAnnotationList;
    }

}
