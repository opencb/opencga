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

package org.opencb.opencga.storage.core.variant.annotation.annotators;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.databind.MapperFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.opencb.biodata.models.variant.Variant;
import org.opencb.biodata.models.variant.avro.VariantAnnotation;
import org.opencb.commons.datastore.core.ObjectMap;
import org.opencb.commons.datastore.core.QueryResult;
import org.opencb.opencga.storage.core.config.StorageConfiguration;
import org.opencb.opencga.storage.core.variant.annotation.VariantAnnotationManager;
import org.opencb.opencga.storage.core.variant.annotation.VariantAnnotatorException;
import org.opencb.opencga.storage.core.variant.io.json.mixin.VariantAnnotationMixin;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by jacobo on 9/01/15.
 */
public abstract class AbstractCellBaseVariantAnnotator extends VariantAnnotator {


    private final JsonFactory factory;
    private ObjectMapper jsonObjectMapper;

    protected static Logger logger = LoggerFactory.getLogger(AbstractCellBaseVariantAnnotator.class);
    protected final String species;
    protected final String assembly;
    protected final String cellbaseVersion;

    public AbstractCellBaseVariantAnnotator(StorageConfiguration storageConfiguration, ObjectMap options) throws VariantAnnotatorException {
        super(storageConfiguration, options);

        this.factory = new JsonFactory();
        this.jsonObjectMapper = new ObjectMapper(factory);
        jsonObjectMapper.addMixIn(VariantAnnotation.class, VariantAnnotationMixin.class);
        jsonObjectMapper.configure(MapperFeature.REQUIRE_SETTERS_FOR_GETTERS, true);
        species = toCellBaseSpeciesName(options.getString(VariantAnnotationManager.SPECIES));
        assembly = options.getString(VariantAnnotationManager.ASSEMBLY);
        cellbaseVersion = storageConfiguration.getCellbase().getVersion();
        List<String> hosts = storageConfiguration.getCellbase().getHosts();
        if (hosts.isEmpty()) {
            throw new VariantAnnotatorException("Missing defaultValue \"CellBase Hosts\"");
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
            if (variant.getAlternate().length() + variant.getReference().length() > Variant.SV_THRESHOLD * 2) { // TODO: Manage SV variants
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
        for (QueryResult<VariantAnnotation> queryResult : queryResults) {
            variantAnnotationList.addAll(queryResult.getResult());
        }
        return variantAnnotationList;
    }

}
