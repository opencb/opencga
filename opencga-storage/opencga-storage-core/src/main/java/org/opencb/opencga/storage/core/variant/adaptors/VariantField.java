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

package org.opencb.opencga.storage.core.variant.adaptors;

import org.opencb.commons.datastore.core.QueryOptions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Created on 01/02/17.
 *
 * @author Jacobo Coll &lt;jacobo167@gmail.com&gt;
 */
public enum VariantField {
    ID("id", "ids", "name"),
    CHROMOSOME,
    START,
    END,
    REFERENCE,
    ALTERNATE,
    LENGTH,
    TYPE,
    HGVS,

    STUDIES("studies", "sourceEntries"),
    STUDIES_SAMPLES_DATA(STUDIES, "studies.samplesData", "samples", "samplesData"),
    STUDIES_FILES(STUDIES, "studies.files", "files"),
    STUDIES_STATS(STUDIES, "studies.stats", "studies.cohortStats", "stats", "sourceEntries.stats"),
    STUDIES_SECONDARY_ALTERNATES(STUDIES, "studies.secondaryAlternates"),
    STUDIES_STUDY_ID(STUDIES, "studies.studyId"),

    ANNOTATION("annotation"),
    ANNOTATION_ANCESTRAL_ALLELE(ANNOTATION, "annotation.ancestralAllele"),
    ANNOTATION_ID(ANNOTATION, "annotation.id"),
    ANNOTATION_XREFS(ANNOTATION, "annotation.xrefs"),
    ANNOTATION_HGVS(ANNOTATION, "annotation.hgvs"),
    ANNOTATION_DISPLAY_CONSEQUENCE_TYPE(ANNOTATION, "annotation.displayConsequenceType"),
    ANNOTATION_CONSEQUENCE_TYPES(ANNOTATION, "annotation.consequenceTypes"),
    ANNOTATION_POPULATION_FREQUENCIES(ANNOTATION, "annotation.populationFrequencies"),
    ANNOTATION_MINOR_ALLELE(ANNOTATION, "annotation.minorAllele"),
    ANNOTATION_MINOR_ALLELE_FREQ(ANNOTATION, "annotation.minorAlleleFreq"),
    ANNOTATION_CONSERVATION(ANNOTATION, "annotation.conservation"),
    ANNOTATION_GENE_EXPRESSION(ANNOTATION, "annotation.geneExpression"),
    ANNOTATION_GENE_TRAIT_ASSOCIATION(ANNOTATION, "annotation.geneTraitAssociation"),
    ANNOTATION_GENE_DRUG_INTERACTION(ANNOTATION, "annotation.geneDrugInteraction"),
    ANNOTATION_VARIANT_TRAIT_ASSOCIATION(ANNOTATION, "annotation.variantTraitAssociation"),
    ANNOTATION_FUNCTIONAL_SCORE(ANNOTATION, "annotation.functionalScore"),
    ANNOTATION_ADDITIONAL_ATTRIBUTES(ANNOTATION, "annotation.additionalAttributes");

    private static final List<VariantField> SUMMARY_EXCLUDED_FIELDS = Arrays.asList(
            STUDIES_FILES,
            STUDIES_SAMPLES_DATA,
            STUDIES_SECONDARY_ALTERNATES);

    public static final String SUMMARY = "summary";

    private final List<String> names;
    private final VariantField parent;
    private final AtomicReference<List<VariantField>> children = new AtomicReference<>();
    private static final AtomicReference<Map<String, VariantField>> NAMES_MAP = new AtomicReference<>();

    VariantField(String... names) {
        this(null, names);
    }
    VariantField(VariantField parent, String... names) {
        this.parent = parent;
        if (names.length == 0) {
            this.names = Collections.singletonList(name().toLowerCase());
        } else {
            this.names = Collections.unmodifiableList(Arrays.asList(names));
        }
    }

    private static Logger logger = LoggerFactory.getLogger(VariantField.class);

    public String fieldName() {
        return names.get(0);
    }

    @Override
    public String toString() {
        return fieldName();
    }

    public VariantField getParent() {
        return parent;
    }

    public List<VariantField> getChildren() {
        if (children.get() == null) {
            ArrayList<VariantField> childrenList = new ArrayList<>();
            for (VariantField variantField : VariantField.values()) {
                if (variantField.getParent() == this) {
                    childrenList.add(variantField);
                }
            }
            childrenList.trimToSize();
            children.compareAndSet(null, Collections.unmodifiableList(childrenList));
        }
        return children.get();
    }

    public static VariantField get(String field) {
        return getNamesMap().get(field);
    }

    public static Set<VariantField> getReturnedFields(QueryOptions options) {
        return getReturnedFields(options, false);
    }

    /**
     * Given a QueryOptions, reads the {@link QueryOptions#INCLUDE}, {@link QueryOptions#EXCLUDE} and  {@link VariantField#SUMMARY}
     * to determine which fields from Variant will be returned.
     * In case of more than one fields present, will use the first valid field in this order:
     *   1. {@link QueryOptions#INCLUDE}
     *   2. {@link QueryOptions#EXCLUDE}
     *   3. {@link VariantField#SUMMARY}
     *
     * @param options   Non null options
     * @param prune     Remove intermediate nodes some child is missing, or all children from a node if all are present
     * @return          List of fields to be returned.
     */
    public static Set<VariantField> getReturnedFields(QueryOptions options, boolean prune) {
        Set<VariantField> returnedFields;

        if (options == null) {
            options = QueryOptions.empty();
        }

        List<String> includeList = options.getAsStringList(QueryOptions.INCLUDE);
        if (includeList != null && !includeList.isEmpty()) {
            returnedFields = new HashSet<>();
            for (String include : includeList) {
                VariantField field = get(include);
                if (field == null) {
                    throw VariantQueryException.unknownVariantField(QueryOptions.INCLUDE, include);
//                    continue;
                }
                if (field.getParent() != null) {
                    returnedFields.add(field.getParent());
                }
                returnedFields.add(field);
                returnedFields.addAll(field.getChildren());
            }

        } else {
            List<String> excludeList = options.getAsStringList(QueryOptions.EXCLUDE);
            returnedFields = new HashSet<>(Arrays.asList(values()));
            if (excludeList != null && !excludeList.isEmpty()) {
                for (String exclude : excludeList) {
                    VariantField field = get(exclude);
                    if (field == null) {
                        throw VariantQueryException.unknownVariantField(QueryOptions.EXCLUDE, exclude);
//                        continue;
                    }
                    returnedFields.remove(field);
                    returnedFields.removeAll(field.getChildren());
                }
            } else if (options.getBoolean(SUMMARY, false)) {
                returnedFields.removeAll(SUMMARY_EXCLUDED_FIELDS);
            }
        }

        if (prune) {
            return prune(returnedFields);
        } else {
            return returnedFields;
        }
    }

    /**
     * Remove intermediate nodes some child is missing, or all children from a node if all are present.
     *
     * @param returnedFields Set of non pruned fields
     * @return  Pruned set of fields
     */
    public static Set<VariantField> prune(Set<VariantField> returnedFields) {
        if (returnedFields.containsAll(VariantField.STUDIES.getChildren())) {
            returnedFields.removeAll(VariantField.STUDIES.getChildren());
        } else {
            returnedFields.remove(VariantField.STUDIES);
        }
        if (returnedFields.containsAll(VariantField.ANNOTATION.getChildren())) {
            returnedFields.removeAll(VariantField.ANNOTATION.getChildren());
        } else {
            returnedFields.remove(VariantField.ANNOTATION);
        }
        return returnedFields;
    }

    private static Map<String, VariantField> getNamesMap() {
        if (NAMES_MAP.get() == null) {
            Map<String, VariantField> map = new HashMap<>();
            for (VariantField variantField : VariantField.values()) {
                for (String name : variantField.names) {
                    map.put(name, variantField);
                }
            }
            NAMES_MAP.compareAndSet(null, Collections.unmodifiableMap(map));
        }
        return NAMES_MAP.get();
    }

}
