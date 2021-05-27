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
    NAMES,
    CHROMOSOME,
    START,
    END,
    REFERENCE,
    ALTERNATE,
    LENGTH,
    TYPE,
    STRAND,
    SV,

    STUDIES("studies", "sourceEntries"),
    STUDIES_SAMPLES(STUDIES, "studies.samples", "studies.samplesData", "samples", "samplesData"),
    STUDIES_FILES(STUDIES, "studies.files", "files"),
    STUDIES_STATS(STUDIES, "studies.stats", "studies.cohortStats", "stats", "sourceEntries.stats"),
    STUDIES_SAMPLE_DATA_KEYS(STUDIES, "studies.sampleDataKeys"),
    STUDIES_SCORES(STUDIES, "studies.scores"),
    STUDIES_ISSUES(STUDIES, "studies.issues"),
    STUDIES_SECONDARY_ALTERNATES(STUDIES, "studies.secondaryAlternates"),
    STUDIES_STUDY_ID(STUDIES, "studies.studyId"),

    ANNOTATION("annotation"),
    ANNOTATION_CHROMOSOME(ANNOTATION, "annotation.chromosome"),
    ANNOTATION_START(ANNOTATION, "annotation.start"),
    ANNOTATION_END(ANNOTATION, "annotation.end"),
    ANNOTATION_REFERENCE(ANNOTATION, "annotation.reference"),
    ANNOTATION_ALTERNATE(ANNOTATION, "annotation.alternate"),
    ANNOTATION_ANCESTRAL_ALLELE(ANNOTATION, "annotation.ancestralAllele"),
    ANNOTATION_ID(ANNOTATION, "annotation.id"),
    ANNOTATION_XREFS(ANNOTATION, "annotation.xrefs"),
    ANNOTATION_HGVS(ANNOTATION, "annotation.hgvs"),
    ANNOTATION_CYTOBAND(ANNOTATION, "annotation.cytoband"),
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
    ANNOTATION_TRAIT_ASSOCIATION(ANNOTATION, "annotation.traitAssociation"),
    ANNOTATION_FUNCTIONAL_SCORE(ANNOTATION, "annotation.functionalScore"),
    ANNOTATION_REPEAT(ANNOTATION, "annotation.repeat"),
    ANNOTATION_DRUGS(ANNOTATION, "annotation.drugs"),
    ANNOTATION_ADDITIONAL_ATTRIBUTES(ANNOTATION, "annotation.additionalAttributes");

    private static final Set<VariantField> ALL_FIELDS = Collections.unmodifiableSet(new HashSet<>(Arrays.asList(values())));

    /**
     * Known additional attributes defined by OpenCGA.
     *
     * <code>
     *   "annotation" {
     *       "additionalAttributes" {
     *           "opencga" : { <<<< GROUP_NAME
     *               "attribute" : {
     *                   "release" : "4"
     *               }
     *           }
     *       }
     *   }
     * </code>
     */
    public enum AdditionalAttributes {
        GROUP_NAME("opencga"),
        RELEASE("release"),
        INDEX_SYNCHRONIZATION("indexSync"),
        INDEX_STUDIES("indexStudies"),
        ANNOTATION_ID("annotationId"),
        VARIANT_ID("id");

        private final String key;

        AdditionalAttributes(String key) {
            this.key = key;
        }

        public String key() {
            return key;
        }
    }

    private static final List<VariantField> SUMMARY_EXCLUDED_FIELDS = Arrays.asList(
            STUDIES_FILES,
            STUDIES_SAMPLES,
            STUDIES_ISSUES,
            STUDIES_SCORES,
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

    /**
     * Given a QueryOptions, reads the {@link QueryOptions#INCLUDE}, {@link QueryOptions#EXCLUDE} and  {@link VariantField#SUMMARY}
     * to determine which fields from Variant will be included.
     * In case of more than one fields present, will use the first valid field in this order:
     *   1. {@link QueryOptions#INCLUDE}
     *   2. {@link QueryOptions#EXCLUDE}
     *   3. {@link VariantField#SUMMARY}
     *
     * @param options   Non null options
     * @return          List of fields to be included.
     */
    public static Set<VariantField> getIncludeFields(QueryOptions options) {
        Set<VariantField> includeFields;

        if (options == null) {
            options = QueryOptions.empty();
        }

        List<String> includeList = options.getAsStringList(QueryOptions.INCLUDE);
        if (includeList != null && !includeList.isEmpty()) {
            includeFields = parseInclude(includeList);
        } else {
            List<String> excludeList = options.getAsStringList(QueryOptions.EXCLUDE);
            if (excludeList != null && !excludeList.isEmpty()) {
                includeFields = parseExclude(excludeList);
            } else {
                includeFields = new HashSet<>(Arrays.asList(values()));
                if (options.getBoolean(SUMMARY, false)) {
                    includeFields.removeAll(SUMMARY_EXCLUDED_FIELDS);
                }
            }
        }

        return includeFields;
    }

    public static Set<VariantField> parseInclude(String... includeList) {
        return parseInclude(Arrays.asList(includeList));
    }

    public static Set<VariantField> parseInclude(List<String> includeList) {
        Set<VariantField> includeFields = new HashSet<>();
        if (includeList == null) {
            return includeFields;
        }
        for (String include : includeList) {
            VariantField field = get(include);
            if (field == null) {
                throw VariantQueryException.unknownVariantField(QueryOptions.INCLUDE, include);
//                    continue;
            }
            if (field.getParent() != null) {
                includeFields.add(field.getParent());
            }
            includeFields.add(field);
            includeFields.addAll(field.getChildren());
        }
        return includeFields;
    }

    public static Set<VariantField> parseExclude(String... includeList) {
        return parseExclude(Arrays.asList(includeList));
    }

    public static Set<VariantField> parseExclude(List<String> excludeList) {
        Set<VariantField> includeFields = new HashSet<>(Arrays.asList(values()));
        if (excludeList == null) {
            return includeFields;
        }
        for (String exclude : excludeList) {
            VariantField field = get(exclude);
            if (field == null) {
                throw VariantQueryException.unknownVariantField(QueryOptions.EXCLUDE, exclude);
//                        continue;
            }
            includeFields.remove(field);
            includeFields.removeAll(field.getChildren());
        }
        return includeFields;
    }

    public static Set<VariantField> all() {
        return ALL_FIELDS;
    }

    /**
     * Remove intermediate nodes some child is missing, or all children from a node if all are present.
     *
     * @param includeFields Set of non pruned fields
     * @return  Pruned set of fields
     */
    public static Set<VariantField> prune(Set<VariantField> includeFields) {
        if (includeFields.containsAll(VariantField.STUDIES.getChildren())) {
            includeFields.removeAll(VariantField.STUDIES.getChildren());
        } else {
            includeFields.remove(VariantField.STUDIES);
        }
        if (includeFields.containsAll(VariantField.ANNOTATION.getChildren())) {
            includeFields.removeAll(VariantField.ANNOTATION.getChildren());
        } else {
            includeFields.remove(VariantField.ANNOTATION);
        }
        return includeFields;
    }

    private static Map<String, VariantField> getNamesMap() {
        if (NAMES_MAP.get() == null) {
            Map<String, VariantField> map = new HashMap<>();
            for (VariantField variantField : VariantField.values()) {
                map.put(variantField.name(), variantField);
                for (String name : variantField.names) {
                    map.put(name, variantField);
                }
            }
            NAMES_MAP.compareAndSet(null, Collections.unmodifiableMap(map));
        }
        return NAMES_MAP.get();
    }

}
