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

package org.opencb.opencga.analysis.variant.manager;

import com.google.common.collect.Iterables;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.opencb.biodata.models.clinical.ClinicalProperty;
import org.opencb.biodata.models.clinical.Disorder;
import org.opencb.biodata.models.clinical.interpretation.DiseasePanel;
import org.opencb.biodata.models.clinical.interpretation.DiseasePanel.GenePanel;
import org.opencb.biodata.models.clinical.pedigree.Member;
import org.opencb.biodata.models.clinical.pedigree.Pedigree;
import org.opencb.biodata.models.clinical.pedigree.PedigreeManager;
import org.opencb.biodata.models.core.Region;
import org.opencb.biodata.models.core.SexOntologyTermAnnotation;
import org.opencb.biodata.models.variant.StudyEntry;
import org.opencb.biodata.models.variant.Variant;
import org.opencb.biodata.tools.pedigree.ModeOfInheritance;
import org.opencb.commons.datastore.core.DataResult;
import org.opencb.commons.datastore.core.Query;
import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.commons.datastore.core.QueryParam;
import org.opencb.opencga.catalog.db.api.*;
import org.opencb.opencga.catalog.exceptions.CatalogAuthorizationException;
import org.opencb.opencga.catalog.exceptions.CatalogException;
import org.opencb.opencga.catalog.managers.*;
import org.opencb.opencga.core.api.ParamConstants;
import org.opencb.opencga.core.models.PrivateStudyUid;
import org.opencb.opencga.core.models.cohort.Cohort;
import org.opencb.opencga.core.models.common.Enums;
import org.opencb.opencga.core.models.common.InternalStatus;
import org.opencb.opencga.core.models.family.Family;
import org.opencb.opencga.core.models.file.File;
import org.opencb.opencga.core.models.file.FileInternal;
import org.opencb.opencga.core.models.file.VariantIndexStatus;
import org.opencb.opencga.core.models.individual.Individual;
import org.opencb.opencga.core.models.panel.Panel;
import org.opencb.opencga.core.models.project.Project;
import org.opencb.opencga.core.models.sample.Sample;
import org.opencb.opencga.core.models.sample.SampleAclEntry;
import org.opencb.opencga.core.models.study.Study;
import org.opencb.opencga.core.models.user.UserFilter;
import org.opencb.opencga.core.response.OpenCGAResult;
import org.opencb.opencga.storage.core.exceptions.StorageEngineException;
import org.opencb.opencga.storage.core.metadata.VariantStorageMetadataManager;
import org.opencb.opencga.storage.core.utils.CellBaseUtils;
import org.opencb.opencga.storage.core.variant.adaptors.VariantField;
import org.opencb.opencga.storage.core.variant.adaptors.VariantQueryException;
import org.opencb.opencga.storage.core.variant.adaptors.VariantQueryParam;
import org.opencb.opencga.storage.core.variant.query.*;
import org.opencb.opencga.storage.core.variant.query.projection.VariantQueryProjectionParser;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.stream.Collectors;

import static org.opencb.biodata.models.clinical.ClinicalProperty.ModeOfInheritance.*;
import static org.opencb.commons.datastore.core.QueryOptions.INCLUDE;
import static org.opencb.opencga.storage.core.variant.adaptors.VariantQueryParam.*;
import static org.opencb.opencga.storage.core.variant.query.VariantQueryUtils.*;
import static org.opencb.opencga.storage.core.variant.query.executors.CompoundHeterozygousQueryExecutor.MISSING_SAMPLE;

/**
 * Created on 28/02/17.
 *
 * @author Jacobo Coll &lt;jacobo167@gmail.com&gt;
 */
public class VariantCatalogQueryUtils extends CatalogUtils {

    public static final String SAMPLE_ANNOTATION_DESC =
            "Selects some samples using metadata information from Catalog. e.g. age>20;phenotype=hpo:123,hpo:456;name=smith";
    public static final QueryParam SAMPLE_ANNOTATION
            = QueryParam.create("sampleAnnotation", SAMPLE_ANNOTATION_DESC, QueryParam.Type.TEXT_ARRAY);
    public static final String PROJECT_DESC = ParamConstants.PROJECT_DESCRIPTION;
    public static final QueryParam PROJECT = QueryParam.create(ParamConstants.PROJECT_PARAM, PROJECT_DESC, QueryParam.Type.TEXT_ARRAY);

    public static final String FAMILY_DESC = "Filter variants where any of the samples from the given family contains the variant "
            + "(HET or HOM_ALT)";
    public static final QueryParam FAMILY =
            QueryParam.create("family", FAMILY_DESC, QueryParam.Type.TEXT);
    public static final String FAMILY_MEMBERS_DESC = "Sub set of the members of a given family";
    public static final QueryParam FAMILY_MEMBERS =
            QueryParam.create("familyMembers", FAMILY_MEMBERS_DESC, QueryParam.Type.TEXT);
    public static final String FAMILY_DISORDER_DESC = "Specify the disorder to use for the family segregation";
    public static final QueryParam FAMILY_DISORDER =
            QueryParam.create("familyDisorder", FAMILY_DISORDER_DESC, QueryParam.Type.TEXT);
    public static final String FAMILY_PROBAND_DESC = "Specify the proband child to use for the family segregation";
    public static final QueryParam FAMILY_PROBAND =
            QueryParam.create("familyProband", FAMILY_PROBAND_DESC, QueryParam.Type.TEXT);
    public static final String FAMILY_SEGREGATION_DESCR = "Filter by segregation mode from a given family. Accepted values: "
            + "[ autosomalDominant, autosomalRecessive, XLinkedDominant, XLinkedRecessive, YLinked, mitochondrial, "
            + "deNovo, mendelianError, compoundHeterozygous ]";
    public static final QueryParam FAMILY_SEGREGATION =
            QueryParam.create("familySegregation", FAMILY_SEGREGATION_DESCR, QueryParam.Type.TEXT);

    public static final String SAVED_FILTER_DESCR = "Use a saved filter at User level";
    public static final QueryParam SAVED_FILTER =
            QueryParam.create("savedFilter", SAVED_FILTER_DESCR, QueryParam.Type.TEXT);

    @Deprecated
    public static final QueryParam FAMILY_PHENOTYPE = FAMILY_DISORDER;
    @Deprecated
    public static final QueryParam MODE_OF_INHERITANCE = FAMILY_SEGREGATION;

    public static final String PANEL_DESC = "Filter by genes from the given disease panel";
    public static final QueryParam PANEL =
            QueryParam.create("panel", PANEL_DESC, QueryParam.Type.TEXT);
    public static final String PANEL_MOI_DESC = "Filter genes from specific panels that match certain mode of inheritance. " +
            "Accepted values : "
            + "[ autosomalDominant, autosomalRecessive, XLinkedDominant, XLinkedRecessive, YLinked, mitochondrial, "
            + "deNovo, mendelianError, compoundHeterozygous ]";
    public static final QueryParam PANEL_MODE_OF_INHERITANCE =
            QueryParam.create("panelModeOfInheritance", PANEL_MOI_DESC
                    , QueryParam.Type.TEXT);
    public static final String PANEL_CONFIDENCE_DESC = "Filter genes from specific panels that match certain confidence. " +
            "Accepted values : [ high, medium, low, rejected ]";
    public static final QueryParam PANEL_CONFIDENCE =
            QueryParam.create("panelConfidence", PANEL_CONFIDENCE_DESC, QueryParam.Type.TEXT);

    public static final String PANEL_INTERSECTION_DESC = "Intersect panel genes and regions with given "
            + "genes and regions from que input query. This will prevent returning variants from regions out of the panel.";
    public static final QueryParam PANEL_INTERSECTION =
            QueryParam.create("panelIntersection", PANEL_INTERSECTION_DESC, Type.BOOLEAN);

    public static final String PANEL_ROLE_IN_CANCER_DESC = "Filter genes from specific panels that match certain role in cancer. " +
            "Accepted values : [ both, oncogene, tumorSuppressorGene, fusion ]";
    public static final QueryParam PANEL_ROLE_IN_CANCER =
            QueryParam.create("panelRoleInCancer", PANEL_ROLE_IN_CANCER_DESC, QueryParam.Type.TEXT);

    public static final String PANEL_FEATURE_TYPE_DESC = "Filter elements from specific panels by type. " +
            "Accepted values : [ gene, region, str, variant ]";
    public static final QueryParam PANEL_FEATURE_TYPE =
            QueryParam.create("panelFeatureType", PANEL_FEATURE_TYPE_DESC, QueryParam.Type.TEXT);

    public static final List<QueryParam> VARIANT_CATALOG_QUERY_PARAMS = Arrays.asList(
            SAMPLE_ANNOTATION,
            PROJECT,
            FAMILY,
            FAMILY_MEMBERS,
            FAMILY_DISORDER,
            FAMILY_PROBAND,
            FAMILY_SEGREGATION,
            PANEL,
            PANEL_MODE_OF_INHERITANCE,
            PANEL_CONFIDENCE,
            PANEL_ROLE_IN_CANCER,
            PANEL_FEATURE_TYPE,
            PANEL_INTERSECTION,
            SAVED_FILTER
    );

//    public enum SegregationMode {
//        AUTOSOMAL_DOMINANT("monoallelic"),
//        AUTOSOMAL_RECESSIVE("biallelic"),
//        X_LINKED_DOMINANT,
//        X_LINKED_RECESSIVE,
//        Y_LINKED,
//        MITOCHONDRIAL,
//
//        DE_NOVO,
//        MENDELIAN_ERROR("me"),
//        COMPOUND_HETEROZYGOUS("ch");
//
//        private static Map<String, SegregationMode> namesMap;
//
//        static {
//            namesMap = new HashMap<>();
//            for (SegregationMode mode : values()) {
//                namesMap.put(mode.name().toLowerCase(), mode);
//                namesMap.put(mode.name().replace("_", "").toLowerCase(), mode);
//                if (mode.names != null) {
//                    for (String name : mode.names) {
//                        namesMap.put(name.toLowerCase(), mode);
//                    }
//                }
//            }
//        }
//
//        private final String[] names;
//
//        SegregationMode(String... names) {
//            this.names = names;
//        }
//
//        @Nullable
//        public static SegregationMode parseOrNull(String name) {
//            return namesMap.get(name.toLowerCase());
//        }
//
//        @Nonnull
//        public static SegregationMode parse(String name) {
//            SegregationMode segregationMode = namesMap.get(name.toLowerCase());
//            if (segregationMode == null) {
//                throw new VariantQueryException("Unknown SegregationMode value: '" + name + "'");
//            }
//            return segregationMode;
//        }
//
//    }

    private final StudyFilterValidator studyFilterValidator;
    private final FileFilterValidator fileFilterValidator;
    private final SampleFilterValidator sampleFilterValidator;
    private final CohortFilterValidator cohortFilterValidator;
    //    public static final QueryParam SAMPLE_FILTER_GENOTYPE = QueryParam.create("sampleFilterGenotype", "", QueryParam.Type.TEXT_ARRAY);
    protected static Logger logger = LoggerFactory.getLogger(VariantCatalogQueryUtils.class);

    public VariantCatalogQueryUtils(CatalogManager catalogManager) {
        super(catalogManager);
        studyFilterValidator = new StudyFilterValidator();
        fileFilterValidator = new FileFilterValidator();
        sampleFilterValidator = new SampleFilterValidator();
        cohortFilterValidator = new CohortFilterValidator();
    }

    public static QueryParam valueOf(String param) {
        QueryParam queryParam = VariantQueryParam.valueOf(param);
        if (queryParam == null) {
            queryParam = VARIANT_CATALOG_QUERY_PARAMS.stream().filter(q -> q.key().equals(param)).findFirst().orElse(null);
        }
        return queryParam;
    }

    public static VariantQueryException wrongReleaseException(VariantQueryParam param, String value, int release) {
        return new VariantQueryException("Unable to have '" + value + "' within '" + param.key() + "' filter. "
                + "Not part of release " + release);
    }

    /**
     * Transforms a high level Query to a query fully understandable by storage.
     *
     * @param query        High level query. Will be modified by the method.
     * @param queryOptions Query options. Won't be modified
     * @param token        User's session id
     * @return Modified input query (same instance)
     * @throws CatalogException if there is any catalog error
     */
    public Query parseQuery(Query query, QueryOptions queryOptions, String token) throws CatalogException {
        return parseQuery(query, queryOptions, null, token);
    }

    /**
     * Transforms a high level Query to a query fully understandable by storage.
     *
     * @param query         High level query. Will be modified by the method.
     * @param queryOptions  Query options. Won't be modified
     * @param cellBaseUtils Cellbase utils
     * @param token         User's session id
     * @return Modified input query (same instance)
     * @throws CatalogException if there is any catalog error
     */
    public Query parseQuery(Query query, QueryOptions queryOptions, CellBaseUtils cellBaseUtils, String token) throws CatalogException {
        if (query == null) {
            // Nothing to do!
            return new Query();
        }

        if (isValidParam(query, SAVED_FILTER)) {
            String savedFilter = query.getString(SAVED_FILTER.key());
            String userId = catalogManager.getUserManager().getUserId(token);
            UserFilter userFilter = catalogManager.getUserManager().getFilter(userId, savedFilter, token).first();
            if (!userFilter.getResource().equals(Enums.Resource.VARIANT)) {
                throw VariantQueryException.malformedParam(SAVED_FILTER, savedFilter,
                        "The selected saved filter is not a filter for '" + Enums.Resource.VARIANT + "'. "
                                + "It is a filter for '" + userFilter.getResource() + "'");
            }

            userFilter.getQuery().forEach(query::putIfAbsent);
        }

        List<String> studies = getStudies(query, token);
        String defaultStudyStr = getDefaultStudyId(studies);
        Integer release = getReleaseFilter(query, token);

        studyFilterValidator.processFilter(query, VariantQueryParam.STUDY, release, token, defaultStudyStr);
        studyFilterValidator.processFilter(query, VariantQueryParam.INCLUDE_STUDY, release, token, defaultStudyStr);
        sampleFilterValidator.processFilter(query, VariantQueryParam.SAMPLE, release, token, defaultStudyStr);
        sampleFilterValidator.processFilter(query, VariantQueryParam.INCLUDE_SAMPLE, release, token, defaultStudyStr);
        sampleFilterValidator.processFilter(query, VariantQueryParam.GENOTYPE, release, token, defaultStudyStr);
        fileFilterValidator.processFilter(query, VariantQueryParam.FILE, release, token, defaultStudyStr);
        fileFilterValidator.processFilter(query, VariantQueryParam.INCLUDE_FILE, release, token, defaultStudyStr);
        cohortFilterValidator.processFilter(query, VariantQueryParam.COHORT, release, token, defaultStudyStr);
        cohortFilterValidator.processFilter(query, VariantQueryParam.STATS_ALT, release, token, defaultStudyStr);
        cohortFilterValidator.processFilter(query, VariantQueryParam.STATS_REF, release, token, defaultStudyStr);
        cohortFilterValidator.processFilter(query, VariantQueryParam.STATS_MAF, release, token, defaultStudyStr);
        cohortFilterValidator.processFilter(query, VariantQueryParam.STATS_MGF, release, token, defaultStudyStr);
        cohortFilterValidator.processFilter(query, VariantQueryParam.STATS_PASS_FREQ, release, token, defaultStudyStr);
        cohortFilterValidator.processFilter(query, VariantQueryParam.MISSING_ALLELES, release, token, defaultStudyStr);
        cohortFilterValidator.processFilter(query, VariantQueryParam.MISSING_GENOTYPES, release, token, defaultStudyStr);

        if (release != null) {
            // If include all files:
            if (VariantQueryProjectionParser.getIncludeFileStatus(query, VariantField.all())
                    .equals(VariantQueryProjectionParser.IncludeStatus.ALL)) {
                List<String> includeFiles = new ArrayList<>();
                QueryOptions fileOptions = new QueryOptions(INCLUDE, FileDBAdaptor.QueryParams.UID.key());
                Query fileQuery = new Query(FileDBAdaptor.QueryParams.RELEASE.key(), "<=" + release)
                        .append(FileDBAdaptor.QueryParams.INTERNAL_VARIANT_INDEX_STATUS_ID.key(), VariantIndexStatus.READY);

                for (String study : studies) {
                    for (File file : catalogManager.getFileManager().search(study, fileQuery, fileOptions, token)
                            .getResults()) {
                        includeFiles.add(file.getName());
                    }
                }
                query.append(VariantQueryParam.INCLUDE_FILE.key(), includeFiles);
            }
            // If include all samples:
            if (VariantQueryProjectionParser.getIncludeFileStatus(query, VariantField.all())
                    .equals(VariantQueryProjectionParser.IncludeStatus.ALL)) {
                List<String> includeSamples = new ArrayList<>();
                Query sampleQuery = new Query(SampleDBAdaptor.QueryParams.RELEASE.key(), "<=" + release);
                QueryOptions sampleOptions = new QueryOptions(INCLUDE, SampleDBAdaptor.QueryParams.UID.key());

                for (String study : studies) {
                    Query cohortQuery = new Query(CohortDBAdaptor.QueryParams.ID.key(), StudyEntry.DEFAULT_COHORT);
                    QueryOptions cohortOptions = new QueryOptions(INCLUDE, CohortDBAdaptor.QueryParams.SAMPLE_UIDS.key());
                    // Get default cohort. It contains the list of indexed samples. If it doesn't exist, or is empty, do not include any
                    // sample from this study.
                    DataResult<Cohort> result = catalogManager.getCohortManager().search(study, cohortQuery, cohortOptions, token);
                    if (result.first() != null || result.first().getSamples().isEmpty()) {
                        Set<String> sampleIds = result
                                .first()
                                .getSamples()
                                .stream()
                                .map(Sample::getId)
                                .collect(Collectors.toSet());
                        for (Sample s : catalogManager.getSampleManager().search(study, sampleQuery, sampleOptions, token)
                                .getResults()) {
                            if (sampleIds.contains(s.getId())) {
                                includeSamples.add(s.getId());
                            }
                        }
                    }
                }
                query.append(VariantQueryParam.INCLUDE_SAMPLE.key(), includeSamples);
            }
        }

        if (isValidParam(query, SAMPLE_ANNOTATION)) {
            String sampleAnnotation = query.getString(SAMPLE_ANNOTATION.key());
            Query sampleQuery = parseSampleAnnotationQuery(sampleAnnotation, SampleDBAdaptor.QueryParams::getParam);
            sampleQuery.append(SampleDBAdaptor.QueryParams.STUDY_UID.key(), defaultStudyStr);
            QueryOptions options = new QueryOptions(INCLUDE, SampleDBAdaptor.QueryParams.UID);
            List<String> sampleIds = catalogManager.getSampleManager().search(defaultStudyStr, sampleQuery, options, token)
                    .getResults()
                    .stream()
                    .map(Sample::getId)
                    .collect(Collectors.toList());

            if (sampleIds.isEmpty()) {
                throw new VariantQueryException("Could not found samples with this annotation: " + sampleAnnotation);
            }

            String genotype = query.getString("sampleAnnotationGenotype");
//            String genotype = query.getString(VariantDBAdaptor.VariantQueryParams.GENOTYPE.key());
            if (StringUtils.isNotBlank(genotype)) {
                StringBuilder sb = new StringBuilder();
                for (String sampleId : sampleIds) {
                    sb.append(sampleId).append(IS)
                            .append(genotype)
                            .append(AND); // TODO: Should this be an AND (;) or an OR (,)?
                }
                query.append(VariantQueryParam.GENOTYPE.key(), sb.toString());
                if (!isValidParam(query, VariantQueryParam.INCLUDE_SAMPLE)) {
                    query.append(VariantQueryParam.INCLUDE_SAMPLE.key(), sampleIds);
                }
            } else {
                query.append(VariantQueryParam.SAMPLE.key(), sampleIds);
            }
        }


        if (isValidParam(query, FAMILY)) {
            String familyId = query.getString(FAMILY.key());
            if (StringUtils.isEmpty(defaultStudyStr)) {
                throw VariantQueryException.missingStudyFor("family", familyId, null);
            }
            Family family = catalogManager.getFamilyManager().get(defaultStudyStr, familyId, null, token).first();

            if (family.getMembers().isEmpty()) {
                throw VariantQueryException.malformedParam(FAMILY, familyId, "Empty family");
            }
            List<String> familyMembers = query.getAsStringList(FAMILY_MEMBERS.key());
            if (familyMembers.size() == 1) {
                throw VariantQueryException.malformedParam(FAMILY_MEMBERS, familyMembers.toString(), "Only one member provided");
            }

            Set<Long> indexedSampleUids = fetchIndexedSampleUIds(token, defaultStudyStr);

            boolean multipleSamplesPerIndividual = false;
            List<Long> sampleUids = new ArrayList<>();
            Map<String, Long> individualToSampleUid = new HashMap<>();
            if (!familyMembers.isEmpty()) {
                family.getMembers().removeIf(member -> !familyMembers.contains(member.getId()));
                if (family.getMembers().size() != familyMembers.size()) {
                    List<String> actualFamilyMembers = family.getMembers().stream().map(Individual::getId).collect(Collectors.toList());
                    List<String> membersNotInFamily = familyMembers.stream()
                            .filter(member -> !actualFamilyMembers.contains(member))
                            .collect(Collectors.toList());
                    throw VariantQueryException.malformedParam(FAMILY_MEMBERS, familyMembers.toString(),
                            "Members " + membersNotInFamily + " not present in family '" + family.getId() + "'. "
                                    + "Family members: " + actualFamilyMembers);
                }
            }
            for (Iterator<Individual> iterator = family.getMembers().iterator(); iterator.hasNext(); ) {
                Individual member = iterator.next();
                int numSamples = 0;
                for (Iterator<Sample> sampleIt = member.getSamples().iterator(); sampleIt.hasNext(); ) {
                    Sample sample = sampleIt.next();
                    long uid = sample.getUid();
                    if (indexedSampleUids.contains(uid)) {
                        numSamples++;
                        sampleUids.add(uid);
                        individualToSampleUid.put(member.getId(), uid);
                    } else {
                        sampleIt.remove();
                    }
                }
                if (numSamples == 0) {
                    iterator.remove();
                }
                multipleSamplesPerIndividual |= numSamples > 1;
            }
            if (sampleUids.isEmpty()) {
                throw VariantQueryException.malformedParam(FAMILY, familyId, "Family not indexed in storage");
            }

            List<Sample> samples = catalogManager.getSampleManager().search(defaultStudyStr,
                    new Query(SampleDBAdaptor.QueryParams.UID.key(), sampleUids), new QueryOptions(INCLUDE, Arrays.asList(
                            SampleDBAdaptor.QueryParams.ID.key(),
                            SampleDBAdaptor.QueryParams.UID.key())), token).getResults();
            Map<Long, Sample> sampleMap = samples.stream().collect(Collectors.toMap(Sample::getUid, s -> s));

            // By default, include all samples from the family
            if (!isValidParam(query, INCLUDE_SAMPLE)) {
                query.append(INCLUDE_SAMPLE.key(), samples.stream().map(Sample::getId).collect(Collectors.toList()));
            }

            // If filter FAMILY is among with MODE_OF_INHERITANCE, fill the list of genotypes.
            // Otherwise, add the samples from the family to the SAMPLES query param.
            if (isValidParam(query, FAMILY_SEGREGATION)) {
                if (isValidParam(query, GENOTYPE)) {
                    throw VariantQueryException.malformedParam(FAMILY_SEGREGATION, query.getString(FAMILY_SEGREGATION.key()),
                            "Can not be used along with filter \"" + GENOTYPE.key() + '"');
                }
                if (isValidParam(query, SAMPLE)) {
                    throw VariantQueryException.malformedParam(FAMILY_SEGREGATION, query.getString(FAMILY_SEGREGATION.key()),
                            "Can not be used along with filter \"" + SAMPLE.key() + '"');
                }
                if (multipleSamplesPerIndividual) {
                    throw VariantQueryException.malformedParam(FAMILY, familyId,
                            "Some individuals from this family have multiple indexed samples");
                }
                if (sampleUids.size() == 1) {
                    throw VariantQueryException.malformedParam(FAMILY_SEGREGATION, familyId,
                            "Only one member of the family is indexed in storage");
                }
                Pedigree pedigree = FamilyManager.getPedigreeFromFamily(family, null);
                PedigreeManager pedigreeManager = new PedigreeManager(pedigree);

                String proband = query.getString(FAMILY_PROBAND.key());
                ClinicalProperty.ModeOfInheritance segregationMode = parse(query.getString(FAMILY_SEGREGATION.key()));

                List<Member> children;
                if (StringUtils.isNotEmpty(proband)) {
                    String memberIndividualId = toIndividualId(defaultStudyStr, proband, token);

                    Member probandMember = pedigree.getMembers()
                            .stream()
                            .filter(member -> member.getId().equals(memberIndividualId))
                            .findFirst()
                            .orElse(null);
                    if (probandMember == null) {
                        throw VariantQueryException.malformedParam(FAMILY_PROBAND, proband,
                                "Individual '" + memberIndividualId + "' " + "not found in family '" + familyId + "'.");
                    }
                    children = Collections.singletonList(probandMember);
                } else {
                    children = pedigreeManager.getWithoutChildren();
                }

                if (segregationMode == MENDELIAN_ERROR || segregationMode == DE_NOVO) {
                    List<String> childrenIds = children.stream().map(Member::getId).collect(Collectors.toList());
                    List<String> childrenSampleIds = new ArrayList<>(childrenIds.size());

                    for (String childrenId : childrenIds) {
                        Long sampleUid = individualToSampleUid.get(childrenId);
                        Sample sample = sampleMap.get(sampleUid);
                        if (sample == null) {
                            throw new VariantQueryException("Sample not found for individual \"" + childrenId + '"');
                        }
                        childrenSampleIds.add(sample.getId());
                    }

                    if (segregationMode == DE_NOVO) {
                        query.put(SAMPLE_DE_NOVO.key(), childrenSampleIds);
                    } else {
                        query.put(SAMPLE_MENDELIAN_ERROR.key(), childrenSampleIds);
                    }
                } else if (segregationMode == COMPOUND_HETEROZYGOUS) {
                    if (children.size() > 1) {
                        String childrenStr = children.stream().map(Member::getId).collect(Collectors.joining("', '", "[ '", "' ]"));
                        throw new VariantQueryException(
                                "Unsupported compoundHeterozygous method with families with more than one child."
                                        + " Specify proband with parameter '" + FAMILY_PROBAND.key() + "'."
                                        + " Available children: " + childrenStr);
                    }

                    Member child = children.get(0);

                    String childId = sampleMap.get(individualToSampleUid.get(child.getId())).getId();
                    String fatherId = MISSING_SAMPLE;
                    String motherId = MISSING_SAMPLE;
                    if (child.getFather() != null && child.getFather().getId() != null) {
                        Sample fatherSample = sampleMap.get(individualToSampleUid.get(child.getFather().getId()));
                        if (fatherSample != null) {
                            fatherId = fatherSample.getId();
                        }
                    }

                    if (child.getMother() != null && child.getMother().getId() != null) {
                        Sample motherSample = sampleMap.get(individualToSampleUid.get(child.getMother().getId()));
                        if (motherSample != null) {
                            motherId = motherSample.getId();
                        }
                    }

                    if (fatherId.equals(MISSING_SAMPLE) && motherId.equals(MISSING_SAMPLE)) {
                        throw VariantQueryException.malformedParam(FAMILY_SEGREGATION, segregationMode.toString(),
                                "Require at least one parent to get compound heterozygous");
                    }

                    query.append(SAMPLE_COMPOUND_HETEROZYGOUS.key(), Arrays.asList(childId, fatherId, motherId));
                } else {
                    if (family.getDisorders().isEmpty()) {
                        throw VariantQueryException.malformedParam(FAMILY, familyId, "Family doesn't have disorders");
                    }
                    Disorder disorder;
                    if (isValidParam(query, FAMILY_DISORDER)) {
                        String disorderId = query.getString(FAMILY_DISORDER.key());
                        disorder = family.getDisorders()
                                .stream()
                                .filter(familyDisorder -> familyDisorder.getId().equals(disorderId))
                                .findFirst()
                                .orElse(null);
                        if (disorder == null) {
                            throw VariantQueryException.malformedParam(FAMILY_DISORDER, disorderId,
                                    "Available disorders: " + family.getDisorders()
                                            .stream()
                                            .map(Disorder::getId)
                                            .collect(Collectors.toList()));
                        }

                    } else {
                        if (family.getDisorders().size() > 1) {
                            throw VariantQueryException.missingParam(FAMILY_DISORDER,
                                    "More than one disorder found for the family \"" + familyId + "\". "
                                            + "Available disorders: " + family.getDisorders()
                                            .stream()
                                            .map(Disorder::getId)
                                            .collect(Collectors.toList()));
                        }
                        disorder = family.getDisorders().get(0);
                    }

                    Map<Long, String> samplesUidToId = new HashMap<>();
                    for (Sample sample : samples) {
                        samplesUidToId.put(sample.getUid(), sample.getId());
                    }

                    Map<String, String> individualToSample = new HashMap<>();
                    for (Map.Entry<String, Long> entry : individualToSampleUid.entrySet()) {
                        individualToSample.put(entry.getKey(), samplesUidToId.get(entry.getValue()));
                    }

                    String gtFilter = buildMoIGenotypeFilter(pedigree, disorder, segregationMode, individualToSample);
                    if (gtFilter == null) {
                        throw VariantQueryException.malformedParam(FAMILY_SEGREGATION, segregationMode.toString(),
                                "Invalid segregation mode for the family '" + family.getId() + "'");
                    }
                    query.put(GENOTYPE.key(), gtFilter);
                }
            } else {
                if (isValidParam(query, FAMILY_DISORDER)) {
                    throw VariantQueryException.malformedParam(FAMILY_DISORDER, query.getString(FAMILY_DISORDER.key()),
                            "Require parameter \"" + FAMILY.key() + "\" and \"" + FAMILY_SEGREGATION.key() + "\" to use \""
                                    + FAMILY_DISORDER.key() + "\".");
                }

                List<String> sampleIds = new ArrayList<>();
                if (isValidParam(query, VariantQueryParam.SAMPLE)) {
//                    Pair<QueryOperation, List<String>> pair = splitValue(query.getString(VariantQueryParam.SAMPLE.key()));
//                    if (pair.getKey().equals(QueryOperation.AND)) {
//                        throw VariantQueryException.malformedParam(VariantQueryParam.SAMPLE, familyId,
//                                "Can not be used along with filter \"" + FAMILY.key() + "\" with operator AND (" + AND + ").");
//                    }
//                    sampleIds.addAll(pair.getValue());
                    throw VariantQueryException.unsupportedParamsCombination(SAMPLE, query.getString(SAMPLE.key()), FAMILY, familyId);
                }

                for (Sample sample : samples) {
                    sampleIds.add(sample.getId());
                }

                query.put(VariantQueryParam.SAMPLE.key(), String.join(OR, sampleIds));
            }
        } else if (isValidParam(query, FAMILY_MEMBERS)) {
            throw VariantQueryException.malformedParam(FAMILY_MEMBERS, query.getString(FAMILY_MEMBERS.key()),
                    "Require parameter \"" + FAMILY.key() + "\" to use \"" + FAMILY_MEMBERS.toString() + "\".");
        } else if (isValidParam(query, FAMILY_PROBAND)) {
            throw VariantQueryException.malformedParam(FAMILY_PROBAND, query.getString(FAMILY_PROBAND.key()),
                    "Require parameter \"" + FAMILY.key() + "\" to use \"" + FAMILY_PROBAND.toString() + "\".");
        } else if (isValidParam(query, FAMILY_SEGREGATION)) {
            throw VariantQueryException.malformedParam(FAMILY_SEGREGATION, query.getString(FAMILY_SEGREGATION.key()),
                    "Require parameter \"" + FAMILY.key() + "\" to use \"" + FAMILY_SEGREGATION.toString() + "\".");
        } else if (isValidParam(query, FAMILY_DISORDER)) {
            throw VariantQueryException.malformedParam(FAMILY_DISORDER, query.getString(FAMILY_DISORDER.key()),
                    "Require parameter \"" + FAMILY.key() + "\" and \"" + FAMILY_SEGREGATION.key() + "\" to use \""
                            + FAMILY_DISORDER.toString() + "\".");
        }

        if (isValidParam(query, SAMPLE)) {
            processSampleFilter(query, defaultStudyStr, token);
        }

        if (isValidParam(query, PANEL)) {
            String assembly = null;
            Set<String> panelGenes = new HashSet<>();
            Set<Region> panelRegions = new HashSet<>();
            Set<String> variants = new HashSet<>();
            List<String> panels = query.getAsStringList(PANEL.key());

            List<String> featureType = query.getAsStringList(PANEL_FEATURE_TYPE.key());

            for (String panelId : panels) {
                Panel panel = getPanel(defaultStudyStr, panelId, token);
                if (featureType.isEmpty() || featureType.contains("gene")) {
                    panelGenes.addAll(getGenesFromPanel(query, panel));
                }

                if (CollectionUtils.isNotEmpty(panel.getRegions())
                        || CollectionUtils.isNotEmpty(panel.getStrs())
                        || CollectionUtils.isNotEmpty(panel.getVariants())) {
                    if (assembly == null) {
                        Project project = getProjectFromQuery(query, token,
                                new QueryOptions(INCLUDE, ProjectDBAdaptor.QueryParams.ORGANISM.key()));
                        assembly = project.getOrganism().getAssembly();
                    }
                    if (featureType.isEmpty() || featureType.contains("region")) {
                        if (panel.getRegions() != null) {
                            for (DiseasePanel.RegionPanel region : panel.getRegions()) {
                                for (DiseasePanel.Coordinate coordinate : region.getCoordinates()) {
                                    if (coordinate.getAssembly().equalsIgnoreCase(assembly)) {
                                        panelRegions.add(Region.parseRegion(coordinate.getLocation()));
                                    }
                                }
                            }
                        }
                    }
                    // TODO: Check assembly of variants
//                    if (panel.getVariants() != null) {
//                        for (DiseasePanel.VariantPanel variant : panel.getVariants()) {
//                            variant.getId()
//                        }
//                    }
                }
            }
            Set<Region> queryRegions = isValidParam(query, REGION)
                    ? new HashSet<>(mergeRegions(Region.parseRegions(query.getString(REGION.key()), true)))
                    : Collections.emptySet();

            ParsedVariantQuery.VariantQueryXref xrefs = VariantQueryParser.parseXrefs(query);
            // Extract GENEs from XRefs
            query.put(GENE.key(), xrefs.getGenes());
            query.put(ANNOT_XREF.key(), xrefs.getOtherXrefs());

            if (queryRegions.isEmpty() && xrefs.getGenes().isEmpty() && xrefs.getVariants().isEmpty()) {
                // Nothing to intersect
                query.put(REGION.key(), panelRegions);
                query.put(GENE.key(), panelGenes);
                query.put(SKIP_MISSING_GENES, true);
            } else if (!query.getBoolean(PANEL_INTERSECTION.key(), false)) {
                // Union panel and query
                panelRegions.addAll(queryRegions);
                query.put(REGION.key(), panelRegions);

                panelGenes.addAll(xrefs.getGenes());
                query.put(GENE.key(), panelGenes);
                query.put(SKIP_MISSING_GENES, true);
            } else {
                logger.info("Panel intersection");
                // Intersect panel with query!
                // genesFinal
                //  - Genes in QUERY + genes in PANEL
                //  - Genes in QUERY + REGIONS from PANEL (potentially partial)
                //  - Genes in PANEL + REGIONS from QUERY (potentially partial)
                // regionsFinal
                //  - Regions in QUERY + regions in PANEL
                // variantsFinal
                //  - Variant in QUERY + genes in PANEL
                //  - Variant in QUERY + regions in PANEL

                Set<String> queryGenes = new HashSet<>(xrefs.getGenes());
                List<Variant> queryVariants = xrefs.getVariants();
                Set<String> genesFinal = new HashSet<>();
                Map<String, Region> geneRegionsFinal = new HashMap<>();
                List<Variant> variantsFinal = new LinkedList<>();
                Set<Region> regionsFinal = new HashSet<>();

                // Genes in PANEL + genes in QUERY
                if (!queryGenes.isEmpty() && !panelGenes.isEmpty()) {
                    // Genes in PANEL + genes in QUERY
                    for (String gene : panelGenes) {
                        if (queryGenes.contains(gene)) {
                            genesFinal.add(gene);
                        }
                    }
                }
                Map<String, Region> panelGeneRegionMap = cellBaseUtils.getGeneRegionMap(new ArrayList<>(panelGenes), true);
                Map<String, Region> queryGeneRegionMap = cellBaseUtils.getGeneRegionMap(new ArrayList<>(queryGenes), false);

                // Genes in PANEL + genes in REGIONS from QUERY (partial)
                geneRegionIntersect(panelGeneRegionMap, queryRegions, genesFinal, geneRegionsFinal);

                // Genes in QUERY + genes in REGIONS from PANEL (partial)
                geneRegionIntersect(queryGeneRegionMap, panelRegions, genesFinal, geneRegionsFinal);

                // Regions in PANEL + regions in QUERY
                if (!queryRegions.isEmpty()) {
                    if (!panelRegions.isEmpty()) {
                        for (Region queryRegion : queryRegions) {
                            for (Region panelRegion : panelRegions) {
                                Region region = intersectRegions(queryRegion, panelRegion);
                                if (region != null) {
                                    regionsFinal.add(region);
                                }
                            }
                        }
                    } // If panelRegions is empty, the QueryRegions will be already intersected with panelGenes
                }

                if (!geneRegionsFinal.isEmpty()) {
                    // Partial genes in query. Translate all genes to Regions
                    geneRegionsFinal.putAll(cellBaseUtils.getGeneRegionMap(new ArrayList<>(genesFinal), true));
                }
                if (!queryVariants.isEmpty()) {
                    //  - Variant in QUERY + genes in PANEL
                    //  - Variant in QUERY + regions in PANEL
                    for (Variant variant : queryVariants) {
                        for (Region region : Iterables.concat(panelGeneRegionMap.values(), panelRegions)) {
                            if (region.contains(variant.getChromosome(), variant.getStart())) {
                                variantsFinal.add(variant);
                                break;
                            }
                        }
                    }
                }

                logger.info("- Panel : {genes: {}, regions: {} }", panelGenes.size(), panelRegions.size());
                logger.info("- Query : {genes: {}, regions: {}, variants: {} }",
                        queryGenes.size(), queryRegions.size(), queryVariants.size());
                logger.info("- Intersection : {genes: {}, partialGenes:{}, regions: {}, variants: {} }",
                        genesFinal.size(), Math.max(0, geneRegionsFinal.size() - genesFinal.size()),
                        regionsFinal.size(), variantsFinal.size());
                if (!geneRegionsFinal.isEmpty()) {
                    // Partial genes. Translate finalGenes to Regions
                    List<Region> geneRegions = new ArrayList<>(geneRegionsFinal.values());
                    mergeRegions(geneRegions);
                    query.put(ANNOT_GENE_REGIONS.key(), geneRegions);
                    Map<String, Region> allGeneRegionMap = new HashMap<>(panelGeneRegionMap);
                    allGeneRegionMap.putAll(queryGeneRegionMap);
                    query.put(ANNOT_GENE_REGIONS_MAP.key(), allGeneRegionMap);

                    List<String> genes = new ArrayList<>(genesFinal);
                    genes.addAll(geneRegionsFinal.keySet());
                    query.put(GENE.key(), genes);
                } else {
                    query.put(GENE.key(), genesFinal);
                    query.put(SKIP_MISSING_GENES, true);
                }

                if (variantsFinal.isEmpty()) {
                    query.remove(ID.key());
                } else {
                    query.put(ID.key(), variantsFinal);
                }
                if (regionsFinal.isEmpty()) {
                    // Discard regions from query. Already in "ANNOT_GENE_REGIONS"
                    query.remove(REGION.key());
                } else {
                    query.put(REGION.key(), regionsFinal);
                }
                if (genesFinal.isEmpty() && variantsFinal.isEmpty() && regionsFinal.isEmpty()) {
                    query.put(REGION.key(), VariantQueryUtils.NON_EXISTING_REGION);
                }
            }
        } else {
            if (isValidParam(query, PANEL_CONFIDENCE)) {
                throw VariantQueryException.malformedParam(PANEL_CONFIDENCE, query.getString(PANEL_CONFIDENCE.key()),
                        "Require parameter \"" + PANEL.key() + "\" to use \"" + PANEL_CONFIDENCE.toString() + "\".");
            }
            if (isValidParam(query, PANEL_MODE_OF_INHERITANCE)) {
                throw VariantQueryException.malformedParam(PANEL_MODE_OF_INHERITANCE, query.getString(PANEL_MODE_OF_INHERITANCE.key()),
                        "Require parameter \"" + PANEL.key() + "\" to use \"" + PANEL_MODE_OF_INHERITANCE.toString() + "\".");
            }
            if (isValidParam(query, PANEL_ROLE_IN_CANCER)) {
                throw VariantQueryException.malformedParam(PANEL_ROLE_IN_CANCER, query.getString(PANEL_ROLE_IN_CANCER.key()),
                        "Require parameter \"" + PANEL.key() + "\" to use \"" + PANEL_ROLE_IN_CANCER.toString() + "\".");
            }
            if (isValidParam(query, PANEL_FEATURE_TYPE)) {
                throw VariantQueryException.malformedParam(PANEL_FEATURE_TYPE, query.getString(PANEL_FEATURE_TYPE.key()),
                        "Require parameter \"" + PANEL.key() + "\" to use \"" + PANEL_FEATURE_TYPE.toString() + "\".");
            }
        }

        logger.debug("Catalog parsed query : " + VariantQueryUtils.printQuery(query));

        return query;
    }

    public static void geneRegionIntersect(CellBaseUtils cellBaseUtils,
                                           Set<String> genes,
                                           Set<Region> regions,
                                           Set<String> genesFinal,
                                           Map<String, Region> geneRegionsFinal) {
        geneRegionIntersect(cellBaseUtils.getGeneRegionMap(new ArrayList<>(genes), true), regions, genesFinal, geneRegionsFinal);
    }

    public static void geneRegionIntersect(Map<String, Region> geneRegionMap,
                                           Set<Region> regions,
                                           Set<String> genesFinal,
                                           Map<String, Region> geneRegionsFinal) {
        if (!geneRegionMap.isEmpty() && !regions.isEmpty()) {
            geneRegionMap.forEach((gene, geneRegion) -> {
                if (!genesFinal.contains(gene)) {
                    for (Region region : regions) {
                        Region intersect = intersectRegions(region, geneRegion);
                        if (intersect != null) {
                            // There was an overlap!
                            if (intersect.equals(geneRegion)) {
                                // This gene is fully covered!
                                genesFinal.add(gene);
                            } else {
                                // Partially covered.
                                geneRegionsFinal.put(gene, intersect);
                            }
                            break;
                        }
                    }
                }
            });
        }
    }

    protected static Set<String> getGenesFromPanel(Query query, Panel panel) {
        Set<String> geneNames = new HashSet<>();
        Set<ClinicalProperty.Confidence> panelConfidence =
                new HashSet<>(getAsEnumList(query, PANEL_CONFIDENCE, ClinicalProperty.Confidence.class));
        Set<ClinicalProperty.ModeOfInheritance> panelModeOfInheritance =
                query.getAsStringList(PANEL_MODE_OF_INHERITANCE.key())
                        .stream()
                        .map(ClinicalProperty.ModeOfInheritance::parse)
                        .collect(Collectors.toSet());
        Set<ClinicalProperty.RoleInCancer> panelRoleInCancer =
                new HashSet<>(getAsEnumList(query, PANEL_ROLE_IN_CANCER, ClinicalProperty.RoleInCancer.class));

        for (GenePanel genePanel : panel.getGenes()) {
            // Do not filter out if undefined
            if (!panelConfidence.isEmpty()
                    && genePanel.getConfidence() != null
                    && !panelConfidence.contains(genePanel.getConfidence())) {
                // Discard this gene
                continue;
            }
            // Do not filter out if undefined
            if (!panelModeOfInheritance.isEmpty()
                    && genePanel.getModeOfInheritance() != null
                    && !panelModeOfInheritance.contains(genePanel.getModeOfInheritance())) {
                // Discard this gene
                continue;
            }
            // Do not filter out if undefined
            if (!panelRoleInCancer.isEmpty()
                    && genePanel.getCancer() != null && genePanel.getCancer().getRole() != null
                    && !panelRoleInCancer.contains(genePanel.getCancer().getRole())) {
                // Discard this gene
                continue;
            }
            String gene = genePanel.getName();
            if (StringUtils.isEmpty(gene)) {
                gene = genePanel.getId();
            }
            geneNames.add(gene);
        }
        return geneNames;
    }

    /**
     * Gets the individual ID given an individual or sample id.
     *
     * @param study             study
     * @param individuaOrSample either an individual or sample
     * @param token             user's token
     * @return individualId
     * @throws CatalogException on catalog exception
     */
    private String toIndividualId(String study, String individuaOrSample, String token) throws CatalogException {
        OpenCGAResult<Individual> result = catalogManager.getIndividualManager().search(study,
                new Query(IndividualDBAdaptor.QueryParams.SAMPLES.key(), individuaOrSample),
                new QueryOptions(INCLUDE, IndividualDBAdaptor.QueryParams.ID.key()),
                token);
        if (result.getNumResults() == 1) {
            individuaOrSample = result.first().getId();
        }
        return individuaOrSample;
    }

    private void processSampleFilter(Query query, String defaultStudyStr, String token) throws CatalogException {
        String sampleFilterValue = query.getString(SAMPLE.key());
        if (sampleFilterValue.contains(IS)) {
            ClinicalProperty.ModeOfInheritance moi = null;
            ParsedQuery<KeyOpValue<String, List<String>>> sampleFilter = parseGenotypeFilter(sampleFilterValue);
            for (KeyOpValue<String, List<String>> keyOpValue : sampleFilter.getValues()) {
                for (String value : keyOpValue.getValue()) {
                    ClinicalProperty.ModeOfInheritance aux = ClinicalProperty.ModeOfInheritance.parseOrNull(value);
                    if (aux != null) {
                        moi = aux;
                    }
                }
            }

            if (moi != null) {
                if (sampleFilter.getValues().size() != 1) {
                    throw VariantQueryException.malformedParam(SAMPLE, sampleFilterValue,
                            "Only one sample is allowed when filtering by segregation mode '" + moi + "'");
                }
                if (sampleFilter.getValues().get(0).getValue().size() != 1) {
                    throw VariantQueryException.malformedParam(SAMPLE, sampleFilterValue,
                            "Only one segregation mode is allowed when filtering by segregation mode");
                }
                String sampleId = sampleFilter.getValues().get(0).getKey();

                Sample sample = catalogManager.getSampleManager().get(defaultStudyStr, sampleId, new QueryOptions(), token).first();
                if (StringUtils.isEmpty(sample.getIndividualId())) {
                    throw VariantQueryException.malformedParam(SAMPLE, sampleFilterValue,
                            "Sample '" + sampleId + "' does not have an Individual associated.");
                }

                Set<Long> indexedSampleUids = fetchIndexedSampleUIds(token, defaultStudyStr);
                Individual individual = catalogManager.getIndividualManager().get(defaultStudyStr, sample.getIndividualId(), new QueryOptions(), token).first();

                Member member = new Member(sampleId, sampleId, individual.getSex());
                member.setDisorders(individual.getDisorders());

                if (individual.getFather() != null) {
                    Individual father = catalogManager.getIndividualManager().get(defaultStudyStr, individual.getFather().getId(), new QueryOptions(), token).first();
                    String fatherId = null;
                    int numSamples = 0;
                    for (Sample s : father.getSamples()) {
                        if (indexedSampleUids.contains(s.getUid())) {
                            numSamples++;
                            fatherId = s.getId();
                        }
                    }
                    if (numSamples > 1) {
                        throw VariantQueryException.malformedParam(SAMPLE, sampleFilterValue,
                                "Multiple samples found for individual '" + father.getId() + "'");
                    } else if (numSamples == 1) {
                        member.setFather(new Member(fatherId, fatherId, SexOntologyTermAnnotation.initMale()).setDisorders(father.getDisorders()));
                    }
                }
                if (individual.getMother() != null) {
                    Individual mother = catalogManager.getIndividualManager().get(defaultStudyStr, individual.getMother().getId(), new QueryOptions(), token).first();
                    String motherId = null;
                    int numSamples = 0;
                    for (Sample s : mother.getSamples()) {
                        if (indexedSampleUids.contains(s.getUid())) {
                            numSamples++;
                            motherId = s.getId();
                        }
                    }
                    if (numSamples > 1) {
                        throw VariantQueryException.malformedParam(SAMPLE, sampleFilterValue,
                                "Multiple samples found for individual '" + mother.getId() + "'");
                    } else if (numSamples == 1) {
                        member.setMother(new Member(motherId, motherId, SexOntologyTermAnnotation.initFemale()).setDisorders(mother.getDisorders()));
                    }
                }

                if (individual.getMother() == null && individual.getFather() == null) {
                    throw VariantQueryException.malformedParam(SAMPLE, sampleFilterValue,
                            "Sample '" + sampleId + "' does not have parents defined or indexed in storage.");
                }

                if (!isValidParam(query, INCLUDE_SAMPLE)) {
                    List<String> includeSample = new ArrayList<>(3);
                    includeSample.add(member.getId());
                    if (member.getFather() != null) {
                        includeSample.add(member.getFather().getId());
                    }
                    if (member.getMother() != null) {
                        includeSample.add(member.getMother().getId());
                    }
                    query.put(INCLUDE_SAMPLE.key(), includeSample);
                }

                if (moi == ClinicalProperty.ModeOfInheritance.COMPOUND_HETEROZYGOUS) {
                    String fatherId = member.getFather() != null ? member.getFather().getId() : MISSING_SAMPLE;
                    String motherId = member.getMother() != null ? member.getMother().getId() : MISSING_SAMPLE;

                    query.put(SAMPLE_COMPOUND_HETEROZYGOUS.key(), Arrays.asList(member.getId(), fatherId, motherId));
                    query.remove(SAMPLE.key());
                } else if (moi == ClinicalProperty.ModeOfInheritance.DE_NOVO) {
                    query.put(SAMPLE_DE_NOVO.key(), member.getId());
                    query.remove(SAMPLE.key());
                } else if (moi == ClinicalProperty.ModeOfInheritance.MENDELIAN_ERROR) {
                    query.put(SAMPLE_MENDELIAN_ERROR.key(), member.getId());
                    query.remove(SAMPLE.key());
                } else {
                    Pedigree pedigree = new Pedigree("", new ArrayList<>(3), Collections.emptyMap());
                    pedigree.getMembers().add(member);
                    if (member.getFather() != null) {
                        pedigree.getMembers().add(member.getFather());
                    }
                    if (member.getMother() != null) {
                        pedigree.getMembers().add(member.getMother());
                    }
                    if (CollectionUtils.isEmpty(individual.getDisorders())) {
                        throw VariantQueryException.malformedParam(SAMPLE, sampleFilterValue,
                                "Missing disorder for sample '" + sampleId + "'");
                    } else if (individual.getDisorders().size() != 1) {
                        throw VariantQueryException.malformedParam(SAMPLE, sampleFilterValue,
                                "Found multiple disorders for sample '" + sampleId + "'");
                    }
                    String genotypeFilter = buildMoIGenotypeFilter(pedigree, individual.getDisorders().get(0), moi);
                    if (genotypeFilter == null) {
                        throw VariantQueryException.malformedParam(SAMPLE, sampleFilterValue,
                                "Invalid segregation mode for the sample '" + sampleId + "'");
                    }
                    query.put(GENOTYPE.key(), genotypeFilter);
                    query.remove(SAMPLE.key());
                }
            }

        }
    }

    private Set<Long> fetchIndexedSampleUIds(String token, String defaultStudyStr) throws CatalogException {
        // Use search instead of get to avoid smartResolutor to fetch all samples
        return catalogManager.getCohortManager()
                .search(defaultStudyStr, new Query(CohortDBAdaptor.QueryParams.ID.key(), StudyEntry.DEFAULT_COHORT),
                        new QueryOptions(INCLUDE, CohortDBAdaptor.QueryParams.SAMPLE_UIDS.key()), token)
                .first()
                .getSamples()
                .stream()
                .map(Sample::getUid).collect(Collectors.toSet());
    }


    private String buildMoIGenotypeFilter(Pedigree pedigree, Disorder disorder, ClinicalProperty.ModeOfInheritance moi) {
        return buildMoIGenotypeFilter(pedigree, disorder, moi, null);
    }

    private String buildMoIGenotypeFilter(Pedigree pedigree, Disorder disorder, ClinicalProperty.ModeOfInheritance moi,
                                          Map<String, String> pedigreeMemberToSampleId) {
        Map<String, List<String>> genotypes;
        switch (moi) {
            case AUTOSOMAL_DOMINANT:
                genotypes = ModeOfInheritance.dominant(pedigree, disorder, ClinicalProperty.Penetrance.COMPLETE);
                break;
//            case AUTOSOMAL_DOMINANT_INCOMPLETE_PENETRANCE:
//                genotypes = ModeOfInheritance.dominant(pedigree, disorder, ClinicalProperty.Penetrance.INCOMPLETE);
//                break;
            case AUTOSOMAL_RECESSIVE:
                genotypes = ModeOfInheritance.recessive(pedigree, disorder, ClinicalProperty.Penetrance.COMPLETE);
                break;
//            case AUTOSOMAL_RECESSIVE_INCOMPLETE_PENETRANCE:
//                genotypes = ModeOfInheritance.recessive(pedigree, disorder, ClinicalProperty.Penetrance.INCOMPLETE);
//                break;
            case X_LINKED_DOMINANT:
                genotypes = ModeOfInheritance.xLinked(pedigree, disorder, true, ClinicalProperty.Penetrance.COMPLETE);
                break;
            case X_LINKED_RECESSIVE:
                genotypes = ModeOfInheritance.xLinked(pedigree, disorder, false, ClinicalProperty.Penetrance.COMPLETE);
                break;
            case Y_LINKED:
                genotypes = ModeOfInheritance.yLinked(pedigree, disorder, ClinicalProperty.Penetrance.COMPLETE);
                break;
            case MITOCHONDRIAL:
                genotypes = ModeOfInheritance.mitochondrial(pedigree, disorder, ClinicalProperty.Penetrance.COMPLETE);
                break;
            default:
                throw new IllegalArgumentException("Unexpected segregation mode " + moi);
        }
        if (ModeOfInheritance.isEmptyMapOfGenotypes(genotypes)) {
            return null;
        }

        StringBuilder sb = new StringBuilder();

        // Sort by sampleId
        genotypes = new TreeMap<>(genotypes);

        boolean firstSample = true;
        for (Map.Entry<String, List<String>> entry : genotypes.entrySet()) {
            if (firstSample) {
                firstSample = false;
            } else {
                sb.append(AND);
            }
            if (pedigreeMemberToSampleId == null) {
                sb.append(entry.getKey());
            } else {
                sb.append(pedigreeMemberToSampleId.get(entry.getKey()));
            }
            sb.append(IS);

            boolean firstGenotype = true;
            for (String gt : entry.getValue()) {
                if (firstGenotype) {
                    firstGenotype = false;
                } else {
                    sb.append(OR);
                }
                sb.append(gt);
            }
        }
        return sb.toString();
    }

    /**
     * Get the panel from catalog.
     *
     * @param studyId   StudyId
     * @param panelId   PanelId
     * @param sessionId users sessionId
     * @return The panel
     * @throws CatalogException if the panel does not exist, or the user does not have permissions to see it.
     */
    public Panel getPanel(String studyId, String panelId, String sessionId) throws CatalogException {
        Panel panel = null;
        if (StringUtils.isNotEmpty(studyId)) {
            try {
                panel = catalogManager.getPanelManager().get(studyId, panelId, null, sessionId).first();
            } catch (CatalogException e) {
                logger.debug("Ignore Panel not found", e);
            }
        }
        if (panel == null) {
            throw new CatalogException("Panel '" + panelId + "' not found");
        }
        return panel;
    }

    public String getDefaultStudyId(Collection<String> studies) throws CatalogException {
        final String defaultStudyId;
        if (studies.size() == 1) {
            defaultStudyId = studies.iterator().next();
        } else {
            defaultStudyId = null;
        }
        return defaultStudyId;
    }

    public Integer getReleaseFilter(Query query, String sessionId) throws CatalogException {
        Integer release;
        if (isValidParam(query, VariantQueryParam.RELEASE)) {
            release = query.getInt(VariantQueryParam.RELEASE.key(), -1);
            if (release <= 0) {
                throw VariantQueryException.malformedParam(VariantQueryParam.RELEASE, query.getString(VariantQueryParam.RELEASE.key()));
            }
            Project project = getProjectFromQuery(query, sessionId,
                    new QueryOptions(INCLUDE, ProjectDBAdaptor.QueryParams.CURRENT_RELEASE.key()));
            int currentRelease = project.getCurrentRelease();
            if (release > currentRelease) {
                throw VariantQueryException.malformedParam(VariantQueryParam.RELEASE, query.getString(VariantQueryParam.RELEASE.key()));
            } else if (release == currentRelease) {
                // Using latest release. We don't need to filter by release!
                release = null;
            } // else, filter by release

        } else {
            release = null;
        }
        return release;
    }

    public List<List<String>> getTriosFromFamily(
            String studyFqn, Family family, VariantStorageMetadataManager metadataManager, boolean skipIncompleteFamily, String sessionId)
            throws StorageEngineException, CatalogException {
        List<List<String>> trios = getTrios(studyFqn, metadataManager, family.getMembers(), sessionId);
        if (trios.size() == 0) {
            if (skipIncompleteFamily) {
                logger.debug("Skip family '" + family.getId() + "'. ");
            } else {
                throw new StorageEngineException("Can not calculate mendelian errors on family '" + family.getId() + "'");
            }
        }
        return trios;
    }

    public List<List<String>> getTriosFromSamples(
            String studyFqn, VariantStorageMetadataManager metadataManager, Collection<String> sampleIds, String token)
            throws CatalogException {
        OpenCGAResult<Individual> individualResult = catalogManager.getIndividualManager()
                .search(studyFqn,
                        new Query(IndividualDBAdaptor.QueryParams.SAMPLES.key(), sampleIds),
                        new QueryOptions(QueryOptions.INCLUDE, Arrays.asList(
                                IndividualDBAdaptor.QueryParams.ID.key(),
                                IndividualDBAdaptor.QueryParams.NAME.key(),
                                IndividualDBAdaptor.QueryParams.UID.key(),
                                IndividualDBAdaptor.QueryParams.FATHER_UID.key(),
                                IndividualDBAdaptor.QueryParams.MOTHER_UID.key(),
                                IndividualDBAdaptor.QueryParams.SAMPLE_UIDS.key()
                        )), token);
        return getTrios(studyFqn, metadataManager, individualResult.getResults(), token);
    }

    public List<List<String>> getTrios(
            String studyFqn, VariantStorageMetadataManager metadataManager, List<Individual> membersList, String sessionId)
            throws CatalogException {
        int studyId = metadataManager.getStudyId(studyFqn);
        Map<Long, Individual> membersMap = membersList.stream().collect(Collectors.toMap(Individual::getUid, i -> i));
        List<List<String>> trios = new LinkedList<>();
        for (Individual individual : membersList) {
            String fatherSample = null;
            String motherSample = null;
            String childSample = null;

            if (CollectionUtils.isNotEmpty(individual.getSamples())) {
                for (Sample sample : individual.getSamples()) {
                    sample = catalogManager.getSampleManager().search(studyFqn,
                            new Query(SampleDBAdaptor.QueryParams.UID.key(), sample.getUid()),
                            new QueryOptions(QueryOptions.INCLUDE, SampleDBAdaptor.QueryParams.ID.key()), sessionId).first();
                    Integer sampleId = metadataManager.getSampleId(studyId, sample.getId(), true);
                    if (sampleId != null) {
                        childSample = sample.getId();
                        break;
                    }
                }
            }
            if (individual.getFather() != null && membersMap.containsKey(individual.getFather().getUid())) {
                Individual father = membersMap.get(individual.getFather().getUid());
                if (CollectionUtils.isNotEmpty(father.getSamples())) {
                    for (Sample sample : father.getSamples()) {
                        sample = catalogManager.getSampleManager().search(studyFqn,
                                new Query(SampleDBAdaptor.QueryParams.UID.key(), sample.getUid()),
                                new QueryOptions(QueryOptions.INCLUDE, SampleDBAdaptor.QueryParams.ID.key()), sessionId).first();
                        Integer sampleId = metadataManager.getSampleId(studyId, sample.getId(), true);
                        if (sampleId != null) {
                            fatherSample = sample.getId();
                            break;
                        }
                    }
                }
            }
            if (individual.getMother() != null && membersMap.containsKey(individual.getMother().getUid())) {
                Individual mother = membersMap.get(individual.getMother().getUid());
                if (CollectionUtils.isNotEmpty(mother.getSamples())) {
                    for (Sample sample : mother.getSamples()) {
                        sample = catalogManager.getSampleManager().search(studyFqn,
                                new Query(SampleDBAdaptor.QueryParams.UID.key(), sample.getUid()),
                                new QueryOptions(QueryOptions.INCLUDE, SampleDBAdaptor.QueryParams.ID.key()), sessionId).first();
                        Integer sampleId = metadataManager.getSampleId(studyId, sample.getId(), true);
                        if (sampleId != null) {
                            motherSample = sample.getId();
                            break;
                        }
                    }
                }
            }

            // Allow one missing parent
            if (childSample != null && (fatherSample != null || motherSample != null)) {
                trios.add(Arrays.asList(
                        fatherSample == null ? "-" : fatherSample,
                        motherSample == null ? "-" : motherSample,
                        childSample));
            }
        }
        return trios;
    }

    public abstract class FilterValidator {
        protected final QueryOptions RELEASE_OPTIONS = new QueryOptions(INCLUDE, Arrays.asList(
                FileDBAdaptor.QueryParams.ID.key(),
                FileDBAdaptor.QueryParams.NAME.key(),
                FileDBAdaptor.QueryParams.INTERNAL_VARIANT_INDEX.key(),
                FileDBAdaptor.QueryParams.RELEASE.key()));

        /**
         * Splits the value from the query (if any) and translates the IDs to numerical Ids.
         * If a release value is given, checks that every element is part of that release.
         *
         * @param query        Query with the data
         * @param param        Param to modify
         * @param release      Release filter, if any
         * @param sessionId    SessionId
         * @param defaultStudy Default study
         * @throws CatalogException if there is any catalog error
         */
        protected void processFilter(Query query, VariantQueryParam param, Integer release, String sessionId, String defaultStudy)
                throws CatalogException {
            if (VariantQueryUtils.isValidParam(query, param)) {
                String valuesStr = query.getString(param.key());
                // Do not try to transform ALL or NONE values
                if (isNoneOrAll(valuesStr)) {
                    return;
                }
                QueryOperation queryOperation = getQueryOperation(valuesStr);
                List<String> rawValues = splitValue(valuesStr, queryOperation);
                List<String> values = getValuesToValidate(rawValues);
                List<String> validatedValues = validate(defaultStudy, values, release, param, sessionId);

                StringBuilder sb = new StringBuilder();
                for (int i = 0; i < rawValues.size(); i++) {
                    String rawValue = rawValues.get(i);
                    String value = values.get(i);
                    String validatedValue = validatedValues.get(i);
                    if (sb.length() > 0) {
                        sb.append(queryOperation.separator());
                    }

                    if (!value.equals(validatedValue)) {
                        sb.append(StringUtils.replace(rawValue, value, validatedValue, 1));
                    } else {
                        sb.append(rawValue);
                    }

                }
                String newValue = sb.toString();
                query.put(param.key(), newValue);
            }
        }

        protected QueryOperation getQueryOperation(String valuesStr) {
            QueryOperation queryOperation = VariantQueryUtils.checkOperator(valuesStr);
            if (queryOperation == null) {
                queryOperation = QueryOperation.OR;
            }
            return queryOperation;
        }

        protected List<String> splitValue(String valuesStr, QueryOperation queryOperation) {
            return VariantQueryUtils.splitValue(valuesStr, queryOperation);
        }

        protected List<String> getValuesToValidate(List<String> rawValues) {
            return rawValues.stream()
                    .map(value -> {
                        value = isNegated(value) ? removeNegation(value) : value;
                        String[] strings = VariantQueryUtils.splitOperator(value);
                        boolean withComparisionOperator = strings[0] != null;
                        if (withComparisionOperator) {
                            value = strings[0];
                        }
                        return value;
                    })
                    .collect(Collectors.toList());
        }


        protected abstract List<String> validate(String defaultStudyStr, List<String> values, Integer release, VariantQueryParam param,
                                                 String sessionId)
                throws CatalogException;

        protected final void checkRelease(Integer release, int resourceRelease, VariantQueryParam param, String value) {
            if (release != null && resourceRelease > release) {
                throw wrongReleaseException(param, value, release);
            }
        }

        protected final <T extends PrivateStudyUid> List<String> validate(String defaultStudyStr, List<String> values, Integer release,
                                                                          VariantQueryParam param, ResourceManager<T> manager,
                                                                          Function<T, String> getId, Function<T, Integer> getRelease,
                                                                          Consumer<T> valueValidator, String sessionId)
                throws CatalogException {
            return validate(defaultStudyStr, values, release, param, manager, getId, getRelease, valueValidator, sessionId, new Query());
        }

        protected final <T extends PrivateStudyUid> List<String> validate(String defaultStudyStr, List<String> values, Integer release,
                                                                          VariantQueryParam param, ResourceManager<T> manager,
                                                                          Function<T, String> getId, Function<T, Integer> getRelease,
                                                                          Consumer<T> valueValidator, String sessionId, Query query)
                throws CatalogException {
            DataResult<T> queryResult = manager.get(defaultStudyStr, values, query, RELEASE_OPTIONS, false, sessionId);
            List<String> validatedValues = new ArrayList<>(values.size());
            for (T value : queryResult.getResults()) {
                if (valueValidator != null) {
                    valueValidator.accept(value);
                }
                String id = getId.apply(value);
                validatedValues.add(id);
                checkRelease(release, getRelease.apply(value), param, id);
            }
            return validatedValues;
        }
    }


    public class StudyFilterValidator extends FilterValidator {

        @Override
        protected List<String> validate(String defaultStudyStr, List<String> values, Integer release, VariantQueryParam param,
                                        String sessionId) throws CatalogException {
            if (release == null) {
                List<Study> studies = catalogManager.getStudyManager().get(values, StudyManager.INCLUDE_STUDY_IDS, false, sessionId)
                        .getResults();
                return studies.stream().map(Study::getFqn).collect(Collectors.toList());
            } else {
                List<String> validatedValues = new ArrayList<>(values.size());
                DataResult<Study> queryResult = catalogManager.getStudyManager().get(values, RELEASE_OPTIONS, false, sessionId);
                for (Study study : queryResult.getResults()) {
                    validatedValues.add(study.getFqn());
                    checkRelease(release, study.getRelease(), param, study.getFqn());
                }
                return validatedValues;
            }
        }
    }

    public class FileFilterValidator extends FilterValidator {

        @Override
        protected List<String> validate(String defaultStudyStr, List<String> values, Integer release, VariantQueryParam param,
                                        String sessionId)
                throws CatalogException {
            if (release == null) {
                DataResult<File> files = catalogManager.getFileManager().get(defaultStudyStr, values,
                        FileManager.INCLUDE_FILE_IDS, sessionId);
                return files.getResults().stream().map(File::getName).collect(Collectors.toList());
            } else {
                return validate(defaultStudyStr, values, release, param, catalogManager.getFileManager(), File::getName,
                        file -> file.getInternal().getVariant().getIndex().getRelease(), file -> {
                            if (!FileInternal.getVariantIndexStatusId(file.getInternal()).equals(InternalStatus.READY)) {
                                throw new VariantQueryException("File '" + file.getName() + "' is not indexed");
                            }
                        },
                        sessionId);

            }
        }
    }

    public class SampleFilterValidator extends FilterValidator {

        @Override
        protected QueryOperation getQueryOperation(String valuesStr) {
            if (valuesStr.contains(IS)) {
                Map<Object, List<String>> genotypesMap = new HashMap<>();
                return VariantQueryUtils.parseGenotypeFilter(valuesStr, genotypesMap);
            } else {
                return super.getQueryOperation(valuesStr);
            }
        }

        @Override
        protected List<String> splitValue(String valuesStr, QueryOperation queryOperation) {
            if (valuesStr.contains(IS)) {
                Map<Object, List<String>> genotypesMap = new LinkedHashMap<>();
                VariantQueryUtils.parseGenotypeFilter(valuesStr, genotypesMap);

                return genotypesMap.entrySet().stream().map(entry -> entry.getKey() + ":" + String.join(",", entry.getValue()))
                        .collect(Collectors.toList());
            } else {
                return super.splitValue(valuesStr, queryOperation);
            }
        }

        @Override
        protected List<String> getValuesToValidate(List<String> rawValues) {
            return rawValues.
                    stream()
                    .map(value -> value.split(":")[0])
                    .map(VariantQueryUtils::removeNegation)
                    .collect(Collectors.toList());
        }

        @Override
        protected List<String> validate(String defaultStudyStr, List<String> values, Integer release, VariantQueryParam param,
                                        String sessionId) throws CatalogException {
            if (release == null) {
                String userId = catalogManager.getUserManager().getUserId(sessionId);
//                DataResult<Sample> samples = catalogManager.getSampleManager().get(defaultStudyStr, values,
//                        SampleManager.INCLUDE_SAMPLE_IDS, sessionId);
                long numMatches = catalogManager.getSampleManager()
                        .count(defaultStudyStr, new Query(SampleDBAdaptor.QueryParams.ID.key(), values)
                                        .append(ParamConstants.ACL_PARAM, userId + ":" + SampleAclEntry.SamplePermissions.VIEW_VARIANTS),
                                sessionId).getNumMatches();
                if (numMatches != values.size()) {
                    OpenCGAResult<Sample> samples = catalogManager.getSampleManager()
                            .search(defaultStudyStr, new Query(SampleDBAdaptor.QueryParams.ID.key(), values),
                                    SampleManager.INCLUDE_SAMPLE_IDS, sessionId);
                    if (samples.getResults().size() != values.size()) {
                        // Can not view some samples. May not exist
                        throw new CatalogAuthorizationException("Some samples from query param '" + param.key() + "' can not be found");
                    } else {
                        // Can not view_variants in some samples.
                        throw new CatalogAuthorizationException("Some samples from query param '" + param.key() + "' "
                                + "can not be used for filtering variants");
                    }
                }
                return values;
            } else {
                return validate(defaultStudyStr, values, release, param, catalogManager.getSampleManager(),
                        Sample::getId, Sample::getRelease, null, sessionId);
            }
        }
    }

    public class CohortFilterValidator extends FilterValidator {

        @Override
        protected List<String> validate(String defaultStudyStr, List<String> values, Integer release, VariantQueryParam param,
                                        String sessionId)
                throws CatalogException {
            if (release == null) {
                // Query cohort by cohort if
                if (StringUtils.isEmpty(defaultStudyStr) || values.stream().anyMatch(value -> value.contains(":"))) {
                    List<String> validated = new ArrayList<>(values.size());
                    for (String value : values) {
                        String[] split = VariantQueryUtils.splitStudyResource(value);
                        String study = defaultStudyStr;
                        if (split.length == 2) {
                            study = split[0];
                            value = split[1];
                        }
                        Cohort cohort = catalogManager.getCohortManager().get(study, value, CohortManager.INCLUDE_COHORT_IDS, sessionId)
                                .first();
                        String fqn = catalogManager.getStudyManager().get(study,
                                new QueryOptions(INCLUDE, StudyDBAdaptor.QueryParams.FQN.key()), sessionId).first().getFqn();
                        if (fqn.equals(defaultStudyStr)) {
                            validated.add(cohort.getId());
                        } else {
                            validated.add(fqn + ":" + cohort.getId());
                        }
                    }
                    return validated;
                } else {
                    DataResult<Cohort> cohorts = catalogManager.getCohortManager().get(defaultStudyStr, values,
                            CohortManager.INCLUDE_COHORT_IDS, sessionId);
                    return cohorts.getResults().stream().map(Cohort::getId).collect(Collectors.toList());
                }
            } else {
                return validate(defaultStudyStr, values, release, param, catalogManager.getCohortManager(),
                        Cohort::getId, Cohort::getRelease, null, sessionId);
            }
        }
    }

}
