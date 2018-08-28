package org.opencb.opencga.storage.core.manager.variant.metadata;

import org.opencb.biodata.models.metadata.Cohort;
import org.opencb.biodata.models.variant.metadata.VariantMetadata;
import org.opencb.biodata.models.variant.metadata.VariantStudyMetadata;
import org.opencb.commons.datastore.core.Query;
import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.opencga.catalog.db.api.IndividualDBAdaptor;
import org.opencb.opencga.catalog.db.api.SampleDBAdaptor;
import org.opencb.opencga.catalog.exceptions.CatalogException;
import org.opencb.opencga.catalog.managers.CatalogManager;
import org.opencb.opencga.core.models.AnnotationSet;
import org.opencb.opencga.core.models.Individual;
import org.opencb.opencga.core.models.Sample;
import org.opencb.opencga.core.models.Study;
import org.opencb.opencga.storage.core.exceptions.StorageEngineException;
import org.opencb.opencga.storage.core.metadata.ProjectMetadata;
import org.opencb.opencga.storage.core.metadata.StudyConfiguration;
import org.opencb.opencga.storage.core.metadata.VariantMetadataFactory;
import org.opencb.opencga.storage.core.variant.adaptors.VariantDBAdaptor;

import java.util.*;
import java.util.stream.Collectors;

/**
 * Created on 22/08/17.
 *
 * @author Jacobo Coll &lt;jacobo167@gmail.com&gt;
 */
public final class CatalogVariantMetadataFactory extends VariantMetadataFactory {

    private static final QueryOptions SAMPLE_QUERY_OPTIONS = new QueryOptions(QueryOptions.INCLUDE,
            Arrays.asList(
                    SampleDBAdaptor.QueryParams.UID.key(),
                    SampleDBAdaptor.QueryParams.ID.key(),
                    SampleDBAdaptor.QueryParams.DESCRIPTION.key(),
                    SampleDBAdaptor.QueryParams.ANNOTATION_SETS.key()
            ));
    private static final QueryOptions INDIVIDUAL_QUERY_OPTIONS = new QueryOptions(QueryOptions.INCLUDE,
            Arrays.asList(
                    IndividualDBAdaptor.QueryParams.UID.key(),
                    IndividualDBAdaptor.QueryParams.ID.key(),
                    IndividualDBAdaptor.QueryParams.SEX.key(),
                    IndividualDBAdaptor.QueryParams.AFFECTATION_STATUS.key(),
                    IndividualDBAdaptor.QueryParams.MOTHER.key(),
                    IndividualDBAdaptor.QueryParams.FATHER.key()
            ));
    public static final int CATALOG_QUERY_BATCH_SIZE = 1000;
    public static final String BASIC_METADATA = "basic";
    private final CatalogManager catalogManager;
    private final String sessionId;

    public CatalogVariantMetadataFactory(CatalogManager catalogManager, VariantDBAdaptor dbAdaptor, String sessionId) {
        super(dbAdaptor.getStudyConfigurationManager());
        this.catalogManager = catalogManager;
        this.sessionId = sessionId;
    }

    @Override
    protected VariantMetadata makeVariantMetadata(List<StudyConfiguration> studyConfigurations,
                                                  ProjectMetadata projectMetadata, Map<Integer, List<Integer>> returnedSamples,
                                                  Map<Integer, List<Integer>> returnedFiles,
                                                  QueryOptions queryOptions) throws StorageEngineException {
        VariantMetadata metadata = super.makeVariantMetadata(studyConfigurations, projectMetadata,
                returnedSamples, returnedFiles, queryOptions);
        if (queryOptions != null) {
            if (queryOptions.getBoolean(BASIC_METADATA, false)) {
                // If request BasicMetadata, do not return extra catalog information, neither samples in cohorts
                for (VariantStudyMetadata variantStudyMetadata : metadata.getStudies()) {
                    for (Cohort cohort : variantStudyMetadata.getCohorts()) {
                        cohort.setSampleIds(Collections.emptyList());
                    }
                }
                return metadata;
            }
        }
        Map<String, Integer> studyConfigurationMap = studyConfigurations.stream()
                .collect(Collectors.toMap(StudyConfiguration::getStudyName, StudyConfiguration::getStudyId));
        try {
            for (VariantStudyMetadata studyMetadata : metadata.getStudies()) {
                String studyId = studyMetadata.getId();
                fillStudy(studyId, studyMetadata);

                List<org.opencb.biodata.models.metadata.Individual> individuals = new ArrayList<>(CATALOG_QUERY_BATCH_SIZE);
                List<org.opencb.biodata.models.metadata.Sample> samples = new ArrayList<>(CATALOG_QUERY_BATCH_SIZE);
                Iterator<org.opencb.biodata.models.metadata.Individual> iterator = studyMetadata.getIndividuals().iterator();
                while (iterator.hasNext()) {
                    org.opencb.biodata.models.metadata.Individual individual = iterator.next();
                    individuals.add(individual);
                    samples.addAll(individual.getSamples());
                    if (individuals.size() >= CATALOG_QUERY_BATCH_SIZE || !iterator.hasNext()) {
                        fillIndividuals(studyId, individuals);
                        individuals.clear();
                    }
                    if (samples.size() >= CATALOG_QUERY_BATCH_SIZE || !iterator.hasNext()) {
                        fillSamples(studyId, samples);
                        samples.clear();
                    }
                }
            }
        } catch (CatalogException e) {
            throw new StorageEngineException("Error generating VariantMetadata", e);
        }
        return metadata;
    }

    private void fillStudy(String studyId, VariantStudyMetadata studyMetadata) throws CatalogException {
        QueryOptions options = new QueryOptions(QueryOptions.INCLUDE, SampleDBAdaptor.QueryParams.DESCRIPTION.key());

        // Just add file description
        Study study = catalogManager.getStudyManager().get(studyId, options, sessionId).first();
        studyMetadata.setDescription(study.getDescription());
    }

    private void fillIndividuals(String studyId, List<org.opencb.biodata.models.metadata.Individual> individuals) throws CatalogException {
        Map<String, org.opencb.biodata.models.metadata.Individual> individualMap = individuals
                .stream()
                .collect(Collectors.toMap(org.opencb.biodata.models.metadata.Individual::getId, i -> i));
        Query query = new Query(IndividualDBAdaptor.QueryParams.ID.key(), new ArrayList<>(individualMap.keySet()));

        List<Individual> catalogIndividuals = catalogManager.getIndividualManager().get(studyId, query, INDIVIDUAL_QUERY_OPTIONS, sessionId)
                .getResult();

        for (Individual catalogIndividual : catalogIndividuals) {
            org.opencb.biodata.models.metadata.Individual individual = individualMap.get(catalogIndividual.getName());

            individual.setSex(catalogIndividual.getSex().name());
//            individual.setFamily(catalogIndividual.getFamily());
            individual.setPhenotype(catalogIndividual.getAffectationStatus().toString());

            if (catalogIndividual.getMother() != null) {
                individual.setMother(catalogIndividual.getMother().getId());
            }
            if (catalogIndividual.getFather() != null) {
                individual.setFather(catalogIndividual.getFather().getId());
            }
        }
    }

    private void fillSamples(String studyId, List<org.opencb.biodata.models.metadata.Sample> samples) throws CatalogException {
        Map<String, org.opencb.biodata.models.metadata.Sample> samplesMap = samples
                .stream()
                .collect(Collectors.toMap(org.opencb.biodata.models.metadata.Sample::getId, i -> i));
        Query query = new Query(SampleDBAdaptor.QueryParams.ID.key(), new ArrayList<>(samplesMap.keySet()));

        List<Sample> catalogSamples = catalogManager.getSampleManager().get(studyId, query, SAMPLE_QUERY_OPTIONS,
                sessionId).getResult();
        for (Sample catalogSample : catalogSamples) {
            org.opencb.biodata.models.metadata.Sample sample = samplesMap.get(catalogSample.getId());

            List<AnnotationSet> annotationSets = catalogSample.getAnnotationSets();
            if (annotationSets != null) {
                sample.setAnnotations(new LinkedHashMap<>(sample.getAnnotations()));
                for (AnnotationSet annotationSet : annotationSets) {
                    String prefix = annotationSets.size() > 1 ? annotationSet.getName() + '.' : "";
                    Map<String, Object> annotations = annotationSet.getAnnotations();
                    for (Map.Entry<String, Object> annotationEntry : annotations.entrySet()) {
                        Object value = annotationEntry.getValue();
                        String stringValue;
                        if (value instanceof Collection) {
                            stringValue = ((Collection<?>) value).stream().map(Object::toString).collect(Collectors.joining(","));
                        } else {
                            stringValue = value.toString();
                        }
                        sample.getAnnotations().put(prefix + annotationEntry.getKey(), stringValue);
                    }
                }
            }
        }
    }
}
