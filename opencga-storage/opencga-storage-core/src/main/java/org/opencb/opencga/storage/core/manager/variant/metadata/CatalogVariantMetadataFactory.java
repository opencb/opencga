package org.opencb.opencga.storage.core.manager.variant.metadata;

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
import org.opencb.opencga.storage.core.metadata.StudyConfiguration;
import org.opencb.opencga.storage.core.metadata.VariantMetadataFactory;
import org.opencb.opencga.storage.core.variant.adaptors.VariantDBAdaptor;

import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * Created on 22/08/17.
 *
 * @author Jacobo Coll &lt;jacobo167@gmail.com&gt;
 */
public final class CatalogVariantMetadataFactory extends VariantMetadataFactory {

    private final CatalogManager catalogManager;
    private final String sessionId;

    public CatalogVariantMetadataFactory(CatalogManager catalogManager, VariantDBAdaptor dbAdaptor, String sessionId) {
        super(dbAdaptor.getStudyConfigurationManager(), dbAdaptor.getVariantFileMetadataDBAdaptor());
        this.catalogManager = catalogManager;
        this.sessionId = sessionId;
    }

    @Override
    protected VariantMetadata makeVariantMetadata(List<StudyConfiguration> studyConfigurations,
                                                  Map<Integer, List<Integer>> returnedSamples,
                                                  Map<Integer, List<Integer>> returnedFiles) throws StorageEngineException {
        VariantMetadata metadata = super.makeVariantMetadata(studyConfigurations, returnedSamples, returnedFiles);

        Map<String, Integer> studyConfigurationMap = studyConfigurations.stream()
                .collect(Collectors.toMap(StudyConfiguration::getStudyName, StudyConfiguration::getStudyId));
        try {
            for (VariantStudyMetadata studyMetadata : metadata.getStudies()) {
                int studyId = studyConfigurationMap.get(studyMetadata.getId());

                fillStudy(studyId, studyMetadata);

                for (org.opencb.biodata.models.metadata.Individual individual : studyMetadata.getIndividuals()) {

                    fillIndividual(studyId, individual);

                    for (org.opencb.biodata.models.metadata.Sample sample : individual.getSamples()) {
                        fillSample(studyId, sample);
                    }
                }
            }
        } catch (CatalogException e) {
            throw new StorageEngineException("Error generating VariantMetadata", e);
        }
        return metadata;
    }

    private void fillStudy(long studyId, VariantStudyMetadata studyMetadata) throws CatalogException {
        QueryOptions options = new QueryOptions(QueryOptions.INCLUDE, SampleDBAdaptor.QueryParams.DESCRIPTION.key());

        // Just add file description
        Study study = catalogManager.getStudyManager().get(String.valueOf(studyId), options, sessionId).first();
        studyMetadata.setDescription(study.getDescription());
    }

    private void fillIndividual(Integer studyId, org.opencb.biodata.models.metadata.Individual individual) throws CatalogException {
        Query query = new Query(IndividualDBAdaptor.QueryParams.NAME.key(), individual.getId());

        Individual catalogIndividual = catalogManager.getIndividualManager().get(studyId, query, null, sessionId).first();
        if (catalogIndividual != null) {
            individual.setSex(catalogIndividual.getSex().name());
            individual.setFamily(catalogIndividual.getFamily());
            individual.setPhenotype(catalogIndividual.getAffectationStatus().toString());

            QueryOptions options = new QueryOptions(QueryOptions.INCLUDE, IndividualDBAdaptor.QueryParams.NAME.key());
            if (catalogIndividual.getMotherId() > 0) {
                String motherName = catalogManager.getIndividualManager().get(String.valueOf(studyId),
                        String.valueOf(catalogIndividual.getMotherId()), options, sessionId).first().getName();
                individual.setMother(motherName);
            }
            if (catalogIndividual.getFatherId() > 0) {
                String fatherName = catalogManager.getIndividualManager().get(String.valueOf(studyId),
                        String.valueOf(catalogIndividual.getFatherId()), options, sessionId).first().getName();
                individual.setFather(fatherName);
            }
        }
    }

    private void fillSample(int studyId, org.opencb.biodata.models.metadata.Sample sample) throws CatalogException {
        Query query = new Query(2)
                .append(SampleDBAdaptor.QueryParams.NAME.key(), sample.getId())
                .append(SampleDBAdaptor.QueryParams.STUDY_ID.key(), studyId);

        Sample catalogSample = catalogManager.getSampleManager().get(studyId, query, null, sessionId).first();
        List<AnnotationSet> annotationSets = catalogSample.getAnnotationSets();
        sample.setAnnotations(new LinkedHashMap<>(sample.getAnnotations()));
        for (AnnotationSet annotationSet : annotationSets) {
            String prefix = annotationSets.size() > 1 ? annotationSet.getName() + '.' : "";
            Map<String, Object> annotations = annotationSet.getAnnotations();
            for (Map.Entry<String, Object> annotation : annotations.entrySet()) {
                Object value = annotation.getValue();
                String stringValue;
                if (value instanceof Collection) {
                    stringValue = ((Collection<?>) value).stream().map(Object::toString).collect(Collectors.joining(","));
                } else {
                    stringValue = value.toString();
                }
                sample.getAnnotations().put(prefix + annotation.getKey(), stringValue);
            }
        }
    }
}
