package org.opencb.opencga.server.rest.ga4gh;

import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.opencb.biodata.models.commons.Disorder;
import org.opencb.biodata.models.core.Region;
import org.opencb.biodata.models.variant.Variant;
import org.opencb.biodata.models.variant.avro.ConsequenceType;
import org.opencb.biodata.models.variant.avro.EvidenceEntry;
import org.opencb.biodata.models.variant.avro.VariantAnnotation;
import org.opencb.commons.datastore.core.Query;
import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.opencga.analysis.variant.manager.VariantStorageManager;
import org.opencb.opencga.catalog.db.api.StudyDBAdaptor;
import org.opencb.opencga.catalog.exceptions.CatalogException;
import org.opencb.opencga.catalog.managers.CatalogManager;
import org.opencb.opencga.core.models.individual.Individual;
import org.opencb.opencga.core.models.project.Project;
import org.opencb.opencga.core.models.study.Study;
import org.opencb.opencga.core.response.OpenCGAResult;
import org.opencb.opencga.core.response.VariantQueryResult;
import org.opencb.opencga.server.rest.ga4gh.models.*;
import org.opencb.opencga.storage.core.variant.adaptors.VariantField;
import org.opencb.opencga.storage.core.variant.adaptors.VariantQueryParam;

import java.util.Collections;
import java.util.LinkedList;
import java.util.stream.Collectors;

public class Ga4ghBeaconManager {

    private final CatalogManager catalogManager;
    private final VariantStorageManager variantManager;

    public Ga4ghBeaconManager(CatalogManager catalogManager, VariantStorageManager variantManager) {
        this.catalogManager = catalogManager;
        this.variantManager = variantManager;
    }

    public Ga4ghBeacon getBeacon(String token) {
        Ga4ghBeacon beacon = new Ga4ghBeacon();
        beacon.id("org.opencb.opencga")
                .name("OpenCGA")
                .apiVersion("1")
                .organization(new Ga4ghBeaconOrganization()
                        .id("opencb")
                        .name("OpenCB")
                        .address("Cambridge")
                )
                .description("")
                .version("1");

        try {
            for (Project project : catalogManager.getProjectManager().get(new Query(), new QueryOptions(), token).getResults()) {
                for (Study study : project.getStudies()) {
                    beacon.addDatasetsItem(new Ga4ghBeaconDataset()
                            .id(study.getFqn())
                            .name(study.getName())
                            .updateDateTime(study.getModificationDate())
                            .sampleCount(catalogManager.getSampleManager().count(study.getFqn(), new Query(), token).getNumMatches())
                            .assemblyId(project.getOrganism().getAssembly())
                            .createDateTime(study.getCreationDate())
                            .description(study.getDescription())
                    );
                }
            }
        } catch (CatalogException e) {
            e.printStackTrace();
        }
        return beacon;
    }


    public void variant(Ga4ghGenomicVariantResponseValue value, String token) throws Exception {
        Ga4ghRequestDatasets datasets = value.getRequest().getQuery().getDatasets();

        String assembly = null;
        if (datasets.getDatasetIds() != null && !datasets.getDatasetIds().isEmpty()) {
            String datasetId = datasets.getDatasetIds().get(0);
            String studyFqn = catalogManager.getStudyManager().get(datasetId,
                    new QueryOptions(QueryOptions.INCLUDE, StudyDBAdaptor.QueryParams.FQN.key()), token).first().getFqn();
            String projectFqn = catalogManager.getStudyManager().getProjectFqn(studyFqn);
            Project project = catalogManager.getProjectManager().get(projectFqn,
                    new QueryOptions(QueryOptions.EXCLUDE, "studies"), token).first();
            assembly = project.getOrganism().getAssembly();
        }

        Query query = new Query();
        query.put(VariantQueryParam.STUDY.key(), datasets.getDatasetIds());
        for (String filter : value.getRequest().getQuery().getCustomFilters()) {
            String[] split = filter.split("=", 2);
            if (split.length != 2) {
                throw new IllegalArgumentException("Malformed filter '" + filter + "', expected key=value");
            }
            query.put(split[0], split[1]);
        }

        Ga4ghGenomicVariantFields variantQuery = value.getRequest().getQuery().getVariant();
        if (variantQuery != null) {
            if (StringUtils.isNotEmpty(variantQuery.getVariantType())) {
                query.put(VariantQueryParam.TYPE.key(), variantQuery.getVariantType());
            }
            if (variantQuery.getReferenceName() != null) {
                if (CollectionUtils.isNotEmpty(variantQuery.getStart())) {
                    // From 0-based to 1-based
                    long start = variantQuery.getStart().get(0) + 1;
                    if (CollectionUtils.isNotEmpty(variantQuery.getEnd())) {
                        Long end = variantQuery.getStart().get(0);
                        query.append(VariantQueryParam.REGION.key(), new Region(variantQuery.getReferenceName(), (int) start, end.intValue()));
                    } else {
                        query.append(VariantQueryParam.REGION.key(), new Region(variantQuery.getReferenceName(), (int) start));
                    }
                } else {
                    query.append(VariantQueryParam.REGION.key(), new Region(variantQuery.getReferenceName()));
                }
            }
        }


        query.append(VariantQueryParam.SAMPLE_METADATA.key(), true);
//            logger.info("query.toJson() = " + query.toJson());
        VariantQueryResult<Variant> result = variantManager.get(query, new QueryOptions()
                .append(QueryOptions.EXCLUDE, Collections.singletonList(VariantField.STUDIES_SAMPLES_DATA))
                .append(QueryOptions.LIMIT, 10), token);
        for (Variant variant : result.getResults()) {
            Ga4ghVariantsFoundResponse ga4ghVariantsFoundResponse = new Ga4ghVariantsFoundResponse();
            value.addResultsItem(ga4ghVariantsFoundResponse);

            ga4ghVariantsFoundResponse.setVariant(new Ga4ghVariant()._default(new Ga4ghVariantDefault().version("1").value(
                    new Ga4ghVariant2()
                            .variantDetails(new Ga4ghVariantDetails()
                                    .chromosome(Ga4ghChromosome2.fromValue(variant.getChromosome()))
                                    .start(((long) (variant.getStart() - 1)))
                                    .end(((long) (variant.getEnd())))
                                    .referenceBases(variant.getReference())
                                    .alternateBases(variant.getAlternate())
                                    .assemblyId(assembly)
                                    .variantType(variant.getType().toString())
                            )
                    ))
            );
            Ga4ghVariantAnnotations ga4ghAnnotation = new Ga4ghVariantAnnotations();
            VariantAnnotation annotation = variant.getAnnotation();
            ga4ghAnnotation.setProteinHGVSIds(annotation.getHgvs());
            LinkedList<String> alternativeIds = new LinkedList<>();
            if (annotation.getId() != null) {
                alternativeIds.add(annotation.getId());
            }
            if (annotation.getTraitAssociation() != null) {
                alternativeIds.addAll(annotation.getTraitAssociation().stream().map(EvidenceEntry::getId).collect(Collectors.toSet()));
                for (EvidenceEntry evidenceEntry : annotation.getTraitAssociation()) {
                    ga4ghAnnotation.addClinicalRelevanceItem(new Ga4ghClinicalRelevance()
                            .diseaseId(evidenceEntry.getId())
                            .variantClassification(evidenceEntry.getVariantClassification() == null || evidenceEntry.getVariantClassification().getClinicalSignificance() == null
                                    ? null
                                    : evidenceEntry.getVariantClassification().getClinicalSignificance().toString())
                            .references(evidenceEntry.getBibliography())
                    );
                }
            }

            ga4ghAnnotation.setAlternativeIds(alternativeIds);
            if (annotation.getConsequenceTypes() != null) {
                annotation.getConsequenceTypes()
                        .stream()
                        .map(ConsequenceType::getGeneName)
                        .filter(StringUtils::isNotEmpty)
                        .collect(Collectors.toSet())
                        .forEach(ga4ghAnnotation::addGeneIdsItem);
                annotation.getConsequenceTypes()
                        .stream()
                        .map(ConsequenceType::getEnsemblTranscriptId)
                        .filter(StringUtils::isNotEmpty)
                        .collect(Collectors.toSet())
                        .forEach(ga4ghAnnotation::addTranscriptIdsItem);
//                    annotation.getConsequenceTypes()
//                            .stream()
//                            .flatMap(c->c.getSequenceOntologyTerms().stream())
//                            .map(SequenceOntologyTerm::getAccession)
//                            .filter(Objects::nonNull)
//                            .collect(Collectors.toSet())
//                            .forEach(ga4ghAnnotation::addMolecularConsequence);
                ga4ghAnnotation.setMolecularConsequence(annotation.getDisplayConsequenceType());
            }

            for (String study : result.getSamples().keySet()) {
                ga4ghVariantsFoundResponse.addDatasetAlleleResponesItem(new Ga4ghBeaconDatasetAlleleResponse()
                        .datasetId(study)
                        .exists(variant.getStudy(study) != null));
            }

            ga4ghVariantsFoundResponse.setVariantAnnotations(new Ga4ghVariantAnnotation()
                    ._default(new Ga4ghVariantAnnotationDefault().version("1").value(ga4ghAnnotation)));
//                ga4ghVariantsFoundResponse.getVariantAnnotations().addAlternativeSchemasItem(
//                        new Ga4ghResponseBasicStructure().value(variant.getAnnotation()));

        }
    }



    public void individual(Ga4ghIndividualResponseValue value, String token) throws CatalogException {
        Ga4ghRequestDatasets datasets = value.getRequest().getQuery().getDatasets();
        for (String datasetId : datasets.getDatasetIds()) {
            Query query = new Query();
            for (String filter : value.getRequest().getQuery().getCustomFilters()) {
                String[] split = filter.split("=", 2);
                query.put(split[0], split[1]);
            }

            OpenCGAResult<Individual> openCGAResult = catalogManager.getIndividualManager().search(datasetId, query, new QueryOptions(), token);
            for (Individual individual : openCGAResult.getResults()) {
                Ga4ghResponseResult ga4ghResponse = new Ga4ghResponseResult();
                value.addResultsItem(new Ga4ghIndividualResponseResults().individual(ga4ghResponse));

                // Add default
                Ga4ghIndividual ga4ghIndividual = convertIndividual(individual);
                ga4ghResponse.setDefault(new Ga4ghResponseBasicStructure().version("1").value(ga4ghIndividual));

                // Add catalog model as alternative
                ga4ghResponse.addAlternativeSchemasItem(new Ga4ghResponseBasicStructure().version("1").value(individual));
            }
        }
    }

    private Ga4ghIndividual convertIndividual(Individual individual) {
        Ga4ghIndividual ga4ghIndividual = new Ga4ghIndividual();
        ga4ghIndividual.setIndividualId(individual.getId());
        ga4ghIndividual.setEthnicity(individual.getEthnicity());
        if (individual.getDisorders() != null) {
            for (Disorder disorder : individual.getDisorders()) {
                ga4ghIndividual.addDiseasesItem(new Ga4ghDisease()
                        .diseaseId(disorder.getId())
                );
            }
        }
        if (individual.getSex() != null) {
            //Categorical value from NCIT General Qualifier ontology (NCIT:C27993):
            // UNKNOWN (not assessed or not available) (NCIT:C17998),
            // FEMALE (NCIT:C46113),
            // MALE (NCIT:C46112)
            // OTHER SEX (NCIT:C45908)
            String ga4ghSex;
            switch (individual.getSex()) {
                case MALE:
                    ga4ghSex = "NCIT:C46112";
                    break;
                case FEMALE:
                    ga4ghSex = "NCIT:C46113";
                    break;
                case UNKNOWN:
                    ga4ghSex = "NCIT:C17998";
                    break;
                case UNDETERMINED:
                    ga4ghSex = "NCIT:C45908";
                    break;
                default:
                    throw new IllegalArgumentException("Unknown sex " + individual.getSex());
            }
            ga4ghIndividual.setSex(ga4ghSex);
        }
        return ga4ghIndividual;
    }

}
