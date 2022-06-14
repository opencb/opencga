package org.opencb.opencga.catalog.utils;

import org.junit.Test;
import org.opencb.biodata.models.pedigree.IndividualProperty;
import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.opencga.catalog.exceptions.CatalogException;
import org.opencb.opencga.catalog.managers.AbstractManagerTest;
import org.opencb.opencga.core.models.individual.Individual;
import org.opencb.opencga.core.models.individual.IndividualPopulation;
import org.opencb.opencga.core.models.sample.Sample;
import org.opencb.opencga.core.models.sample.SampleProcessing;

import java.util.Collections;

import static org.junit.Assert.assertEquals;

public class CataletTest extends AbstractManagerTest {

    @Test
    public void testFetch() throws CatalogException {
        Sample sample = new Sample()
                .setId("mySample")
                .setProcessing(new SampleProcessing().setLabSampleId("myLabId"));
        Individual individual = new Individual()
                .setId("myIndividual")
                .setLifeStatus(IndividualProperty.LifeStatus.ALIVE)
                .setPopulation(new IndividualPopulation("name", "subpopulation", "My description"));

        catalogManager.getSampleManager().create(studyFqn, sample, QueryOptions.empty(), token);
        catalogManager.getIndividualManager().create(studyFqn, individual, Collections.singletonList(sample.getId()), QueryOptions.empty(), token);

        Catalet catalet = new Catalet(catalogManager, studyFqn, token);
        assertEquals(sample.getProcessing().getLabSampleId(), catalet.fetch("FETCH_FIELD(sample, id, mySample, processing.labSampleId)"));
        assertEquals(individual.getId(), catalet.fetch("FETCH_FIELD(sample, id, mySample, individualId)"));
        assertEquals(individual.getLifeStatus().name(), catalet.fetch("FETCH_FIELD(individual, id, myIndividual, lifeStatus)"));
        assertEquals(individual.getPopulation().getSubpopulation(), catalet.fetch("FETCH_FIELD(individual, id, myIndividual, population.subpopulation)"));
        assertEquals(individual.getPopulation().getDescription(), catalet.fetch("FETCH_FIELD(individual, id, myIndividual, population.description)"));

        // TODO: Think about this possibility. samples[].id returning an array of strings directly
//        assertEquals(Collections.singletonList(sample.getId()), catalet.fetch("FETCH_FIELD(individual, id, myIndividual, samples.id)"));
    }
}