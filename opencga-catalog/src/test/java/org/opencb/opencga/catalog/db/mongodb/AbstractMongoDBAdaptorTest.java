package org.opencb.opencga.catalog.db.mongodb;

import org.junit.After;
import org.junit.Before;
import org.opencb.commons.datastore.core.Query;
import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.opencga.catalog.db.api.*;
import org.opencb.opencga.catalog.exceptions.CatalogAuthorizationException;
import org.opencb.opencga.catalog.exceptions.CatalogDBException;
import org.opencb.opencga.catalog.exceptions.CatalogParameterException;
import org.opencb.opencga.catalog.managers.AbstractManagerTest;
import org.opencb.opencga.core.models.individual.Individual;
import org.opencb.opencga.core.models.job.Job;
import org.opencb.opencga.core.models.project.Project;
import org.opencb.opencga.core.models.sample.Sample;
import org.opencb.opencga.core.models.study.Study;

public class AbstractMongoDBAdaptorTest extends AbstractManagerTest {

    protected MongoDBAdaptorFactory dbAdaptorFactory;

    protected UserMongoDBAdaptor catalogUserDBAdaptor;
    protected ProjectMongoDBAdaptor catalogProjectDBAdaptor;
    protected FileMongoDBAdaptor catalogFileDBAdaptor;
    protected SampleMongoDBAdaptor catalogSampleDBAdaptor;
    protected JobMongoDBAdaptor catalogJobDBAdaptor;
    protected StudyMongoDBAdaptor catalogStudyDBAdaptor;
    protected IndividualMongoDBAdaptor catalogIndividualDBAdaptor;
    protected PanelMongoDBAdaptor catalogPanelDBAdaptor;

    @Before
    public void setUp() throws Exception {
        super.setUp();
        dbAdaptorFactory = new MongoDBAdaptorFactory(catalogManager.getConfiguration(), catalogManager.getIoManagerFactory());
        catalogUserDBAdaptor = (UserMongoDBAdaptor) dbAdaptorFactory.getCatalogUserDBAdaptor(organizationId);
        catalogStudyDBAdaptor = (StudyMongoDBAdaptor) dbAdaptorFactory.getCatalogStudyDBAdaptor(organizationId);
        catalogProjectDBAdaptor = (ProjectMongoDBAdaptor) dbAdaptorFactory.getCatalogProjectDbAdaptor(organizationId);
        catalogFileDBAdaptor = (FileMongoDBAdaptor) dbAdaptorFactory.getCatalogFileDBAdaptor(organizationId);
        catalogSampleDBAdaptor = (SampleMongoDBAdaptor) dbAdaptorFactory.getCatalogSampleDBAdaptor(organizationId);
        catalogJobDBAdaptor = (JobMongoDBAdaptor) dbAdaptorFactory.getCatalogJobDBAdaptor(organizationId);
        catalogIndividualDBAdaptor = (IndividualMongoDBAdaptor) dbAdaptorFactory.getCatalogIndividualDBAdaptor(organizationId);
        catalogPanelDBAdaptor = (PanelMongoDBAdaptor) dbAdaptorFactory.getCatalogPanelDBAdaptor(organizationId);
    }

    @After
    public void after() {
        dbAdaptorFactory.close();
    }

    protected Sample getSample(long studyUid, String sampleId)
            throws CatalogDBException, CatalogParameterException, CatalogAuthorizationException {
        return getSample(studyUid, sampleId, QueryOptions.empty());
    }

    protected Sample getSample(long studyUid, String sampleId, QueryOptions options)
            throws CatalogDBException, CatalogParameterException, CatalogAuthorizationException {
        Query query = new Query()
                .append(SampleDBAdaptor.QueryParams.STUDY_UID.key(), studyUid)
                .append(SampleDBAdaptor.QueryParams.ID.key(), sampleId);
        return catalogSampleDBAdaptor.get(query, options).first();
    }

    protected Individual getIndividual(long studyUid, String individualId)
            throws CatalogDBException, CatalogParameterException, CatalogAuthorizationException {
        Query query = new Query()
                .append(IndividualDBAdaptor.QueryParams.STUDY_UID.key(), studyUid)
                .append(IndividualDBAdaptor.QueryParams.ID.key(), individualId);
        return catalogIndividualDBAdaptor.get(query, QueryOptions.empty()).first();
    }

    protected Job getJob(long studyUid, String jobId) throws CatalogDBException, CatalogParameterException, CatalogAuthorizationException {
        Query query = new Query()
                .append(JobDBAdaptor.QueryParams.STUDY_UID.key(), studyUid)
                .append(JobDBAdaptor.QueryParams.ID.key(), jobId);
        return catalogJobDBAdaptor.get(query, QueryOptions.empty()).first();
    }

    Project getProject(String projectId) throws CatalogDBException {
        Query query = new Query()
                .append(ProjectDBAdaptor.QueryParams.ID.key(), projectId);
        return catalogProjectDBAdaptor.get(query, QueryOptions.empty()).first();
    }

    Study getStudy(long projectUid, String studyId) throws CatalogDBException {
        Query query = new Query()
                .append(StudyDBAdaptor.QueryParams.PROJECT_UID.key(), projectUid)
                .append(StudyDBAdaptor.QueryParams.ID.key(), studyId);
        return catalogStudyDBAdaptor.get(query, QueryOptions.empty()).first();
    }
}
