package org.opencb.opencga.catalog.db.mongodb;

import org.junit.Test;
import org.opencb.commons.datastore.core.Query;
import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.commons.datastore.core.QueryResult;
import org.opencb.opencga.catalog.db.api.JobDBAdaptor;
import org.opencb.opencga.catalog.exceptions.CatalogDBException;
import org.opencb.opencga.catalog.exceptions.CatalogException;
import org.opencb.opencga.catalog.models.Job;
import org.opencb.opencga.core.common.TimeUtils;

import java.util.Calendar;
import java.util.Collections;
import java.util.Date;

import static org.junit.Assert.*;

/**
 * Created by pfurio on 3/2/16.
 */
public class CatalogMongoJobDBAdaptorTest extends CatalogMongoDBAdaptorTest {

    @Test
    public void createJobTest() throws CatalogDBException {
        Job job = new Job();
        job.setVisits(0);

        long studyId = user3.getProjects().get(0).getStudies().get(0).getId();
        job.setName("jobName1");
        System.out.println(catalogJobDBAdaptor.insert(job, studyId, null));
//        long analysisId = catalogDBAdaptor.getAnalysisId(studyId, "analysis1Alias");

        job.setName("jobName2");
        System.out.println(catalogJobDBAdaptor.insert(job, studyId, null));
        try {
            catalogJobDBAdaptor.insert(job, -1, null);
            fail("error: expected exception");
        } catch (CatalogDBException e) {
            System.out.println("correct exception: " + e);
        }
    }

    @Test
    public void deleteJobTest() throws CatalogException {
        long studyId = user3.getProjects().get(0).getStudies().get(0).getId();

        Job job = catalogJobDBAdaptor.insert(new Job("name", user3.getId(), "", "", "", 4, Collections.<Long>emptyList
                ()), studyId, null).first();
        long jobId = job.getId();
        assertEquals(Job.JobStatus.PREPARED, job.getStatus().getName());
        thrown.expect(CatalogDBException.class);
        thrown.expectMessage("Please, stop the job before");
        catalogJobDBAdaptor.delete(jobId, new QueryOptions());
    }

    @Test
    public void deleteJobTest2() throws CatalogException {
        long studyId = user3.getProjects().get(0).getStudies().get(0).getId();

        Job job = catalogJobDBAdaptor.insert(new Job("name", user3.getId(), "", "", "", 4, Collections.<Long>emptyList
                ()), studyId, null).first();
        long jobId = job.getId();
        assertEquals(Job.JobStatus.PREPARED, job.getStatus().getName());
        catalogJobDBAdaptor.setStatus(jobId, Job.JobStatus.READY);
        QueryResult<Job> queryResult = catalogJobDBAdaptor.delete(jobId, new QueryOptions());
        System.out.println(queryResult);
        assertTrue(queryResult.getNumResults() == 1);
        assertEquals(Job.JobStatus.TRASHED, queryResult.first().getStatus().getName());
        try {
            System.out.println(catalogJobDBAdaptor.delete(-1, new QueryOptions()));
            fail("error: Expected \"Job not found\" exception");
        } catch (CatalogDBException e) {
            System.out.println("correct exception: " + e);
        }
    }

    @Test
    public void getAllJobTest() throws CatalogDBException {
        long studyId = user3.getProjects().get(0).getStudies().get(0).getId();
//        long analysisId = catalogDBAdaptor.getAnalysisId(studyId, "analysis1Alias");
        QueryResult<Job> allJobs = catalogJobDBAdaptor.getAllInStudy(studyId, null);
        System.out.println(allJobs);
    }


    @Test
    public void getJobTest() throws CatalogException {
        long studyId = user3.getProjects().get(0).getStudies().get(0).getId();

        Job job = catalogJobDBAdaptor.insert(new Job("name", user3.getId(), "", "", "", 4, Collections.<Long>emptyList
                ()), studyId, null).first();
        long jobId = job.getId();

        job = catalogJobDBAdaptor.get(jobId, null).first();
        System.out.println(job);

        try {
            catalogJobDBAdaptor.get(-1, null);
            fail("error: expected exception");
        } catch (CatalogDBException e) {
            System.out.println("correct exception: " + e);
        }

    }

    @Test
    public void incJobVisits() throws CatalogException {
        long studyId = user3.getProjects().get(0).getStudies().get(0).getId();
        Job jobBefore = catalogJobDBAdaptor.insert(new Job("name", user3.getId(), "", "", "", 4, Collections
                .<Long>emptyList()), studyId, null).first();
        long jobId = jobBefore.getId();

        Long visits = (Long) catalogJobDBAdaptor.incJobVisits(jobBefore.getId()).first().get("visits");

        Job jobAfter = catalogJobDBAdaptor.get(jobId, null).first();
        assertTrue(jobBefore.getVisits() == jobAfter.getVisits() - 1);
        assertTrue(visits == jobAfter.getVisits());
    }

    @Test
    public void getJobsOrderedByDate() throws CatalogDBException {
        long studyId = user3.getProjects().get(0).getStudies().get(0).getId();

        // Job with current date
        Job job1 = new Job();
        job1.setName("job1");
        job1.setCreationDate(TimeUtils.getTime());

        // Job with current date one hour before
        Calendar cal = Calendar.getInstance();
        cal.setTime(new Date());
        cal.add(Calendar.HOUR, -1);
        Date oneHourBack = cal.getTime();

        Job job2 = new Job();
        job2.setName("job2");
        job2.setCreationDate(TimeUtils.getTime(oneHourBack));

        // We create the jobs
        catalogJobDBAdaptor.insert(job1, studyId, new QueryOptions());
        catalogJobDBAdaptor.insert(job2, studyId, new QueryOptions());

        // Obtain the jobs in descending order
        QueryOptions queryOptions = new QueryOptions()
                .append(QueryOptions.SORT, JobDBAdaptor.QueryParams.CREATION_DATE.key())
                .append(QueryOptions.ORDER, QueryOptions.DESCENDING);


        QueryResult<Job> jobQueryResult1 = catalogJobDBAdaptor.get(new Query(), queryOptions);

        assertTrue("job1".equals(jobQueryResult1.getResult().get(0).getName()));
        assertTrue("job2".equals(jobQueryResult1.getResult().get(1).getName()));

        // Obtain the jobs in ascending order
        queryOptions = new QueryOptions()
                .append(QueryOptions.SORT, JobDBAdaptor.QueryParams.CREATION_DATE.key())
                .append(QueryOptions.ORDER, QueryOptions.ASCENDING);

        QueryResult<Job> jobQueryResult2 = catalogJobDBAdaptor.get(new Query(), queryOptions);

        assertTrue("job2".equals(jobQueryResult2.getResult().get(0).getName()));
        assertTrue("job1".equals(jobQueryResult2.getResult().get(1).getName()));
    }

}
