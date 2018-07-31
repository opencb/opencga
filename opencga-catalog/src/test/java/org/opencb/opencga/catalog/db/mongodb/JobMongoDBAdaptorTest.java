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

package org.opencb.opencga.catalog.db.mongodb;

import org.junit.Test;
import org.opencb.commons.datastore.core.ObjectMap;
import org.opencb.commons.datastore.core.Query;
import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.commons.datastore.core.QueryResult;
import org.opencb.opencga.catalog.db.api.JobDBAdaptor;
import org.opencb.opencga.catalog.exceptions.CatalogDBException;
import org.opencb.opencga.catalog.exceptions.CatalogException;
import org.opencb.opencga.core.models.File;
import org.opencb.opencga.core.models.Job;
import org.opencb.opencga.core.common.TimeUtils;

import java.util.*;
import java.util.stream.Collectors;

import static org.junit.Assert.*;

/**
 * Created by pfurio on 3/2/16.
 */
public class JobMongoDBAdaptorTest extends MongoDBAdaptorTest {

    @Test
    public void createJobTest() throws CatalogDBException {
        Job job = new Job();

        long studyId = user3.getProjects().get(0).getStudies().get(0).getUid();
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
        long studyId = user3.getProjects().get(0).getStudies().get(0).getUid();

        Job job = catalogJobDBAdaptor.insert(new Job("name", user3.getId(), "", "", "", new File().setUid(4), Collections.emptyList(), 1),
                studyId, null).first();
        long jobId = job.getUid();
        assertEquals(Job.JobStatus.PREPARED, job.getStatus().getName());
        catalogJobDBAdaptor.delete(jobId);

        thrown.expect(CatalogDBException.class);
        thrown.expectMessage("not exist");
        catalogJobDBAdaptor.get(jobId, QueryOptions.empty());
    }

    @Test
    public void getAllJobTest() throws CatalogDBException {
        long studyId = user3.getProjects().get(0).getStudies().get(0).getUid();
        QueryResult<Job> allJobs = catalogJobDBAdaptor.getAllInStudy(studyId, null);
        System.out.println(allJobs);
    }


    @Test
    public void getJobTest() throws CatalogException {
        long studyId = user3.getProjects().get(0).getStudies().get(0).getUid();

        Job job = catalogJobDBAdaptor.insert(new Job("name", user3.getId(), "", "", "", new File().setUid(4), Collections.emptyList(), 1),
                studyId, null).first();
        long jobId = job.getUid();

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
    public void SetVisitedJob() throws CatalogException {
        long studyId = user3.getProjects().get(0).getStudies().get(0).getUid();
        Job jobBefore = catalogJobDBAdaptor.insert(new Job("name", user3.getId(), "", "", "", new File().setUid(4),
                        Collections.emptyList(), 1), studyId, null).first();
        long jobId = jobBefore.getUid();
        assertTrue(!jobBefore.isVisited());

        ObjectMap params = new ObjectMap(JobDBAdaptor.QueryParams.VISITED.key(), true);
        catalogJobDBAdaptor.update(jobBefore.getUid(), params, QueryOptions.empty());

        Job jobAfter = catalogJobDBAdaptor.get(jobId, null).first();
        assertTrue(jobAfter.isVisited());
    }

    @Test
    public void getJobsOrderedByDate() throws CatalogDBException {
        long studyId = user3.getProjects().get(0).getStudies().get(0).getUid();

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

    @Test
    public void updateInputAndOutputFiles() throws Exception {
        Job job = new Job();
        job.setOutDir(new File().setUid(5));
        long studyId = user3.getProjects().get(0).getStudies().get(0).getUid();
        job.setName("jobName1");
        QueryResult<Job> insert = catalogJobDBAdaptor.insert(job, studyId, null);

        List<File> fileInput = Arrays.asList(
                new File().setUid(5L).setName("file1"), new File().setUid(6L).setName("file2"), new File().setUid(7L).setName("file3")
        );
        List<File> fileOutput = Arrays.asList(
                new File().setUid(15L).setName("file1"), new File().setUid(16L).setName("file2"), new File().setUid(17L).setName("file3")
        );
        ObjectMap params = new ObjectMap()
                .append(JobDBAdaptor.QueryParams.INPUT.key(), fileInput)
                .append(JobDBAdaptor.QueryParams.OUTPUT.key(), fileOutput);

        QueryResult<Job> update = catalogJobDBAdaptor.update(insert.first().getUid(), params, QueryOptions.empty());
        assertEquals(3, update.first().getInput().size());
        assertEquals(3, update.first().getOutput().size());

        assertTrue(Arrays.asList(5L, 6L, 7L).containsAll(update.first().getInput().stream().map(File::getUid).collect(Collectors.toList())));
        assertTrue(Arrays.asList(15L, 16L, 17L)
                .containsAll(update.first().getOutput().stream().map(File::getUid).collect(Collectors.toList())));
    }

}
