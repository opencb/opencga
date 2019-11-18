package org.opencb.opencga.catalog.db.mongodb.iterators;

import com.mongodb.client.MongoCursor;
import org.bson.Document;
import org.opencb.commons.datastore.core.Query;
import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.opencga.catalog.db.api.FileDBAdaptor;
import org.opencb.opencga.catalog.db.api.JobDBAdaptor;
import org.opencb.opencga.catalog.db.api.SampleDBAdaptor;
import org.opencb.opencga.catalog.db.mongodb.converters.JobConverter;
import org.opencb.opencga.catalog.exceptions.CatalogAuthorizationException;
import org.opencb.opencga.catalog.exceptions.CatalogDBException;
import org.opencb.opencga.core.models.Job;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.stream.Collectors;

public class JobMongoDBIterator extends BatchedMongoDBIterator<Job> {

    private final FileDBAdaptor fileDBAdaptor;
    private final boolean nativeQuery;
    private final String user;
    private final long studyUid;
    private final QueryOptions fileQueryOptions;

    private Logger logger = LoggerFactory.getLogger(FileMongoDBIterator.class);

    public JobMongoDBIterator(MongoCursor mongoCursor, FileDBAdaptor fileDBAdaptor, JobConverter converter, QueryOptions fileQueryOptions) {
        super(mongoCursor, converter);
        this.fileDBAdaptor = fileDBAdaptor;
        nativeQuery = false;
        user = null;
        studyUid = -1;
        this.fileQueryOptions = fileQueryOptions;
    }

    public JobMongoDBIterator(MongoCursor mongoCursor, FileDBAdaptor fileDBAdaptor, JobConverter converter, QueryOptions fileQueryOptions,
                              String user, long studyUid) {
        super(mongoCursor, converter);
        this.fileDBAdaptor = fileDBAdaptor;
        this.user = user;
        this.studyUid = studyUid;
        this.fileQueryOptions = fileQueryOptions;
        nativeQuery = false;
    }

    @Override
    protected void fetchNextBatch(Queue<Document> buffer, int bufferSize) {

        Set<Long> fileUids = new HashSet<>();
        while (mongoCursor.hasNext() && buffer.size() < bufferSize) {
            Document job = (Document) mongoCursor.next();
            buffer.add(job);

            getFile(fileUids, job, JobDBAdaptor.QueryParams.OUT_DIR);
            getFile(fileUids, job, JobDBAdaptor.QueryParams.TMP_DIR);
            getFile(fileUids, job, JobDBAdaptor.QueryParams.LOG);
            getFile(fileUids, job, JobDBAdaptor.QueryParams.ERROR_LOG);
        }

        if (!fileUids.isEmpty()) {
            Query query = new Query(SampleDBAdaptor.QueryParams.UID.key(), new ArrayList<>(fileUids));
            List<Document> fileDocuments;
            try {
                if (user == null) {
                    fileDocuments = fileDBAdaptor.nativeGet(query, fileQueryOptions).getResults();
                } else {
                    fileDocuments = fileDBAdaptor.nativeGet(studyUid, query, fileQueryOptions, user).getResults();
                }
            } catch (CatalogDBException | CatalogAuthorizationException e) {
                logger.warn("Could not obtain the samples associated to the files: {}", e.getMessage(), e);
                return;
            }

            // Map each fileId uid - version to the sample entry
            Map<Long, Document> fileMap = fileDocuments
                    .stream()
                    .collect(Collectors.toMap(d -> d.getLong(FileDBAdaptor.QueryParams.UID.key()), d -> d));

            buffer.forEach(job -> {
                setFile(fileMap, job, JobDBAdaptor.QueryParams.OUT_DIR);
                setFile(fileMap, job, JobDBAdaptor.QueryParams.TMP_DIR);
                setFile(fileMap, job, JobDBAdaptor.QueryParams.LOG);
                setFile(fileMap, job, JobDBAdaptor.QueryParams.ERROR_LOG);
            });
        }
    }

    private void getFile(Set<Long> fileUids, Document job, JobDBAdaptor.QueryParams param) {
        Object file = job.get(param.key());
        if (file != null) {
            Number fileUid = ((Number) ((Document) file).get(FileDBAdaptor.QueryParams.UID.key()));
            if (fileUid != null && fileUid.longValue() > 0) {
                fileUids.add(fileUid.longValue());
            }
        }
    }

    private void setFile(Map<Long, Document> fileMap, Document job, JobDBAdaptor.QueryParams param) {
        Object file = job.get(param.key());
        if (file != null) {
            Number fileUid = ((Number) ((Document) file).get(FileDBAdaptor.QueryParams.UID.key()));
            if (fileUid != null && fileUid.longValue() > 0) {
                job.put(param.key(), fileMap.get(fileUid.longValue()));
            }
        }
    }
}
