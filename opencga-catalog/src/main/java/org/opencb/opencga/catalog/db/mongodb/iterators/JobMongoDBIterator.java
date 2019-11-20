package org.opencb.opencga.catalog.db.mongodb.iterators;

import com.mongodb.client.ClientSession;
import com.mongodb.client.MongoCursor;
import org.bson.Document;
import org.opencb.commons.datastore.core.Query;
import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.opencga.catalog.db.api.FileDBAdaptor;
import org.opencb.opencga.catalog.db.api.JobDBAdaptor;
import org.opencb.opencga.catalog.db.mongodb.FileMongoDBAdaptor;
import org.opencb.opencga.catalog.db.mongodb.converters.JobConverter;
import org.opencb.opencga.catalog.exceptions.CatalogAuthorizationException;
import org.opencb.opencga.catalog.exceptions.CatalogDBException;
import org.opencb.opencga.catalog.managers.FileManager;
import org.opencb.opencga.core.models.Job;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.stream.Collectors;

public class JobMongoDBIterator extends BatchedMongoDBIterator<Job> {

    private final long studyUid;
    private final String user;

    private final FileMongoDBAdaptor fileDBAdaptor;
    private final QueryOptions fileQueryOptions = FileManager.INCLUDE_FILE_URI_PATH;

    private Logger logger = LoggerFactory.getLogger(JobMongoDBIterator.class);

    public JobMongoDBIterator(MongoCursor mongoCursor, ClientSession clientSession, JobConverter converter,
                              FileMongoDBAdaptor fileDBAdaptor) {
        this(mongoCursor, clientSession, converter, fileDBAdaptor, 0, null);
    }

    public JobMongoDBIterator(MongoCursor mongoCursor, ClientSession clientSession, JobConverter converter,
                              FileMongoDBAdaptor fileDBAdaptor, long studyUid, String user) {
        super(mongoCursor, clientSession, converter, null);
        this.fileDBAdaptor = fileDBAdaptor;
        this.user = user;
        this.studyUid = studyUid;
    }

    @Override
    protected void fetchNextBatch(Queue<Document> buffer, int bufferSize) {
        Set<Long> fileUids = new HashSet<>();
        while (mongoCursor.hasNext() && buffer.size() < bufferSize) {
            Document job = (Document) mongoCursor.next();
            buffer.add(job);

            getFiles(fileUids, job, JobDBAdaptor.QueryParams.INPUT);
            getFiles(fileUids, job, JobDBAdaptor.QueryParams.OUTPUT);
            getFile(fileUids, job, JobDBAdaptor.QueryParams.OUT_DIR);
            getFile(fileUids, job, JobDBAdaptor.QueryParams.LOG);
            getFile(fileUids, job, JobDBAdaptor.QueryParams.ERROR_LOG);
        }

        if (!fileUids.isEmpty()) {
            Query query = new Query(FileDBAdaptor.QueryParams.UID.key(), new ArrayList<>(fileUids));
            List<Document> fileDocuments;
            try {
                if (user == null) {
                    fileDocuments = fileDBAdaptor.nativeGet(query, fileQueryOptions).getResults();
                } else {
                    fileDocuments = fileDBAdaptor.nativeGet(studyUid, query, fileQueryOptions, user).getResults();
                }
            } catch (CatalogDBException | CatalogAuthorizationException e) {
                logger.warn("Could not obtain the files associated to the jobs: {}", e.getMessage(), e);
                return;
            }

            // Map each fileId uid to the file entry
            Map<Long, Document> fileMap = fileDocuments
                    .stream()
                    .collect(Collectors.toMap(d -> d.getLong(FileDBAdaptor.QueryParams.UID.key()), d -> d));

            buffer.forEach(job -> {
                setFiles(fileMap, job, JobDBAdaptor.QueryParams.INPUT);
                setFiles(fileMap, job, JobDBAdaptor.QueryParams.OUTPUT);
                setFile(fileMap, job, JobDBAdaptor.QueryParams.OUT_DIR);
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

    private void getFiles(Set<Long> fileUids, Document job, JobDBAdaptor.QueryParams param) {
        Object files = job.get(param.key());
        if (files != null) {
            for (Object file : ((Collection) files)) {
                Number fileUid = ((Number) ((Document) file).get(FileDBAdaptor.QueryParams.UID.key()));
                if (fileUid != null && fileUid.longValue() > 0) {
                    fileUids.add(fileUid.longValue());
                }
            }
        }
    }

    private void setFiles(Map<Long, Document> fileMap, Document job, JobDBAdaptor.QueryParams param) {
        Object files = job.get(param.key());
        if (files != null) {
            List<Document> updatedfiles = new ArrayList<>(((Collection) files).size());
            for (Object file : ((Collection) files)) {
                Number fileUid = ((Number) ((Document) file).get(FileDBAdaptor.QueryParams.UID.key()));
                if (fileUid != null && fileUid.longValue() > 0) {
                    updatedfiles.add(fileMap.get(fileUid.longValue()));
                }
            }
            job.put(param.key(), updatedfiles);
        }
    }
}
