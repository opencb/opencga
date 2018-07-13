package org.opencb.opencga.storage.core.variant.dummy;

import com.fasterxml.jackson.databind.MapperFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.opencb.commons.datastore.core.QueryResult;
import org.opencb.opencga.storage.core.exceptions.StorageEngineException;
import org.opencb.opencga.storage.core.metadata.ProjectMetadata;
import org.opencb.opencga.storage.core.metadata.StudyConfiguration;
import org.opencb.opencga.storage.core.metadata.adaptors.ProjectMetadataAdaptor;

import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.io.UncheckedIOException;
import java.nio.file.Path;
import java.util.Collections;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Created on 02/05/18.
 *
 * @author Jacobo Coll &lt;jacobo167@gmail.com&gt;
 */
public class DummyProjectMetadataAdaptor implements ProjectMetadataAdaptor {
    private static ProjectMetadata projectMetadata;

    @Override
    public long lockProject(long lockDuration, long timeout) throws InterruptedException, TimeoutException {
        return 0;
    }

    @Override
    public void unLockProject(long lockId) {
    }

    @Override
    public synchronized QueryResult<ProjectMetadata> getProjectMetadata() {
        final QueryResult<ProjectMetadata> result = new QueryResult<>("");
        if (projectMetadata == null) {
            result.setResult(Collections.singletonList(new ProjectMetadata("hsapiens", "grch37", 1)));
        } else {
            result.setResult(Collections.singletonList(projectMetadata.copy()));
        }
        return result;
    }

    @Override
    public synchronized QueryResult updateProjectMetadata(ProjectMetadata projectMetadata, boolean updateCounters) {
        this.projectMetadata = projectMetadata;
        return new QueryResult<>();
    }

    @Override
    public synchronized int generateId(StudyConfiguration studyConfiguration, String idType) throws StorageEngineException {
        ProjectMetadata projectMetadata = getProjectMetadata().first();
        Integer id = projectMetadata.getCounters().compute(idType + (studyConfiguration == null ? "" : ('_' + studyConfiguration.getStudyId())),
                (key, value) -> value == null ? 1 : value + 1);
        updateProjectMetadata(projectMetadata, true);
        return id;
    }

    private static final AtomicInteger NUM_PRINTS = new AtomicInteger();

    public static void writeAndClear(Path path) {
        ObjectMapper objectMapper = new ObjectMapper().configure(MapperFeature.REQUIRE_SETTERS_FOR_GETTERS, true);
        String name = "project_metadata_" + NUM_PRINTS.incrementAndGet() + ".json";
        try (OutputStream os = new FileOutputStream(path.resolve(name).toFile())) {
            objectMapper.writerWithDefaultPrettyPrinter().writeValue(os, projectMetadata);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
        clear();
    }

    public static void clear() {
        projectMetadata = null;
    }


}
