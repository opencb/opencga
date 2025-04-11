package org.opencb.opencga.storage.core.variant.dummy;

import com.fasterxml.jackson.databind.MapperFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.opencb.commons.datastore.core.DataResult;
import org.opencb.opencga.storage.core.exceptions.StorageEngineException;
import org.opencb.opencga.storage.core.metadata.adaptors.ProjectMetadataAdaptor;
import org.opencb.opencga.storage.core.metadata.models.Lock;
import org.opencb.opencga.storage.core.metadata.models.ProjectMetadata;

import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.io.UncheckedIOException;
import java.nio.file.Path;
import java.util.Collections;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Created on 02/05/18.
 *
 * @author Jacobo Coll &lt;jacobo167@gmail.com&gt;
 */
public class DummyProjectMetadataAdaptor implements ProjectMetadataAdaptor {
    private static ProjectMetadata projectMetadata;
    private static Map<String, Integer> counters = new ConcurrentHashMap<>();

    @Override
    public Lock lockProject(long lockDuration, long timeout, String lockName) {
        return new Lock(0) {
            @Override
            public void unlock0() {

            }

            @Override
            public void refresh() {

            }
        };
    }

    @Override
    public synchronized DataResult<ProjectMetadata> getProjectMetadata() {
        final DataResult<ProjectMetadata> result = new DataResult<>();
        if (projectMetadata != null) {
            result.setResults(Collections.singletonList(projectMetadata.copy()));
        }
        return result;
    }

    @Override
    public synchronized DataResult updateProjectMetadata(ProjectMetadata projectMetadata, boolean updateCounters) {
        DummyProjectMetadataAdaptor.projectMetadata = projectMetadata;
        return new DataResult();
    }

    @Override
    public synchronized int generateId(Integer studyId, String idType) throws StorageEngineException {
        final int startingId;
        // Use different starting ids for different types of ids to avoid collisions in DummyLock
        switch (idType) {
            case "study":
                startingId = 1;
                break;
            case "file":
                startingId = 100000;
                break;
            case "sample":
                startingId = 200000;
                break;
            case "cohort":
                startingId = 300000;
                break;
            case "task":
                startingId = 400000;
                break;
            case "score":
                startingId = 500000;
                break;
            default:
                startingId = 80000;
                break;

        }
        return counters.compute(idType + (studyId == null ? "" : ("_" + studyId)),
                (key, value) -> value == null ? startingId : value + 1);
    }

    @Override
    public boolean exists() {
        return projectMetadata != null;
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
        counters.clear();
    }


}
