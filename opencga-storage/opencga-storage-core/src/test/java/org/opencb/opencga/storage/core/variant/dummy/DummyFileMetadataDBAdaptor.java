package org.opencb.opencga.storage.core.variant.dummy;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.databind.MapperFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.opencb.biodata.models.variant.VariantFileMetadata;
import org.opencb.commons.datastore.core.DataResult;
import org.opencb.commons.datastore.core.Query;
import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.opencga.storage.core.exceptions.StorageEngineException;
import org.opencb.opencga.storage.core.metadata.VariantStorageMetadataManager;
import org.opencb.opencga.storage.core.metadata.adaptors.FileMetadataDBAdaptor;
import org.opencb.opencga.storage.core.metadata.models.FileMetadata;
import org.opencb.opencga.storage.core.metadata.models.Lock;
import org.opencb.opencga.storage.core.metadata.models.StudyMetadata;

import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.io.UncheckedIOException;
import java.nio.file.Path;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

/**
 * Created on 02/05/18.
 *
 * @author Jacobo Coll &lt;jacobo167@gmail.com&gt;
 */
public class DummyFileMetadataDBAdaptor implements FileMetadataDBAdaptor {

    private static AtomicInteger NUM_PRINTS = new AtomicInteger();
    public static Map<Integer, Map<Integer, FileMetadata>> FILE_METADATA_MAP = new ConcurrentHashMap<>();
    private static Map<String, VariantFileMetadata> VARIANT_FILE_METADATAS = new HashMap<>();
    private static Set<String> DUPLICATED_NAMES = new HashSet<>();


    public static void writeAll(Path path) {
        ObjectMapper objectMapper = new ObjectMapper(new JsonFactory()).configure(MapperFeature.REQUIRE_SETTERS_FOR_GETTERS, true);
        int numPrints = NUM_PRINTS.incrementAndGet();
        String prefix = "study_" + numPrints + "_variant_file_metadata_";
        for (VariantFileMetadata fileMetadata : VARIANT_FILE_METADATAS.values()) {
            try (OutputStream os = new FileOutputStream(path.resolve(prefix + fileMetadata.getId() + ".json").toFile())) {
                objectMapper.writerWithDefaultPrettyPrinter().writeValue(os, fileMetadata);
            } catch (IOException e) {
                throw new UncheckedIOException(e);
            }
        }

        String filesPrefix = "study_" + numPrints + "_files_";
        for (Map.Entry<Integer, Map<Integer, FileMetadata>> entry : FILE_METADATA_MAP.entrySet()) {
            Integer studyId = entry.getKey();
            Map<Integer, FileMetadata> fileMetadataMap = entry.getValue();
            StudyMetadata sm = DummyStudyMetadataDBAdaptor.STUDY_METADATA_MAP.get(studyId);
            try {
                DummyStudyMetadataDBAdaptor.writeAll(path, objectMapper, filesPrefix, sm, fileMetadataMap.values());
            } catch (IOException e) {
                throw new UncheckedIOException("Error writing file metadata for study " + sm.getName(), e);
            }
        }
    }

    public static void clear() {
        FILE_METADATA_MAP.clear();
        VARIANT_FILE_METADATAS.clear();
        DUPLICATED_NAMES.clear();
    }

    public static synchronized void writeAndClear(Path path) {
        writeAll(path);
        clear();
    }

    @Override
    public LinkedHashSet<Integer> getIndexedFiles(int studyId, boolean includePartial) {
        return new LinkedHashSet<>(FILE_METADATA_MAP.getOrDefault(studyId, Collections.emptyMap()).values()
                .stream()
                .filter(FileMetadata::isIndexed)
                .map(FileMetadata::getId)
                .collect(Collectors.toList()));
    }

    @Override
    public Iterator<FileMetadata> fileIterator(int studyId) {
        return FILE_METADATA_MAP.getOrDefault(studyId, Collections.emptyMap()).values().iterator();
    }

//    @Override
//    public void updateIndexedFiles(int studyId, LinkedHashSet<Integer> indexedFiles) {
//
//    }

    @Override
    public FileMetadata getFileMetadata(int studyId, int fileId, Long timeStamp) {
        return FILE_METADATA_MAP.getOrDefault(studyId, Collections.emptyMap()).get(fileId);
    }

    @Override
    public void updateFileMetadata(int studyId, FileMetadata file, Long timeStamp) {
        FILE_METADATA_MAP.computeIfAbsent(studyId, s -> new ConcurrentHashMap<>()).put(file.getId(), file);
        if (file.isDuplicatedName()) {
            addDuplicated(file.getPath());
        }
    }

    @Override
    public Integer getFileId(int studyId, String fileName) {
        if (isDuplicated(fileName)) {
            return VariantStorageMetadataManager.DUPLICATED_NAME_ID;
        }
        return FILE_METADATA_MAP.getOrDefault(studyId, Collections.emptyMap()).values()
                .stream()
                .filter(f -> f.getName().equals(fileName))
                .map(FileMetadata::getId)
                .findFirst()
                .orElse(null);
    }

    private static void addDuplicated(String filePath) {
        int i = filePath.lastIndexOf("/");
        String name = i < 0 ? filePath : filePath.substring(i + 1);
        DUPLICATED_NAMES.add(name);
    }

    private static boolean isDuplicated(String file) {
        int i = file.lastIndexOf("/");
        String name = i < 0 ? file : file.substring(i + 1);
        return DUPLICATED_NAMES.contains(name);
    }

    @Override
    public DataResult count(Query query) {
        return new DataResult<>(0, Collections.emptyList(), 1, Collections.singletonList(((long) VARIANT_FILE_METADATAS.size())), 1);
    }

    @Override
    public void updateVariantFileMetadata(String studyId, VariantFileMetadata variantFileMetadata) throws StorageEngineException {
        VARIANT_FILE_METADATAS.put(studyId + "_" + variantFileMetadata.getId(), variantFileMetadata);
    }

    @Override
    public Iterator<VariantFileMetadata> iterator(Query query, QueryOptions options) throws IOException {
        return Collections.emptyIterator();
    }

    @Override
    public void removeVariantFileMetadata(int study, int file) {

    }

    @Override
    public void close() throws IOException {

    }

    @Override
    public Lock lock(int studyId, int id, long lockDuration, long timeout) throws StorageEngineException {
        return new Lock(0) {
            @Override
            public void unlock0() {

            }

            @Override
            public void refresh() {

            }
        };
    }
}
