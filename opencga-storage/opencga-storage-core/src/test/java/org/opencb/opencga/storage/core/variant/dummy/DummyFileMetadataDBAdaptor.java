package org.opencb.opencga.storage.core.variant.dummy;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.databind.MapperFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.opencb.biodata.models.variant.VariantFileMetadata;
import org.opencb.commons.datastore.core.DataResult;
import org.opencb.commons.datastore.core.Query;
import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.opencga.storage.core.exceptions.StorageEngineException;
import org.opencb.opencga.storage.core.metadata.adaptors.FileMetadataDBAdaptor;
import org.opencb.opencga.storage.core.metadata.models.FileMetadata;
import org.opencb.opencga.storage.core.metadata.models.Locked;

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


    public static void writeAll(Path path) {
        ObjectMapper objectMapper = new ObjectMapper(new JsonFactory()).configure(MapperFeature.REQUIRE_SETTERS_FOR_GETTERS, true);
        String prefix = "variant_file_metadata_" + NUM_PRINTS.incrementAndGet() + "_";
        System.out.println("prefix = " + prefix);
        System.out.println("VARIANT_FILE_METADATAS = " + VARIANT_FILE_METADATAS.size());
        for (VariantFileMetadata fileMetadata : VARIANT_FILE_METADATAS.values()) {
            try (OutputStream os = new FileOutputStream(path.resolve(prefix + fileMetadata.getId() + ".json").toFile())) {
                objectMapper.writerWithDefaultPrettyPrinter().writeValue(os, fileMetadata);
            } catch (IOException e) {
                throw new UncheckedIOException(e);
            }
        }
    }

    public static void clear() {
        FILE_METADATA_MAP.clear();
        VARIANT_FILE_METADATAS.clear();
    }

    public static synchronized void writeAndClear(Path path) {
        writeAll(path);
        clear();
    }

    @Override
    public LinkedHashSet<Integer> getIndexedFiles(int studyId) {
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
    }

    @Override
    public Integer getFileId(int studyId, String fileName) {
        return FILE_METADATA_MAP.getOrDefault(studyId, Collections.emptyMap()).values()
                .stream()
                .filter(f -> f.getName().equals(fileName))
                .map(FileMetadata::getId)
                .findFirst()
                .orElse(null);
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
    public Locked lock(int studyId, int id, long lockDuration, long timeout) throws StorageEngineException {
        return () -> { };
    }
}
