package org.opencb.opencga.storage.core.variant.index.sample.local;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.Iterators;
import org.apache.commons.collections4.CollectionUtils;
import org.opencb.biodata.models.core.Region;
import org.opencb.biodata.models.variant.Variant;
import org.opencb.commons.datastore.core.ObjectMap;
import org.opencb.opencga.storage.core.exceptions.StorageEngineException;
import org.opencb.opencga.storage.core.metadata.VariantStorageMetadataManager;
import org.opencb.opencga.storage.core.metadata.models.SampleMetadata;
import org.opencb.opencga.storage.core.utils.iterators.CloseableIterator;
import org.opencb.opencga.storage.core.variant.VariantStorageEngine;
import org.opencb.opencga.storage.core.variant.adaptors.VariantQueryException;
import org.opencb.opencga.storage.core.variant.adaptors.iterators.VariantDBIterator;
import org.opencb.opencga.storage.core.variant.index.sample.SampleIndexDBAdaptor;
import org.opencb.opencga.storage.core.variant.index.sample.annotation.SampleAnnotationIndexer;
import org.opencb.opencga.storage.core.variant.index.sample.family.SampleFamilyIndexer;
import org.opencb.opencga.storage.core.variant.index.sample.genotype.SampleGenotypeIndexer;
import org.opencb.opencga.storage.core.variant.index.sample.genotype.SampleIndexEntryBuilder;
import org.opencb.opencga.storage.core.variant.index.sample.genotype.SampleIndexVariantBiConverter;
import org.opencb.opencga.storage.core.variant.index.sample.models.SampleIndexEntry;
import org.opencb.opencga.storage.core.variant.index.sample.models.SampleIndexEntryIterator;
import org.opencb.opencga.storage.core.variant.index.sample.models.SampleIndexVariant;
import org.opencb.opencga.storage.core.variant.index.sample.query.LocusQuery;
import org.opencb.opencga.storage.core.variant.index.sample.query.RawSampleIndexEntryFilter;
import org.opencb.opencga.storage.core.variant.index.sample.query.SampleIndexEntryFilter;
import org.opencb.opencga.storage.core.variant.index.sample.query.SingleSampleIndexQuery;
import org.opencb.opencga.storage.core.variant.index.sample.schema.SampleIndexSchema;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.io.UnsupportedEncodingException;
import java.net.URLDecoder;
import java.net.URLEncoder;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.*;
import java.util.function.Consumer;
import java.util.stream.Stream;

public class LocalSampleIndexDBAdaptor extends SampleIndexDBAdaptor {

    private final Path basePath;
    private final ObjectMapper objectMapper;

    public LocalSampleIndexDBAdaptor(VariantStorageMetadataManager metadataManager, Path basePath) {
        super(metadataManager);
        this.basePath = basePath;
        this.objectMapper = new ObjectMapper()
                .configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false)
                .setSerializationInclusion(JsonInclude.Include.NON_NULL);
    }

    @Override
    public SampleGenotypeIndexer newSampleGenotypeIndexer(VariantStorageEngine engine) throws StorageEngineException {
        return new LocalSampleGenotypeIndexer(this, engine);
    }

    @Override
    public LocalSampleIndexEntryWriter newSampleIndexEntryWriter(int studyId, int fileId,
                                                                 SampleIndexSchema schema, ObjectMap options)
            throws StorageEngineException {
        return new LocalSampleIndexEntryWriter(this, studyId, schema);
    }

    @Override
    public SampleAnnotationIndexer newSampleAnnotationIndexer(VariantStorageEngine engine) throws StorageEngineException {
        return new LocalSampleAnnotationIndexer(this, engine);
    }

    @Override
    public SampleFamilyIndexer newSampleFamilyIndexer(VariantStorageEngine engine) throws StorageEngineException {
        return new LocalSampleFamilyIndexer(this, engine);
    }

    @Override
    public SampleIndexEntryBuilder queryByGtBuilder(int study, int sample, String chromosome, int position,
            SampleIndexSchema schema) throws IOException {
        int batchStart = SampleIndexSchema.getChunkStart(position);
        SampleMetadata sampleMetadata = metadataManager.getSampleMetadata(study, sample);
        boolean multiFileSample = sampleMetadata != null && sampleMetadata.isMultiFileSample();
        SampleIndexEntryBuilder builder = new SampleIndexEntryBuilder(sample, chromosome, batchStart, schema,
                false, multiFileSample);
        try {
            SampleIndexEntry entry = readEntry(study, schema.getVersion(), sample, chromosome, batchStart);
            if (entry == null || entry.getGts().isEmpty()) {
                return builder;
            }
            SampleIndexVariantBiConverter converter = new SampleIndexVariantBiConverter(schema);
            for (String gt : entry.getGts().keySet()) {
                SampleIndexEntryIterator iterator = converter.toVariantsIterator(entry, gt);
                while (iterator.hasNext()) {
                    builder.add(gt, iterator.nextSampleIndexVariant());
                }
            }
            return builder;
        } catch (StorageEngineException e) {
            throw new IOException("Error reading sample index entry for sample " + sample + " at "
                    + chromosome + ":" + batchStart, e);
        }
    }

    @Override
    protected VariantDBIterator internalIterator(SingleSampleIndexQuery query, SampleIndexSchema schema) {
        Collection<LocusQuery> locusQueries;
        if (CollectionUtils.isEmpty(query.getLocusQueries())) {
            locusQueries = Collections.singletonList(null);
        } else {
            locusQueries = query.getLocusQueries();
        }

        int studyId = toStudyId(query.getStudy());
        int sampleId = metadataManager.getSampleId(studyId, query.getSample());

        Iterator<Iterator<Variant>> iterators = locusQueries.stream()
                .map(locusQuery -> {
                    try {
                        CloseableIterator<SampleIndexEntry> entryIterator = indexEntryIterator(studyId, sampleId,
                                locusQuery == null ? null : locusQuery.getChunkRegion(),
                                schema);
                        SampleIndexEntryFilter filter = buildSampleIndexEntryFilter(query, locusQuery);
                        Iterator<Iterator<Variant>> transform = Iterators.transform(entryIterator,
                                entry -> filter.filter(entry).iterator());
                        return Iterators.concat(transform);
                    } catch (IOException e) {
                        throw VariantQueryException.internalException(e);
                    }
                }).iterator();

        Iterator<Variant> concatenated = Iterators.concat(iterators);

        return VariantDBIterator.wrapper(concatenated);
    }

    @Override
    protected CloseableIterator<SampleIndexVariant> rawInternalIterator(SingleSampleIndexQuery query,
            SampleIndexSchema schema) {
        Collection<LocusQuery> locusQueries;
        if (CollectionUtils.isEmpty(query.getLocusQueries())) {
            locusQueries = Collections.singletonList(null);
        } else {
            locusQueries = query.getLocusQueries();
        }

        int studyId = toStudyId(query.getStudy());
        int sampleId = metadataManager.getSampleId(studyId, query.getSample());

        Iterator<Iterator<SampleIndexVariant>> iterators = locusQueries.stream()
                .map(locusQuery -> {
                    try {
                        return createIteratorForLocusQuery(studyId, sampleId, schema, query, locusQuery);
                    } catch (StorageEngineException e) {
                        throw new org.opencb.opencga.storage.core.variant.adaptors.VariantQueryException(
                                "Error creating iterator", e);
                    }
                }).iterator();

        Iterator<SampleIndexVariant> concatenated = Iterators.concat(iterators);
        return CloseableIterator.wrap(concatenated);
    }

    private Iterator<SampleIndexVariant> createIteratorForLocusQuery(int studyId, int sampleId,
            SampleIndexSchema schema,
            SingleSampleIndexQuery query,
            LocusQuery locusQuery)
            throws StorageEngineException {

        RawSampleIndexEntryFilter filter = new RawSampleIndexEntryFilter(query, locusQuery);
        // Use the chunk-aligned region covering ALL locus sub-regions (matches internalIterator's
        // approach). Previously took only locusQuery.getRegions().get(0), silently dropping the rest.
        Region region = locusQuery == null ? null : locusQuery.getChunkRegion();

        try {
            CloseableIterator<SampleIndexEntry> entryIterator = indexEntryIterator(studyId, sampleId, region, schema);
            Iterator<Iterator<SampleIndexVariant>> transform = Iterators.transform(entryIterator,
                    entry -> filter.filter(entry).iterator());
            return Iterators.concat(transform);
        } catch (IOException e) {
            throw new StorageEngineException("Error reading sample index", e);
        }
    }

    @Override
    public CloseableIterator<SampleIndexEntry> indexEntryIterator(int study, int sample, Region region,
                                                                  SampleIndexSchema schema) throws IOException {
        int version = schema.getVersion();
        if (region == null) {
            // Lazy iterator over all sorted chunks for the sample
            List<Path> paths;
            try {
                paths = listSortedEntryFiles(study, version, sample);
            } catch (StorageEngineException e) {
                throw new IOException("Error reading sample index entries", e);
            }
            Iterator<Path> pathIt = paths.iterator();
            return CloseableIterator.wrap(new Iterator<SampleIndexEntry>() {
                @Override
                public boolean hasNext() {
                    return pathIt.hasNext();
                }

                @Override
                public SampleIndexEntry next() {
                    Path path = pathIt.next();
                    try {
                        return objectMapper.readValue(path.toFile(), SampleIndexEntry.class);
                    } catch (IOException e) {
                        throw new UncheckedIOException(e);
                    }
                }
            });
        }
        // Bounded region: walk chunk-aligned batchStarts in order; lazily read each.
        String chromosome = region.getChromosome();
        int startBatch = SampleIndexSchema.getChunkStart(region.getStart());
        int endBatch = SampleIndexSchema.getChunkStart(region.getEnd());
        Iterator<Integer> batchStarts = batchStartIterator(startBatch, endBatch);
        return CloseableIterator.wrap(new Iterator<SampleIndexEntry>() {
            private SampleIndexEntry queued;

            @Override
            public boolean hasNext() {
                while (queued == null && batchStarts.hasNext()) {
                    int batchStart = batchStarts.next();
                    try {
                        queued = readEntry(study, version, sample, chromosome, batchStart);
                    } catch (StorageEngineException e) {
                        throw new UncheckedIOException(new IOException(e));
                    }
                }
                return queued != null;
            }

            @Override
            public SampleIndexEntry next() {
                if (!hasNext()) {
                    throw new NoSuchElementException();
                }
                SampleIndexEntry next = queued;
                queued = null;
                return next;
            }
        });
    }

    @Override
    public Iterator<Map<String, List<Variant>>> iteratorByGt(int study, int sample, SampleIndexSchema schema) throws IOException {
        final SampleIndexVariantBiConverter converter = new SampleIndexVariantBiConverter(schema);
        List<Path> paths;
        try {
            paths = listSortedEntryFiles(study, schema.getVersion(), sample);
        } catch (StorageEngineException e) {
            throw new IOException(e);
        }
        Iterator<Path> pathIt = paths.iterator();
        // Lazy: decode one chunk at a time as the consumer pulls
        return new Iterator<Map<String, List<Variant>>>() {
            @Override
            public boolean hasNext() {
                return pathIt.hasNext();
            }

            @Override
            public Map<String, List<Variant>> next() {
                Path path = pathIt.next();
                SampleIndexEntry entry;
                try {
                    entry = objectMapper.readValue(path.toFile(), SampleIndexEntry.class);
                } catch (IOException e) {
                    throw new UncheckedIOException(e);
                }
                Map<String, List<Variant>> byGt = new HashMap<>();
                for (String gt : entry.getGts().keySet()) {
                    SampleIndexEntry.SampleIndexGtEntry gtEntry = entry.getGts().get(gt);
                    if (gtEntry == null || gtEntry.getVariants() == null || gtEntry.getVariantsLength() == 0) {
                        byGt.put(gt, Collections.emptyList());
                        continue;
                    }
                    List<Variant> variants = new ArrayList<>();
                    SampleIndexEntryIterator it = converter.toVariantsIterator(entry, gt);
                    while (it.hasNext()) {
                        variants.add(it.next());
                    }
                    byGt.put(gt, variants);
                }
                return byGt;
            }
        };
    }

    private static Iterator<Integer> batchStartIterator(int startBatch, int endBatch) {
        return new Iterator<Integer>() {
            private int current = startBatch;

            @Override
            public boolean hasNext() {
                return current <= endBatch;
            }

            @Override
            public Integer next() {
                if (!hasNext()) {
                    throw new NoSuchElementException();
                }
                int v = current;
                current += SampleIndexSchema.BATCH_SIZE;
                return v;
            }
        };
    }

    @Override
    protected long count(SingleSampleIndexQuery query) {
        CloseableIterator<SampleIndexVariant> iterator = rawInternalIterator(query, query.getSchema());
        long count = 0;
        try {
            while (iterator.hasNext()) {
                iterator.next();
                count++;
            }
        } finally {
            try {
                iterator.close();
            } catch (Exception e) {
                // Log and ignore close exception
            }
        }
        return count;
    }

    // Chromosome name sanitization for file system compatibility
    private String sanitizeChromosome(String chromosome) {
        try {
            return URLEncoder.encode(chromosome, "UTF-8");
        } catch (UnsupportedEncodingException e) {
            throw new IllegalStateException("UTF-8 is required to be supported by every JVM", e);
        }
    }

    private String desanitizeChromosome(String encoded) {
        try {
            return URLDecoder.decode(encoded, "UTF-8");
        } catch (UnsupportedEncodingException e) {
            throw new IllegalStateException("UTF-8 is required to be supported by every JVM", e);
        }
    }

    public Path getStudyVersionPath(int studyId, int version) {
        return basePath.resolve(studyId + "_" + version);
    }

    public Path getSamplePath(int studyId, int version, int sampleId) {
        return getStudyVersionPath(studyId, version).resolve(String.valueOf(sampleId));
    }

    public Path getEntryPath(int studyId, int version, int sampleId, String chromosome, int batchStart) {
        String filename = sanitizeChromosome(chromosome) + "_" + batchStart + ".json";
        return getSamplePath(studyId, version, sampleId).resolve(filename);
    }

    public void writeEntry(int studyId, int version, SampleIndexEntry entry) throws StorageEngineException {
        Path path = getEntryPath(studyId, version, entry.getSampleId(), entry.getChromosome(), entry.getBatchStart());
        try {
            if (path.getParent() != null && !Files.exists(path.getParent())) {
                Files.createDirectories(path.getParent());
            }
            objectMapper.writeValue(path.toFile(), entry);
        } catch (IOException e) {
            throw new StorageEngineException("Error writing entry to " + path, e);
        }
    }

    public void forEachEntry(int studyId, int version, int sampleId, Consumer<SampleIndexEntry> consumer)
            throws StorageEngineException {
        for (Path path : listSortedEntryFiles(studyId, version, sampleId)) {
            try {
                consumer.accept(objectMapper.readValue(path.toFile(), SampleIndexEntry.class));
            } catch (IOException e) {
                throw new StorageEngineException("Error reading entry from " + path, e);
            }
        }
    }

    /**
     * Get the list of unique regions covered by the sample-index entries of the given samples.
     * Each region corresponds to one chunk (batch). Output is sorted deterministically by
     * (chromosome, batchStart).
     */
    public List<Region> getRegionBounds(int studyId, int version, List<Integer> sampleIds)
            throws StorageEngineException {
        Set<String> regionKeys = new HashSet<>();
        List<Region> regions = new ArrayList<>();
        for (Integer sampleId : sampleIds) {
            for (Path path : listSortedEntryFiles(studyId, version, sampleId)) {
                String filename = path.getFileName().toString();
                String chromosome = parseChromosomeFromFilename(filename);
                int batchStart = parseBatchStartFromFilename(filename);
                int batchEnd = batchStart + SampleIndexSchema.BATCH_SIZE - 1;
                if (regionKeys.add(chromosome + ":" + batchStart)) {
                    regions.add(new Region(chromosome, batchStart, batchEnd));
                }
            }
        }
        regions.sort(Comparator.comparing(Region::getChromosome).thenComparingInt(Region::getStart));
        return regions;
    }

    /**
     * List all .json entry files for the given sample, sorted by (chromosome, batchStart).
     * Returns an empty list if the sample directory does not exist.
     * Throws if a filename does not match the expected {@code <chromosome>_<batchStart>.json} format.
     */
    private List<Path> listSortedEntryFiles(int studyId, int version, int sampleId) throws StorageEngineException {
        Path samplePath = getSamplePath(studyId, version, sampleId);
        if (!Files.exists(samplePath)) {
            return Collections.emptyList();
        }
        List<Path> paths = new ArrayList<>();
        try (Stream<Path> stream = Files.list(samplePath)) {
            stream.forEach(path -> {
                if (Files.isRegularFile(path) && path.getFileName().toString().endsWith(".json")) {
                    paths.add(path);
                }
            });
        } catch (IOException e) {
            throw new StorageEngineException("Error listing files for sample " + sampleId, e);
        }
        paths.sort((p1, p2) -> {
            String f1 = p1.getFileName().toString();
            String f2 = p2.getFileName().toString();
            int chrCmp = parseChromosomeFromFilename(f1).compareTo(parseChromosomeFromFilename(f2));
            if (chrCmp != 0) {
                return chrCmp;
            }
            return Integer.compare(parseBatchStartFromFilename(f1), parseBatchStartFromFilename(f2));
        });
        return paths;
    }

    private String parseChromosomeFromFilename(String filename) {
        int underscoreIdx = filename.lastIndexOf('_');
        if (underscoreIdx <= 0) {
            throw new IllegalStateException(
                    "Malformed filename: " + filename + ". Expected format: chromosome_batchStart.json");
        }
        return desanitizeChromosome(filename.substring(0, underscoreIdx));
    }

    private static int parseBatchStartFromFilename(String filename) {
        int underscoreIdx = filename.lastIndexOf('_');
        int dotIdx = filename.lastIndexOf('.');
        if (underscoreIdx <= 0 || dotIdx <= underscoreIdx) {
            throw new IllegalStateException(
                    "Malformed filename: " + filename + ". Expected format: chromosome_batchStart.json");
        }
        try {
            return Integer.parseInt(filename.substring(underscoreIdx + 1, dotIdx));
        } catch (NumberFormatException e) {
            throw new IllegalStateException(
                    "Malformed filename: " + filename + ". Expected format: chromosome_batchStart.json", e);
        }
    }

    public SampleIndexEntry readEntry(int studyId, int version, int sampleId, String chromosome, int batchStart)
            throws StorageEngineException {
        Path path = getEntryPath(studyId, version, sampleId, chromosome, batchStart);
        if (!Files.exists(path)) {
            return null;
        }
        try {
            return objectMapper.readValue(path.toFile(), SampleIndexEntry.class);
        } catch (IOException e) {
            throw new StorageEngineException("Error reading entry from " + path, e);
        }
    }
}
