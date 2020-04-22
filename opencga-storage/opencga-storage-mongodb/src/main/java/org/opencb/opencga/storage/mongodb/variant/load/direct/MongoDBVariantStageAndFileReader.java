package org.opencb.opencga.storage.mongodb.variant.load.direct;

import org.apache.commons.lang3.tuple.Pair;
import org.bson.Document;
import org.opencb.biodata.models.variant.Variant;
import org.opencb.biodata.models.variant.avro.StructuralVariation;
import org.opencb.commons.datastore.mongodb.MongoDBCollection;
import org.opencb.commons.io.DataReader;
import org.opencb.opencga.storage.mongodb.variant.MongoDBVariantStoragePipeline;
import org.opencb.opencga.storage.mongodb.variant.converters.stage.StageDocumentToVariantConverter;
import org.opencb.opencga.storage.mongodb.variant.load.stage.MongoDBVariantStageReader;

import java.util.*;

/**
 * Read simultaneously the stage collection and a given file, and merge the result.
 *
 * <code>
 *
 * while (stageReaderIterator.hasNext() && fileReaderIterator.hasNext()) {
 *
 *     if (stageVariant == variant) {
 *         emit combination of stage document with variant
 *         move stage and file iterator
 *     } else if (stageVariant < variant) {
 *         emit stage document
 *         move stage iterator
 *     } else {
 *         emit new stage document with variant
 *         move file iterator
 *     }
 * }
 *
 * </code>
 *
 * Created on 21/08/18.
 * @author Jacobo Coll &lt;jacobo167@gmail.com&gt;
 */
public class MongoDBVariantStageAndFileReader implements DataReader<Document> {

    protected static final int BUFFER_SIZE = 1000;
    protected static final StructuralVariation EMPTY_STRUCTURAL_VARIATION = new StructuralVariation();
    protected static final double THRESHOLD_READ_MORE = 0.8;

    /**
     * Comparator to ensure same order from file and the stage collection.
     */
    private final Comparator<Variant> variantComparator = Comparator
            .comparing(Variant::getChromosome)
            .thenComparing(Variant::getStart)
            .thenComparing(Variant::getEnd)
            .thenComparing(Variant::getReference)
            .thenComparing(Variant::getAlternate)
            .thenComparing(variant -> variant.getSv() == null ? EMPTY_STRUCTURAL_VARIATION : variant.getSv());

    private final String studyIdStr;
    private final String fileId;
    private final int studyId;

    private final StageDocumentToVariantConverter converter = new StageDocumentToVariantConverter();

    private final DataReader<Variant> variantReader;
    private final MongoDBCollection stageCollection;
    private MongoDBVariantStageReader stageReader;

    private String currentChromosome;
    private String currentStageChromosome;

    /**
     * Buffer of variants from file being read.
     *
     * Should contain only variants from {@link #currentChromosome}.
     * Variants from other chromosomes should be stored in the variantsBufferPending.
     *
     * Don't need to be modifiable.
     */
    private List<Variant> variantsBuffer;
    /**
     * Iterator to the variant buffer.
     */
    private ListIterator<Variant> variantsIterator;
    /**
     * Pending variants to be added to the variantBuffer.
     *
     */
    private List<Variant> variantsBufferPending = new ArrayList<>();

    private List<Pair<Document, Variant>> stageBuffer;
    private ListIterator<Pair<Document, Variant>> stageIterator;

    private boolean addAllStageDocuments;
    private int skippedVariants = 0;
    private boolean drainStage;
    private boolean variantReaderEmpty;

    public MongoDBVariantStageAndFileReader(DataReader<Variant> variantReader, MongoDBCollection stageCollection, int studyId, int fileId,
                                            boolean addAllStageDocuments) {
        this.variantReader = variantReader;
        this.stageCollection = stageCollection;
        this.fileId = String.valueOf(fileId);
        this.studyId = studyId;
        this.studyIdStr = String.valueOf(studyId);
        this.addAllStageDocuments = addAllStageDocuments;
    }

    @Override
    public boolean open() {
        return variantReader.open();
    }

    @Override
    public boolean close() {
        if (stageReader != null) {
            stageReader.post();
            stageReader.close();
        }

        variantReader.close();

        return true;
    }

    @Override
    public boolean pre() {
        variantReader.pre();

        readMoreVariants();

        configureStageReader();
        readMoreFromStage();

        return true;
    }

    @Override
    public boolean post() {
        variantReader.post();
        return true;
    }


    @Override
    public List<Document> read(int batchSize) {

        List<Document> documents = new ArrayList<>(batchSize);

        Variant variant = null; // Variant from file
        Document document = null; // Document from stage
        Variant stageVariant = null; // Variant from stage

        boolean moveStage = true;
        boolean moveVariants = true;
        while (documents.size() < batchSize) {

            if (moveVariants) {
                if (!variantsIterator.hasNext()
                        || variantsIterator.nextIndex() > variantsBuffer.size() * THRESHOLD_READ_MORE) {
                    readMoreVariants();
                }
                if (!variantsIterator.hasNext()) {
                    variantsIterator = Collections.emptyListIterator();
                    if (addAllStageDocuments) {
                        variant = null;
                    } else {
                        // No more variants in this iterator. finish
                        break;
                    }
                } else {
                    variant = variantsIterator.next();

                    if (variant != null && MongoDBVariantStoragePipeline.SKIPPED_VARIANTS.contains(variant.getType())) {
                        skippedVariants++;
                        moveVariants = true;
                        continue;
                    }
                }

                moveVariants = false;


                // Check if chromosome changes
                if (!currentStageChromosome.equals(currentChromosome)) {
                    drainStage = true;
                }
            }

            if (moveStage) {
                if (!stageIterator.hasNext()) {
                    readMoreFromStage();
                }

                if (!stageIterator.hasNext()) {
                    document = null;
                    stageVariant = null;
                } else {
                    Pair<Document, Variant> pair = stageIterator.next();
                    document = pair.getKey();
                    stageVariant = pair.getValue();
                }
                moveStage = false;
            }

            if (drainStage && stageVariant == null) {
                configureStageReader();
                readMoreFromStage();
                drainStage = false;

                if (!stageIterator.hasNext()) {
                    document = null;
                    stageVariant = null;
                } else {
                    Pair<Document, Variant> pair = stageIterator.next();
                    document = pair.getKey();
                    stageVariant = pair.getValue();
                }
            }

            if (variant == null && stageVariant == null) {
                break;
            }

            int comparision = variant != null && stageVariant != null ? variantComparator.compare(variant, stageVariant) : 0;
            if (!drainStage && variant != null && stageVariant != null && comparision == 0) {
                // Found match! Move both iterators
                documents.add(combine(document, variant));
                moveStage = true;
                moveVariants = true;
            } else if (variant == null
                    || stageVariant != null && (drainStage || comparision > 0)) {
                // Stage variant not in file.
                if (addAllStageDocuments) {
                    documents.add(document);
                }
                moveStage = true;
            } else {
                // Variant not in stage! Add new stageDocument
                documents.add(newStageDocument(variant));
                moveVariants = true;
            }
        }

        // Move back both iterators (if possible), unless it has to be moved forward.
        if (!moveVariants && variantsIterator.hasPrevious()) {
            variantsIterator.previous();
        }
        if (!moveStage && stageIterator.hasPrevious()) {
            stageIterator.previous();
        }

        return documents;
    }

    private Document newStageDocument(Variant variant) {
        return combine(converter.convertToStorageType(variant), variant);
    }

    private Document combine(Document document, Variant variant) {

        Document studyDocument;
        if (!document.containsKey(studyIdStr)) {
            studyDocument = new Document();
            document.put(studyIdStr, studyDocument);
        } else {
            studyDocument = document.get(studyIdStr, Document.class);
        }

        studyDocument.append(fileId, Collections.singletonList(variant));

        return document;
    }

    private void configureStageReader() {
        if (stageReader != null) {
            stageReader.post();
            stageReader.close();
        }
        stageReader = new MongoDBVariantStageReader(stageCollection, studyId, Collections.singleton(currentChromosome));
        currentStageChromosome = currentChromosome;
        stageReader.open();
        stageReader.pre();
    }

    /**
     * Read more variants from {@link #variantReader}.
     *
     * * The {@link #variantsBuffer} may not be exhausted
     * * There may be variants in the {@link #variantsBufferPending}
     *
     * 1. Move remaining variants from {@link #variantsBuffer} to {@link #variantsBufferPending}
     * 2. Read more variants, if needed
     * 3. Incorporate pending variants
     * 4. Check chromosome
     * 5. Check order
     *
     */
    private void readMoreVariants() {
        if (variantReaderEmpty) {
            if (!variantsIterator.hasNext()) {
                variantsBuffer = variantsBufferPending;
                variantsBufferPending = new ArrayList<>();

                // Check chromosome
                checkCurrentChromosome();

                // Sort variants if needed
                ensureVariantsBufferIsSorted();

                variantsIterator = variantsBuffer.listIterator();
            }
            return;
        }

        // Move remaining variants to pending buffer
        if (variantsIterator != null && variantsIterator.hasNext()) {
            variantsBufferPending.addAll(0, variantsBuffer.subList(variantsIterator.nextIndex(), variantsBuffer.size()));
            variantsBuffer = Collections.emptyList();
            variantsIterator = variantsBuffer.listIterator();
        }

        // Read more variants, if needed
        if (BUFFER_SIZE > variantsBufferPending.size()) {
            variantsBuffer = variantReader.read(BUFFER_SIZE - variantsBufferPending.size());
            if (variantsBuffer.isEmpty()) {
                variantReaderEmpty = true;
            }
        }

        // Incorporate pending variants
        if (!variantsBufferPending.isEmpty()) {
            variantsBufferPending.addAll(variantsBuffer);
            variantsBuffer = variantsBufferPending;
            variantsIterator = variantsBuffer.listIterator();
            variantsBufferPending = new ArrayList<>(BUFFER_SIZE);
        }

        // If the readed didn't provide enough variants, read again
        if (!variantReaderEmpty && variantsBuffer.size() < BUFFER_SIZE) {
            variantsBufferPending = variantsBuffer;
            variantsBuffer = Collections.emptyList();
            variantsIterator = variantsBuffer.listIterator();
            readMoreVariants();
        }

        // Check chromosome
        checkCurrentChromosome();

        // Sort variants if needed
        ensureVariantsBufferIsSorted();

        variantsIterator = variantsBuffer.listIterator();
    }

    /**
     * Check that there is only one chromosome in the {@link #variantsBuffer}.
     */
    private void checkCurrentChromosome() {
        // Change the current chromosome
        if (!variantsBuffer.isEmpty()) {
            currentChromosome = variantsBuffer.get(0).getChromosome();
        }

        // Check if the last read variant have the same chromosome as the first one
        if (currentChromosome != null && !variantsBuffer.isEmpty()
                && !variantsBuffer.get(variantsBuffer.size() - 1).getChromosome().equals(currentChromosome)) {
            // Move variants from a different chromosome to variantsBufferPending
            Iterator<Variant> iterator = variantsBuffer.iterator();
            while (iterator.hasNext()) {
                Variant variant = iterator.next();
                if (!variant.getChromosome().equals(currentChromosome)) {
                    iterator.remove();
                    variantsBufferPending.add(variant);
                }
            }
        }
    }

    private void ensureVariantsBufferIsSorted() {
        if (variantsBuffer.isEmpty()) {
            return;
        }
        boolean sort = false;
        Variant variant = variantsBuffer.get(0);
        for (Variant thisVariant : variantsBuffer) {
            if (variantComparator.compare(variant, thisVariant) > 0) {
                sort = true;
                break;
            }
            variant = thisVariant;
        }
        if (sort) {
            variantsBuffer.sort(variantComparator);
        }
    }

    private void readMoreFromStage() {
        boolean sort = false;
        Variant variant = null;
        List<Pair<Document, Variant>> list = new ArrayList<>();
        for (Document document : stageReader.read(BUFFER_SIZE)) {
            Variant thisVariant = converter.convertToDataModelType(document);
            list.add(Pair.of(document, thisVariant));
            if (variant != null) {
                sort = variantComparator.compare(variant, thisVariant) > 0;
            }
            variant = thisVariant;
        }
        if (sort) {
            list.sort(Comparator.comparing(Pair::getValue, variantComparator));
        }

        stageBuffer = list;


        stageIterator = stageBuffer.listIterator();
    }

    public long getSkippedVariants() {
        return skippedVariants;
    }
}
