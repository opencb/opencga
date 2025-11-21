package org.opencb.opencga.storage.mongodb.variant.adaptors;

import org.bson.Document;
import org.bson.conversions.Bson;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.opencb.commons.datastore.core.Query;
import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.opencga.core.testclassification.duration.ShortTests;
import org.opencb.opencga.storage.core.exceptions.StorageEngineException;
import org.opencb.opencga.storage.core.metadata.VariantStorageMetadataManager;
import org.opencb.opencga.storage.core.metadata.models.SampleMetadata;
import org.opencb.opencga.storage.core.metadata.models.StudyMetadata;
import org.opencb.opencga.storage.core.variant.VariantStorageOptions;
import org.opencb.opencga.storage.core.variant.adaptors.GenotypeClass;
import org.opencb.opencga.storage.core.variant.adaptors.VariantQuery;
import org.opencb.opencga.storage.core.variant.dummy.DummyStudyMetadataDBAdaptor;
import org.opencb.opencga.storage.core.variant.dummy.DummyVariantStorageMetadataDBAdaptorFactory;
import org.opencb.opencga.storage.mongodb.variant.MongoDBVariantStorageOptions;

import java.util.*;

import static org.opencb.opencga.storage.core.variant.adaptors.VariantField.*;
import static org.opencb.opencga.storage.core.variant.adaptors.VariantQueryParam.*;
import static org.opencb.opencga.storage.core.variant.query.VariantQueryUtils.*;
import static org.opencb.opencga.storage.mongodb.variant.converters.DocumentToStudyVariantEntryConverter.*;
import static org.opencb.opencga.storage.mongodb.variant.converters.DocumentToVariantAnnotationConverterTest.ANY;
import static org.opencb.opencga.storage.mongodb.variant.converters.DocumentToVariantAnnotationConverterTest.checkEqualDocuments;
import static org.opencb.opencga.storage.mongodb.variant.converters.DocumentToVariantConverter.*;

/**
 * Created on 21/08/17.
 *
 * @author Jacobo Coll &lt;jacobo167@gmail.com&gt;
 */
@Category(ShortTests.class)
public class VariantMongoDBQueryParserTest {

    public static final int sample_10101_id = 10101;
    public static final int sample_10201_id = 10201;
    public static final int sample_10301_id = 10301;
    public static final int file_0_id = 100000;
    public static final int file_1_id = 100001;
    public static final int file_2_id = 100002;
    public static final int file_3_id = 100003;
    public static final int file_4_id = 100004;
    private VariantMongoDBQueryParser parser;
    private VariantStorageMetadataManager metadataManager;

    @Before
    public void setUp() throws Exception {
        DummyVariantStorageMetadataDBAdaptorFactory.clear();
        metadataManager = new VariantStorageMetadataManager(new DummyVariantStorageMetadataDBAdaptorFactory());
        parser = new VariantMongoDBQueryParser(metadataManager);

        newStudy("study_1", 4, false);
        newStudy("study_2", 4, true);
    }

    @After
    public void tearDown() throws Exception {
        DummyVariantStorageMetadataDBAdaptorFactory.clear();
    }

    protected StudyMetadata newStudy(String studyName, int files, boolean sameSamples) throws StorageEngineException {
        StudyMetadata sm = metadataManager.createStudy(studyName);
        int studyId = sm.getId();
        for (int fileIdx = 0; fileIdx <= files; fileIdx++) {
            int fileId = metadataManager.registerFile(studyId, "/data/file_" + fileIdx);

            List<String> samplesInFile = new ArrayList<>();
            for (int sampleIdx = 1; sampleIdx <= 5; sampleIdx++) {
                int sampleId;
                if (sameSamples) {
                    sampleId = studyId * 10000 + sampleIdx;
                } else {
                    sampleId = studyId * 10000 + 100 * (fileId - 100000) + sampleIdx;
                }
                String name = "sample_" + sampleId;
                System.out.println("Registering sample " + name + " in file " + fileId + " of study " + studyName);
                samplesInFile.add(name);
                metadataManager.unsecureUpdateSampleMetadata(studyId, new SampleMetadata(studyId, sampleId, name));
            }
            metadataManager.registerFileSamples(studyId, fileId, samplesInFile);
//            metadataManager.updateFileMetadata(studyId, fileId, fileMetadata -> fileMetadata.setSamples(samplesInFile));
            metadataManager.addIndexedFiles(studyId, Collections.singletonList(fileId));
            System.out.println("Registered file " + fileId + " with samples " + samplesInFile);
        }

        metadataManager.updateStudyMetadata(studyId, studyMetadata -> {
            studyMetadata.getAttributes().put(VariantStorageOptions.LOADED_GENOTYPES.key(), "0/1,1/1,?/?");
            studyMetadata.getAttributes().put(MongoDBVariantStorageOptions.DEFAULT_GENOTYPE.key(), "0/0");
            return studyMetadata;
        });

        return sm;
    }

    @Test
    public void testQuerySampleAddFile() {
        Bson mongoQuery = parser.parseQuery(new Query().append(STUDY.key(), "study_1").append(SAMPLE.key(), "sample_10101"));

        Document expected = new Document(STUDIES_FIELD + '.' + STUDYID_FIELD, 1)
                .append("$and", Collections.singletonList(
                        new Document("$or", Arrays.asList(
                                new Document(STUDIES_FIELD + '.' + GENOTYPES_FIELD + ".0/1", sample_10101_id),
                                new Document(STUDIES_FIELD + '.' + GENOTYPES_FIELD + ".1/1", sample_10101_id)))))
                .append(STUDIES_FIELD + '.' + FILES_FIELD + '.' + FILEID_FIELD, file_1_id);

        checkEqualDocuments(expected, mongoQuery);
    }

    @Test
    public void testQuerySampleNotAddFile() {
        // In Study2 all the files have the same samples.
        // See #641
        Bson mongoQuery = parser.parseQuery(new Query().append(STUDY.key(), "study_2").append(SAMPLE.key(), "sample_20001"));

        Document expected = new Document(STUDIES_FIELD + '.' + STUDYID_FIELD, 2)
                .append("$and", Collections.singletonList(
                        new Document("$or", Arrays.asList(
                                new Document(STUDIES_FIELD + '.' + GENOTYPES_FIELD + ".0/1", 20001),
                                new Document(STUDIES_FIELD + '.' + GENOTYPES_FIELD + ".1/1", 20001)))));

        checkEqualDocuments(expected, mongoQuery);
    }

    @Test
    public void testQuerySampleAddMultipleFile() throws StorageEngineException {
        int studyId = metadataManager.getStudyId("study_2");
        metadataManager.updateFileMetadata(studyId, file_3_id, fileMetadata -> {
            fileMetadata.setSamples(new LinkedHashSet<>());
        });
        metadataManager.addIndexedFiles(studyId, Collections.singletonList(file_3_id));

        // Now, in Study2, not all the files have the same samples, so the parser must add fileId : $in:[1,2,3]
        // Improvement to #641
        Bson mongoQuery = parser.parseQuery(new Query().append(STUDY.key(), "study_2").append(SAMPLE.key(), "sample_20001"));

        Document expected = new Document(STUDIES_FIELD + '.' + STUDYID_FIELD, 2)
                .append("$and", Collections.singletonList(
                        new Document("$or", Arrays.asList(
                                new Document(STUDIES_FIELD + '.' + GENOTYPES_FIELD + ".0/1", 20001),
                                new Document(STUDIES_FIELD + '.' + GENOTYPES_FIELD + ".1/1", 20001)))))
                .append(STUDIES_FIELD + '.' + FILES_FIELD + '.' + FILEID_FIELD, new Document("$in", Arrays.asList(file_1_id, 100000, file_2_id)));

        checkEqualDocuments(expected, mongoQuery);
    }

    @Test
    public void testQuerySampleAddMultipleFileOr() throws StorageEngineException {

        int studyId = metadataManager.getStudyId("study_2");
        DummyStudyMetadataDBAdaptor.SAMPLE_METADATA_MAP.get(studyId).remove(20004); // Replace SampleID
        DummyStudyMetadataDBAdaptor.SAMPLE_METADATA_MAP.get(studyId).remove(20005); // Replace SampleID
        DummyStudyMetadataDBAdaptor.SAMPLE_METADATA_MAP.get(studyId).remove(20006); // Replace SampleID
        metadataManager.unsecureUpdateSampleMetadata(studyId, new SampleMetadata(studyId, 4, "sample_20004"));
        metadataManager.unsecureUpdateSampleMetadata(studyId, new SampleMetadata(studyId, 5, "sample_20005"));
        metadataManager.unsecureUpdateSampleMetadata(studyId, new SampleMetadata(studyId, 6, "sample_20006"));
//        metadataManager.registerSamples(studyId, Arrays.asList("sample_20004", "sample_20005", "sample_20006"));
        metadataManager.registerFileSamples(studyId, 4, Arrays.asList("sample_20004", "sample_20005", "sample_20006"));
        metadataManager.registerFile(studyId, "file_5", Arrays.asList("sample_20004", "sample_20005", "sample_20006"));
        metadataManager.addIndexedFiles(studyId, Collections.singletonList(4));
        metadataManager.addIndexedFiles(studyId, Collections.singletonList(5));

        metadataManager.registerFile(studyId, "file_10", Collections.singletonList("sample_20010"));

        // Now, in Study2, not all the files have the same samples, so the parser must add fileId : $in:[1,2,3]
        // Improvement to #641
        Bson mongoQuery = parser.parseQuery(new Query().append(STUDY.key(), "study_2").append(GENOTYPE.key(), "sample_20001:0/1,sample_20004:0/1"));

        Document expected = new Document(STUDIES_FIELD + '.' + STUDYID_FIELD, 2)
                .append("$or", Arrays.asList(
                        new Document("$or", Arrays.asList(new Document(STUDIES_FIELD + '.' + GENOTYPES_FIELD + ".0/1", 4)))
                                .append(STUDIES_FIELD + '.' + FILES_FIELD + '.' + FILEID_FIELD, new Document("$in", Arrays.asList(4, 5))),
                        new Document("$or", Arrays.asList(new Document(STUDIES_FIELD + '.' + GENOTYPES_FIELD + ".0/1", 20001)))
                                .append(STUDIES_FIELD + '.' + FILES_FIELD + '.' + FILEID_FIELD, new Document("$in", Arrays.asList(1, 2, 3)))
                ));

        checkEqualDocuments(expected, mongoQuery);
    }

    @Test
    public void testQuerySampleAddMultipleFileAnd() throws StorageEngineException {
        int studyId = metadataManager.getStudyId("study_2");
        DummyStudyMetadataDBAdaptor.SAMPLE_METADATA_MAP.get(studyId).remove(20004); // Replace SampleID
        DummyStudyMetadataDBAdaptor.SAMPLE_METADATA_MAP.get(studyId).remove(20005); // Replace SampleID
        DummyStudyMetadataDBAdaptor.SAMPLE_METADATA_MAP.get(studyId).remove(20006); // Replace SampleID
        metadataManager.unsecureUpdateSampleMetadata(studyId, new SampleMetadata(studyId, 4, "sample_20004"));
        metadataManager.unsecureUpdateSampleMetadata(studyId, new SampleMetadata(studyId, 5, "sample_20005"));
        metadataManager.unsecureUpdateSampleMetadata(studyId, new SampleMetadata(studyId, 6, "sample_20006"));
//        metadataManager.registerSamples(studyId, Arrays.asList("sample_20004", "sample_20005", "sample_20006"));
        int file4 = metadataManager.getFileIdOrFail(studyId, "file_4");
        metadataManager.registerFileSamples(studyId, file4, Arrays.asList("sample_20004", "sample_20005", "sample_20006"));
        metadataManager.registerFile(studyId, "file_5", Arrays.asList("sample_20004", "sample_20005", "sample_20006"));
        int file5 = metadataManager.getFileIdOrFail(studyId, "file_5");
        metadataManager.addIndexedFiles(studyId, Collections.singletonList(file4));
        metadataManager.addIndexedFiles(studyId, Collections.singletonList(file5));

        metadataManager.registerFile(studyId, "file_10", Collections.singletonList("sample_20010"));

        // Now, in Study2, not all the files have the same samples, so the parser must add fileId : $in:[1,2,3]
        // Improvement to #641
        Bson mongoQuery = parser.parseQuery(new Query().append(STUDY.key(), "study_2").append(GENOTYPE.key(), "sample_20001:0/1;sample_20004:0/1"));

        Document expected = new Document(STUDIES_FIELD + '.' + STUDYID_FIELD, 2)
                .append("$and", Arrays.asList(
                        new Document("$or", Arrays.asList(new Document(STUDIES_FIELD + '.' + GENOTYPES_FIELD + ".0/1", 4))),
                        new Document("$or", Arrays.asList(new Document(STUDIES_FIELD + '.' + GENOTYPES_FIELD + ".0/1", 20001))),
                        new Document(STUDIES_FIELD + '.' + FILES_FIELD + '.' + FILEID_FIELD, new Document("$in", Arrays.asList(1, 2, 3))),
                        new Document(STUDIES_FIELD + '.' + FILES_FIELD + '.' + FILEID_FIELD, new Document("$in", Arrays.asList(4, 5)))
                ));

        checkEqualDocuments(expected, mongoQuery);
    }

    @Test
    public void testQuerySamplesAddFile() {
        // Filter samples from same file
        Bson mongoQuery = parser.parseQuery(new Query().append(STUDY.key(), "study_1")
                .append(SAMPLE.key(), "sample_10101;sample_10102"));

        Document expected = new Document(STUDIES_FIELD + '.' + STUDYID_FIELD, 1)
                .append("$and", Arrays.asList(
                        new Document("$or", Arrays.asList(
                                new Document(STUDIES_FIELD + '.' + GENOTYPES_FIELD + ".0/1", sample_10101_id),
                                new Document(STUDIES_FIELD + '.' + GENOTYPES_FIELD + ".1/1", sample_10101_id))),
                        new Document("$or", Arrays.asList(
                                new Document(STUDIES_FIELD + '.' + GENOTYPES_FIELD + ".0/1", 10102),
                                new Document(STUDIES_FIELD + '.' + GENOTYPES_FIELD + ".1/1", 10102)))))
                .append(STUDIES_FIELD + '.' + FILES_FIELD + '.' + FILEID_FIELD, file_1_id);

        checkEqualDocuments(expected, mongoQuery);
    }

    @Test
    public void testQuerySamplesANDFileANDAddFile() {
        // Filter samples from same file
        Bson mongoQuery = parser.parseQuery(new Query().append(STUDY.key(), "study_1")
                .append(FILE.key(), "file_1;file_3")
                .append(SAMPLE.key(), "sample_10101;sample_10201"));

        Document expected = new Document(STUDIES_FIELD + '.' + STUDYID_FIELD, 1)
                .append("$and", Arrays.asList(
                        new Document("$or", Arrays.asList(
                                new Document(STUDIES_FIELD + '.' + GENOTYPES_FIELD + ".0/1", sample_10101_id),
                                new Document(STUDIES_FIELD + '.' + GENOTYPES_FIELD + ".1/1", sample_10101_id))),
                        new Document("$or", Arrays.asList(
                                new Document(STUDIES_FIELD + '.' + GENOTYPES_FIELD + ".0/1", sample_10201_id),
                                new Document(STUDIES_FIELD + '.' + GENOTYPES_FIELD + ".1/1", sample_10201_id)))
                ))
                .append(STUDIES_FIELD + '.' + FILES_FIELD + '.' + FILEID_FIELD, new Document("$all", Arrays.asList(1, 2, 3)));

        checkEqualDocuments(expected, mongoQuery);
    }

    @Test
    public void testQuerySamplesANDFileORAddFile() {
        // Filter samples from same file
        Bson mongoQuery = parser.parseQuery(new Query().append(STUDY.key(), "study_1")
                .append(FILE.key(), "file_1,file_3")
                .append(SAMPLE.key(), "sample_10101;sample_10201"));

        Document expected = new Document(STUDIES_FIELD + '.' + STUDYID_FIELD, 1)
                .append("$and", Arrays.asList(
                        new Document("$or", Arrays.asList(
                                new Document(STUDIES_FIELD + '.' + GENOTYPES_FIELD + ".0/1", sample_10101_id),
                                new Document(STUDIES_FIELD + '.' + GENOTYPES_FIELD + ".1/1", sample_10101_id))),
                        new Document("$or", Arrays.asList(
                                new Document(STUDIES_FIELD + '.' + GENOTYPES_FIELD + ".0/1", sample_10201_id),
                                new Document(STUDIES_FIELD + '.' + GENOTYPES_FIELD + ".1/1", sample_10201_id))),
                        new Document(STUDIES_FIELD + '.' + FILES_FIELD + '.' + FILEID_FIELD, new Document("$in", Arrays.asList(1, 3))),
                        new Document(STUDIES_FIELD + '.' + FILES_FIELD + '.' + FILEID_FIELD, new Document("$all", Arrays.asList(1, 2)))
                ));

        checkEqualDocuments(expected, mongoQuery);
    }

    @Test
    public void testQuerySamplesANDFileNegatedAddFile() {
        // Filter samples from same file
        Bson mongoQuery = parser.parseQuery(new Query().append(STUDY.key(), "study_1")
                .append(FILE.key(), "!file_3;!file_4")
                .append(SAMPLE.key(), "sample_10101;sample_10201"));

        Document expected = new Document(STUDIES_FIELD + '.' + STUDYID_FIELD, 1)
                .append("$and", Arrays.asList(
                        new Document("$or", Arrays.asList(
                                new Document(STUDIES_FIELD + '.' + GENOTYPES_FIELD + ".0/1", sample_10101_id),
                                new Document(STUDIES_FIELD + '.' + GENOTYPES_FIELD + ".1/1", sample_10101_id))),
                        new Document("$or", Arrays.asList(
                                new Document(STUDIES_FIELD + '.' + GENOTYPES_FIELD + ".0/1", sample_10201_id),
                                new Document(STUDIES_FIELD + '.' + GENOTYPES_FIELD + ".1/1", sample_10201_id))),
                        new Document(STUDIES_FIELD + '.' + FILES_FIELD + '.' + FILEID_FIELD, new Document("$all", Arrays.asList(1, 2))),
                        new Document(STUDIES_FIELD + '.' + FILES_FIELD + '.' + FILEID_FIELD, new Document("$nin", Arrays.asList(3, 4)))
                ));

        checkEqualDocuments(expected, mongoQuery);
    }

    @Test
    public void testQuerySamplesORFileANDAddFile() {
        // Filter samples from same file
        Bson mongoQuery = parser.parseQuery(new Query().append(STUDY.key(), "study_1")
                .append(FILE.key(), "file_1;file_3")
                .append(SAMPLE.key(), "sample_10101,sample_10201"));

        Document expected = new Document(STUDIES_FIELD + '.' + STUDYID_FIELD, 1)
                .append("$or", Arrays.asList(
                        new Document("$or", Arrays.asList(
                                new Document(STUDIES_FIELD + '.' + GENOTYPES_FIELD + ".0/1", sample_10101_id),
                                new Document(STUDIES_FIELD + '.' + GENOTYPES_FIELD + ".1/1", sample_10101_id)))
                                .append(STUDIES_FIELD + '.' + FILES_FIELD + '.' + FILEID_FIELD, file_1_id),
                        new Document("$or", Arrays.asList(
                                new Document(STUDIES_FIELD + '.' + GENOTYPES_FIELD + ".0/1", sample_10201_id),
                                new Document(STUDIES_FIELD + '.' + GENOTYPES_FIELD + ".1/1", sample_10201_id)))
                                .append(STUDIES_FIELD + '.' + FILES_FIELD + '.' + FILEID_FIELD, file_2_id)
                ))
                .append(STUDIES_FIELD + '.' + FILES_FIELD + '.' + FILEID_FIELD, new Document("$all", Arrays.asList(file_1_id, file_3_id)));

        checkEqualDocuments(expected, mongoQuery);
    }

    @Test
    public void testQuerySamplesORFileORAddFile() {
        // Filter samples from same file
        Bson mongoQuery = parser.parseQuery(new Query().append(STUDY.key(), "study_1")
                .append(FILE.key(), "file_1,file_3")
                .append(SAMPLE.key(), "sample_10101,sample_10201"));

        Document expected = new Document(STUDIES_FIELD + '.' + STUDYID_FIELD, 1)
                .append("$or", Arrays.asList(
                        new Document("$or", Arrays.asList(
                                new Document(STUDIES_FIELD + '.' + GENOTYPES_FIELD + ".0/1", sample_10101_id),
                                new Document(STUDIES_FIELD + '.' + GENOTYPES_FIELD + ".1/1", sample_10101_id)))
                                .append(STUDIES_FIELD + '.' + FILES_FIELD + '.' + FILEID_FIELD, file_1_id),
                        new Document("$or", Arrays.asList(
                                new Document(STUDIES_FIELD + '.' + GENOTYPES_FIELD + ".0/1", sample_10201_id),
                                new Document(STUDIES_FIELD + '.' + GENOTYPES_FIELD + ".1/1", sample_10201_id)))
                                .append(STUDIES_FIELD + '.' + FILES_FIELD + '.' + FILEID_FIELD, file_2_id)))
                .append(STUDIES_FIELD + '.' + FILES_FIELD + '.' + FILEID_FIELD, new Document("$in", Arrays.asList(file_1_id, file_3_id)));

        checkEqualDocuments(expected, mongoQuery);
    }

    @Test
    public void testQuerySamplesORFileNegatedAddFile() {
        // Filter samples from same file
        Bson mongoQuery = parser.parseQuery(new Query().append(STUDY.key(), "study_1")
                .append(FILE.key(), "!file_3;!file_4")
                .append(SAMPLE.key(), "sample_10101,sample_10201"));

        Document expected = new Document(STUDIES_FIELD + '.' + STUDYID_FIELD, 1)
                .append("$or", Arrays.asList(
                        new Document("$or", Arrays.asList(
                                new Document(STUDIES_FIELD + '.' + GENOTYPES_FIELD + ".0/1", sample_10101_id),
                                new Document(STUDIES_FIELD + '.' + GENOTYPES_FIELD + ".1/1", sample_10101_id)))
                                .append(STUDIES_FIELD + '.' + FILES_FIELD + '.' + FILEID_FIELD, file_1_id),
                        new Document("$or", Arrays.asList(
                                new Document(STUDIES_FIELD + '.' + GENOTYPES_FIELD + ".0/1", sample_10201_id),
                                new Document(STUDIES_FIELD + '.' + GENOTYPES_FIELD + ".1/1", sample_10201_id)))
                                .append(STUDIES_FIELD + '.' + FILES_FIELD + '.' + FILEID_FIELD, file_2_id)))
                .append(STUDIES_FIELD + '.' + FILES_FIELD + '.' + FILEID_FIELD, new Document("$nin", Arrays.asList(file_3_id, file_4_id)));

        checkEqualDocuments(expected, mongoQuery);
    }

    @Test
    public void testQuerySamplesAddFiles() {
        // Filter samples from different files
        Bson mongoQuery = parser.parseQuery(new Query().append(STUDY.key(), "study_1")
                .append(SAMPLE.key(), "sample_10101;sample_10201"));

        Document expected = new Document(STUDIES_FIELD + '.' + STUDYID_FIELD, 1)
                .append("$and", Arrays.asList(
                        new Document("$or", Arrays.asList(
                                new Document(STUDIES_FIELD + '.' + GENOTYPES_FIELD + ".0/1", sample_10101_id),
                                new Document(STUDIES_FIELD + '.' + GENOTYPES_FIELD + ".1/1", sample_10101_id))),
                        new Document("$or", Arrays.asList(
                                new Document(STUDIES_FIELD + '.' + GENOTYPES_FIELD + ".0/1", sample_10201_id),
                                new Document(STUDIES_FIELD + '.' + GENOTYPES_FIELD + ".1/1", sample_10201_id)))))
                .append(STUDIES_FIELD + '.' + FILES_FIELD + '.' + FILEID_FIELD, new Document("$all", Arrays.asList(file_1_id, file_2_id)));

        checkEqualDocuments(expected, mongoQuery);
    }

    @Test
    public void testQueryAnySamplesFromAllFiles() {
        // If filter by any samples from all files, don't add the files.fid filter
        Bson mongoQuery = parser.parseQuery(new Query().append(STUDY.key(), "study_1")
                .append(SAMPLE.key(), "sample_10101,sample_10201,sample_10301"));

        Document expected = new Document(STUDIES_FIELD + '.' + STUDYID_FIELD, 1)
                .append("$or", Arrays.asList(
                        new Document("$or", Arrays.asList(
                                new Document(STUDIES_FIELD + '.' + GENOTYPES_FIELD + ".0/1", sample_10101_id),
                                new Document(STUDIES_FIELD + '.' + GENOTYPES_FIELD + ".1/1", sample_10101_id)))
                                .append(STUDIES_FIELD + '.' + FILES_FIELD + '.' + FILEID_FIELD, file_1_id),
                        new Document("$or", Arrays.asList(
                                new Document(STUDIES_FIELD + '.' + GENOTYPES_FIELD + ".0/1", sample_10201_id),
                                new Document(STUDIES_FIELD + '.' + GENOTYPES_FIELD + ".1/1", sample_10201_id)))
                                .append(STUDIES_FIELD + '.' + FILES_FIELD + '.' + FILEID_FIELD, file_2_id),
                        new Document("$or", Arrays.asList(
                                new Document(STUDIES_FIELD + '.' + GENOTYPES_FIELD + ".0/1", sample_10301_id),
                                new Document(STUDIES_FIELD + '.' + GENOTYPES_FIELD + ".1/1", sample_10301_id)))
                                .append(STUDIES_FIELD + '.' + FILES_FIELD + '.' + FILEID_FIELD, file_3_id))
                );

        checkEqualDocuments(expected, mongoQuery);
    }

    @Test
    public void testQueryAllSamplesFromAllFiles2() {
        // If filter by ALL samples from ALL files, add the files.fid filter with $all
        Bson mongoQuery = parser.parseQuery(new Query().append(STUDY.key(), "study_1")
                .append(SAMPLE.key(), "sample_10101;sample_10201;sample_10301"));

        Document expected = new Document(STUDIES_FIELD + '.' + STUDYID_FIELD, 1)
                .append("$and", Arrays.asList(
                        new Document("$or", Arrays.asList(
                                new Document(STUDIES_FIELD + '.' + GENOTYPES_FIELD + ".0/1", sample_10101_id),
                                new Document(STUDIES_FIELD + '.' + GENOTYPES_FIELD + ".1/1", sample_10101_id))),
                        new Document("$or", Arrays.asList(
                                new Document(STUDIES_FIELD + '.' + GENOTYPES_FIELD + ".0/1", sample_10201_id),
                                new Document(STUDIES_FIELD + '.' + GENOTYPES_FIELD + ".1/1", sample_10201_id))),
                        new Document("$or", Arrays.asList(
                                new Document(STUDIES_FIELD + '.' + GENOTYPES_FIELD + ".0/1", sample_10301_id),
                                new Document(STUDIES_FIELD + '.' + GENOTYPES_FIELD + ".1/1", sample_10301_id))))
                )
                .append(STUDIES_FIELD + '.' + FILES_FIELD + '.' + FILEID_FIELD, new Document("$all", Arrays.asList(file_1_id, file_2_id, file_3_id)));

        checkEqualDocuments(expected, mongoQuery);
    }

    @Test
    public void testQueryGenotypesAddFiles() {
        Bson mongoQuery = parser.parseQuery(new Query().append(STUDY.key(), "study_1").append(GENOTYPE.key(), "sample_10101" + IS + "0/1"));

        Document expected = new Document(STUDIES_FIELD + '.' + STUDYID_FIELD, 1)
                .append("$and", Collections.singletonList(
                        new Document("$or", Arrays.asList(
                                new Document(STUDIES_FIELD + '.' + GENOTYPES_FIELD + ".0/1", sample_10101_id)))))
                .append(STUDIES_FIELD + '.' + FILES_FIELD + '.' + FILEID_FIELD, 1);

        checkEqualDocuments(expected, mongoQuery);
    }

    @Test
    public void testQueryAnySample() {
        // Filter samples from different files
        Bson mongoQuery = parser.parseQuery(new Query().append(STUDY.key(), "study_1")
                .append(SAMPLE.key(), "sample_10101,sample_10201"));

        Document expected = new Document(STUDIES_FIELD + '.' + STUDYID_FIELD, 1)
                .append("$or", Arrays.asList(
                        new Document("$or", Arrays.asList(
                                new Document(STUDIES_FIELD + '.' + GENOTYPES_FIELD + ".0/1", sample_10101_id),
                                new Document(STUDIES_FIELD + '.' + GENOTYPES_FIELD + ".1/1", sample_10101_id)))
                                .append(STUDIES_FIELD + '.' + FILES_FIELD + '.' + FILEID_FIELD, file_1_id),
                        new Document("$or", Arrays.asList(
                                new Document(STUDIES_FIELD + '.' + GENOTYPES_FIELD + ".0/1", sample_10201_id),
                                new Document(STUDIES_FIELD + '.' + GENOTYPES_FIELD + ".1/1", sample_10201_id)))
                                .append(STUDIES_FIELD + '.' + FILES_FIELD + '.' + FILEID_FIELD, file_2_id)));

        checkEqualDocuments(expected, mongoQuery);
    }

    @Test
    public void testQueryDefaultGenotypesNotAddFiles() {
        // FILES filter should not be used when the genotype filter is the default genotype
        Bson mongoQuery = parser.parseQuery(new Query()
                .append(STUDY.key(), "study_1")
                .append(GENOTYPE.key(), "sample_10101" + IS + "0/0" + OR + "1/1"));


        Document expected = new Document(STUDIES_FIELD + '.' + STUDYID_FIELD, 1)
                .append("$and", Collections.singletonList(
                        new Document("$or", Arrays.asList(
                                new Document("$and", Arrays.asList(
                                        ANY, // $ne 0/1
                                        ANY, // $ne 1/1
                                        ANY  // $ne ?/?
                                )),
                                new Document(STUDIES_FIELD + '.' + GENOTYPES_FIELD + ".1/1", sample_10101_id) // $eq 1/1
                        ))))
                .append(STUDIES_FIELD + '.' + FILES_FIELD + '.' + FILEID_FIELD, 1);
        checkEqualDocuments(expected, mongoQuery);
    }

    @Test
    public void testQueryUnknownGenotypesNotAddFiles() {
        // FILES filter should not be used when the genotype filter is the unknown genotype
        Bson mongoQuery = parser.parseQuery(new Query().append(STUDY.key(), "study_1").append(GENOTYPE.key(), "sample_10101" + IS + GenotypeClass.UNKNOWN_GENOTYPE));

        Document expected = new Document(STUDIES_FIELD + '.' + STUDYID_FIELD, 1)
                .append("$and", Collections.singletonList(
                        new Document("$or", Arrays.asList(
                                new Document(STUDIES_FIELD + '.' + GENOTYPES_FIELD + ".?/?", sample_10101_id)))));

        checkEqualDocuments(expected, mongoQuery);
    }

    @Test
    public void testQueryNegatedGenotypes() {
        Bson mongoQuery = parser.parseQuery(new Query().append(STUDY.key(), "study_1")
                .append(GENOTYPE.key(), "sample_10101" + IS + NOT + "1/1"));

        Document expected = new Document(STUDIES_FIELD + '.' + STUDYID_FIELD, 1)
                .append("$and", Collections.singletonList(ANY));

        checkEqualDocuments(expected, mongoQuery);
    }

    @Test
    public void testProjectionIncludeSamplesExcludeFiles() {
        Document expected = new Document()
                .append(CHROMOSOME_FIELD, 1)
                .append(START_FIELD, 1)
                .append(END_FIELD, 1)
//                .append(LENGTH_FIELD, 1)
                .append(REFERENCE_FIELD, 1)
                .append(ALTERNATE_FIELD, 1)
//                .append(IDS_FIELD, 1)
                .append(TYPE_FIELD, 1)
                .append(SV_FIELD, 1)
//                .append(HGVS_FIELD, 1)
//                .append(ANNOTATION_FIELD, 1)
//                .append(CUSTOM_ANNOTATION_FIELD, 1)
//                .append(RELEASE_FIELD, 1)
                .append(STATS_FIELD, 1)
                .append(STUDIES_FIELD + '.' + STUDYID_FIELD, 1)
                .append(STUDIES_FIELD + '.' + ALTERNATES_FIELD, 1)
                .append(STUDIES_FIELD + '.' + GENOTYPES_FIELD, 1)
                .append(STUDIES_FIELD + '.' + FILES_FIELD + '.' + FILEID_FIELD, 1) // Ensure that fileId is always returned
                .append(STUDIES_FIELD + '.' + FILES_FIELD + '.' + SAMPLE_DATA_FIELD, 1);

        Bson projection = parser.createProjection(new VariantQuery().includeSampleAll(), new QueryOptions(QueryOptions.INCLUDE,
                STUDIES_SAMPLES + "," + STUDIES_SECONDARY_ALTERNATES + ',' + STUDIES_STATS));
        checkEqualDocuments(expected, projection, false);

        projection = parser.createProjection(new VariantQuery()
                .includeSampleAll()
                .includeGenotype(true)
                .includeSampleData(ALL)
                .includeFileNone(), new QueryOptions(QueryOptions.INCLUDE, STUDIES.fieldName()));
        checkEqualDocuments(expected, projection, false);

        expected.append(LENGTH_FIELD, 1);
        expected.append(IDS_FIELD, 1);
        projection = parser.createProjection(new Query().append(INCLUDE_SAMPLE.key(), ALL)
                .append(INCLUDE_FILE.key(), NONE), new QueryOptions(QueryOptions.EXCLUDE, ANNOTATION.fieldName()));
        checkEqualDocuments(expected, projection, false);

        expected.remove(STUDIES_FIELD + '.' + FILES_FIELD + '.' + SAMPLE_DATA_FIELD);
        projection = parser.createProjection(new Query().append(INCLUDE_SAMPLE.key(), ALL)
                .append(INCLUDE_FILE.key(), NONE).append(INCLUDE_GENOTYPE.key(), true), new QueryOptions(QueryOptions.EXCLUDE, ANNOTATION.fieldName()));
        checkEqualDocuments(expected, projection, false);

    }

}