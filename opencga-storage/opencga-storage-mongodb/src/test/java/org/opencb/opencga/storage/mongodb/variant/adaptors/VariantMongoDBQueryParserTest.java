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
import org.opencb.opencga.storage.core.variant.adaptors.VariantQuery;
import org.opencb.opencga.storage.core.variant.dummy.DummyVariantStorageEngine;
import org.opencb.opencga.storage.core.variant.dummy.DummyVariantStorageMetadataDBAdaptorFactory;
import org.opencb.opencga.storage.core.variant.query.ParsedVariantQuery;
import org.opencb.opencga.storage.core.variant.query.VariantQueryParser;
import org.opencb.opencga.storage.mongodb.variant.MongoDBVariantStorageOptions;
import org.opencb.opencga.storage.mongodb.variant.converters.DocumentToVariantConverter;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.junit.Assert.*;
import static org.opencb.opencga.storage.core.variant.adaptors.VariantField.*;
import static org.opencb.opencga.storage.core.variant.adaptors.VariantQueryParam.*;
import static org.opencb.opencga.storage.core.variant.query.VariantQueryUtils.*;
import static org.opencb.opencga.storage.mongodb.variant.converters.DocumentToStudyEntryConverter.*;
import static org.opencb.opencga.storage.mongodb.variant.converters.DocumentToStudyEntryConverter.FILES_FIELD;
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
    public static final int sample_10102_id = 10102;
    public static final int sample_20001_id = 20001;
    public static final int sample_20002_id = 20002;
    public static final int sample_20003_id = 20003;
    public static final int sample_20004_id = 20004;
    public static final int file_0_id = 100000;
    public static final int file_1_id = 100001;
    public static final int file_2_id = 100002;
    public static final int file_3_id = 100003;
    public static final int file_4_id = 100004;
    private VariantMongoDBQueryParser parser;
    private VariantStorageMetadataManager metadataManager;

    @Before
    public void setUp() throws Exception {
        DummyVariantStorageEngine.clear();
        metadataManager = new VariantStorageMetadataManager(new DummyVariantStorageMetadataDBAdaptorFactory());
        parser = new VariantMongoDBQueryParser(metadataManager);

        newStudy("study_1", 4, false);
        newStudy("study_2", 4, true);
    }

    @After
    public void tearDown() throws Exception {
        DummyVariantStorageEngine.clear();
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
                Integer sampleIdActual = metadataManager.getSampleId(studyId, name);
                if (sampleIdActual == null) {
//                    System.out.println("Registering sample " + name + " in file " + fileId + " of study " + studyName);
                    metadataManager.unsecureUpdateSampleMetadata(studyId, new SampleMetadata(studyId, sampleId, name));
                } else if (sampleIdActual != sampleId) {
                    throw new IllegalStateException("Sample name " + name + " already exists with a different ID: " + sampleIdActual
                            + " != " + sampleId);
                }
                samplesInFile.add(name);
            }
            metadataManager.registerFileSamples(studyId, fileId, samplesInFile);
//            metadataManager.updateFileMetadata(studyId, fileId, fileMetadata -> fileMetadata.setSamples(samplesInFile));
            metadataManager.addIndexedFiles(studyId, Collections.singletonList(fileId));
//            System.out.println("Registered file " + fileId + " with samples " + samplesInFile);
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
                                new Document(FILES_FIELD + '.' + FILE_GENOTYPE_FIELD + ".0/1", sample_10101_id),
                                new Document(FILES_FIELD + '.' + FILE_GENOTYPE_FIELD + ".1/1", sample_10101_id)))))
                .append(FILES_FIELD + '.' + FILEID_FIELD, file_1_id);

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
                                new Document(FILES_FIELD + '.' + FILE_GENOTYPE_FIELD + ".0/1", 20001),
                                new Document(FILES_FIELD + '.' + FILE_GENOTYPE_FIELD + ".1/1", 20001)))));

        checkEqualDocuments(expected, mongoQuery);
    }

    @Test
    public void testQuerySampleAddMultipleFile() throws StorageEngineException {
        int studyId = metadataManager.getStudyId("study_2");
        int newFileId = metadataManager.registerFile(studyId, "file_n", Collections.emptyList());
        metadataManager.addIndexedFiles(studyId, Collections.singletonList(newFileId));
        metadataManager.removeIndexedFiles(studyId, Arrays.asList(file_3_id, file_4_id));

        // Now, in Study2, not all the files have the same samples, so the parser must add fileId : $in:[1,2,3]
        // Improvement to #641
        Bson mongoQuery = parser.parseQuery(new Query().append(STUDY.key(), "study_2").append(SAMPLE.key(), "sample_20001"));

        Document expected = new Document(STUDIES_FIELD + '.' + STUDYID_FIELD, studyId)
                .append("$and", Collections.singletonList(
                        new Document("$or", Arrays.asList(
                                new Document(FILES_FIELD + '.' + FILE_GENOTYPE_FIELD + ".0/1", 20001),
                                new Document(FILES_FIELD + '.' + FILE_GENOTYPE_FIELD + ".1/1", 20001)))))
                .append(FILES_FIELD + '.' + FILEID_FIELD, new Document("$in", Arrays.asList(file_0_id, file_1_id, file_2_id)));

        checkEqualDocuments(expected, mongoQuery);
    }

    @Test
    public void testQuerySampleAddMultipleFileOr() throws StorageEngineException {

        int studyId = metadataManager.getStudyId("study_2");
        int file5 = metadataManager.registerFile(studyId, "file_5", Arrays.asList("sample_2A", "sample_2B", "sample_2C"));
        int file6 = metadataManager.registerFile(studyId, "file_6", Arrays.asList("sample_2A", "sample_2B", "sample_2C"));
        metadataManager.addIndexedFiles(studyId, Collections.singletonList(file5));
        metadataManager.addIndexedFiles(studyId, Collections.singletonList(file6));
        Integer sample2A = metadataManager.getSampleId(studyId, "sample_2A");
        metadataManager.registerFile(studyId, "file_10", Collections.singletonList("sample_20010"));

        metadataManager.registerFile(studyId, "file_10", Collections.singletonList("sample_20010"));

        // Now, in Study2, not all the files have the same samples, so the parser must add fileId : $in:[1,2,3]
        // Improvement to #641
        Bson mongoQuery = parser.parseQuery(new Query().append(STUDY.key(), "study_2").append(GENOTYPE.key(), "sample_20001:0/1,sample_2A:0/1"));

        Document expected = new Document(STUDIES_FIELD + '.' + STUDYID_FIELD, 2)
                .append("$or", Arrays.asList(
                        new Document("$or", Arrays.asList(new Document(FILES_FIELD + '.' + FILE_GENOTYPE_FIELD + ".0/1", sample_20001_id)))
                                .append(FILES_FIELD + '.' + FILEID_FIELD, new Document("$in", Arrays.asList(file_0_id, file_1_id, file_2_id, file_3_id, file_4_id))),
                        new Document("$or", Arrays.asList(new Document(FILES_FIELD + '.' + FILE_GENOTYPE_FIELD + ".0/1", sample2A)))
                                .append(FILES_FIELD + '.' + FILEID_FIELD, new Document("$in", Arrays.asList(file5, file6)))
                        ));

        checkEqualDocuments(expected, mongoQuery);
    }

    @Test
    public void testQuerySampleAddMultipleFileAnd() throws StorageEngineException {
        int studyId = metadataManager.getStudyId("study_2");
        int file5 = metadataManager.registerFile(studyId, "file_5", Arrays.asList("sample_2A", "sample_2B", "sample_2C"));
        int file6 = metadataManager.registerFile(studyId, "file_6", Arrays.asList("sample_2A", "sample_2B", "sample_2C"));
        metadataManager.addIndexedFiles(studyId, Collections.singletonList(file5));
        metadataManager.addIndexedFiles(studyId, Collections.singletonList(file6));
        Integer sample2A = metadataManager.getSampleId(studyId, "sample_2A");
        metadataManager.registerFile(studyId, "file_10", Collections.singletonList("sample_20010"));

        // Now, in Study2, not all the files have the same samples, so the parser must add fileId : $in:[1,2,3]
        // Improvement to #641
        Bson mongoQuery = parser.parseQuery(new Query().append(STUDY.key(), "study_2")
                .append(GENOTYPE.key(), "sample_20001:0/1;sample_2A:0/1"));

        Document expected = new Document("$and", Arrays.asList(
                new Document(STUDIES_FIELD + '.' + STUDYID_FIELD, studyId),
                new Document("$and", Arrays.asList(
                        new Document("$or", Arrays.asList(new Document(FILES_FIELD + '.' + FILE_GENOTYPE_FIELD + ".0/1", sample_20001_id))),
                        new Document("$or", Arrays.asList(new Document(FILES_FIELD + '.' + FILE_GENOTYPE_FIELD + ".0/1", sample2A)))
                )),
                new Document(FILES_FIELD + '.' + FILEID_FIELD, new Document("$in", Arrays.asList(file_0_id, file_1_id, file_2_id, file_3_id, file_4_id))),
                new Document(FILES_FIELD + '.' + FILEID_FIELD, new Document("$in", Arrays.asList(file5, file6)))
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
                                new Document(FILES_FIELD + '.' + FILE_GENOTYPE_FIELD + ".0/1", sample_10101_id),
                                new Document(FILES_FIELD + '.' + FILE_GENOTYPE_FIELD + ".1/1", sample_10101_id))),
                        new Document("$or", Arrays.asList(
                                new Document(FILES_FIELD + '.' + FILE_GENOTYPE_FIELD + ".0/1", sample_10102_id),
                                new Document(FILES_FIELD + '.' + FILE_GENOTYPE_FIELD + ".1/1", sample_10102_id)))))
                .append(FILES_FIELD + '.' + FILEID_FIELD, file_1_id);

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
                                new Document(FILES_FIELD + '.' + FILE_GENOTYPE_FIELD + ".0/1", sample_10101_id),
                                new Document(FILES_FIELD + '.' + FILE_GENOTYPE_FIELD + ".1/1", sample_10101_id))),
                        new Document("$or", Arrays.asList(
                                new Document(FILES_FIELD + '.' + FILE_GENOTYPE_FIELD + ".0/1", sample_10201_id),
                                new Document(FILES_FIELD + '.' + FILE_GENOTYPE_FIELD + ".1/1", sample_10201_id)))
                ))
                .append(FILES_FIELD + '.' + FILEID_FIELD, new Document("$all", Arrays.asList(file_1_id, file_2_id, file_3_id)));

        checkEqualDocuments(expected, mongoQuery);
    }

    @Test
    public void testQuerySamplesANDFileORAddFile() {
        // Filter samples from same file
        Bson mongoQuery = parser.parseQuery(new Query().append(STUDY.key(), "study_1")
                .append(FILE.key(), "file_1,file_3")
                .append(SAMPLE.key(), "sample_10101;sample_10201"));

        Document expected = new Document("$and", Arrays.asList(new Document(STUDIES_FIELD + '.' + STUDYID_FIELD, 1),
                new Document("$and", Arrays.asList(
                        new Document("$or", Arrays.asList(
                                new Document(FILES_FIELD + '.' + FILE_GENOTYPE_FIELD + ".0/1", sample_10101_id),
                                new Document(FILES_FIELD + '.' + FILE_GENOTYPE_FIELD + ".1/1", sample_10101_id))),
                        new Document("$or", Arrays.asList(
                                new Document(FILES_FIELD + '.' + FILE_GENOTYPE_FIELD + ".0/1", sample_10201_id),
                                new Document(FILES_FIELD + '.' + FILE_GENOTYPE_FIELD + ".1/1", sample_10201_id)))
                )),
                new Document(FILES_FIELD + '.' + FILEID_FIELD, new Document("$in", Arrays.asList(file_1_id, file_3_id))),
                new Document(FILES_FIELD + '.' + FILEID_FIELD, new Document("$all", Arrays.asList(file_1_id, file_2_id)))
        ));

        checkEqualDocuments(expected, mongoQuery);
    }

    @Test
    public void testQuerySamplesANDFileNegatedAddFile() {
        // Filter samples from same file
        Bson mongoQuery = parser.parseQuery(new Query().append(STUDY.key(), "study_1")
                .append(FILE.key(), "!file_3;!file_4")
                .append(SAMPLE.key(), "sample_10101;sample_10201"));

        Document expected = new Document("$and", Arrays.asList(
                new Document(STUDIES_FIELD + '.' + STUDYID_FIELD, 1),
                new Document("$and", Arrays.asList(
                        new Document("$or", Arrays.asList(
                                new Document(FILES_FIELD + '.' + FILE_GENOTYPE_FIELD + ".0/1", sample_10101_id),
                                new Document(FILES_FIELD + '.' + FILE_GENOTYPE_FIELD + ".1/1", sample_10101_id))),
                        new Document("$or", Arrays.asList(
                                new Document(FILES_FIELD + '.' + FILE_GENOTYPE_FIELD + ".0/1", sample_10201_id),
                                new Document(FILES_FIELD + '.' + FILE_GENOTYPE_FIELD + ".1/1", sample_10201_id)))
                )),
                new Document(FILES_FIELD + '.' + FILEID_FIELD, new Document("$all", Arrays.asList(file_1_id, file_2_id))),
                new Document(FILES_FIELD + '.' + FILEID_FIELD, new Document("$nin", Arrays.asList(file_3_id, file_4_id)))
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
                                new Document(FILES_FIELD + '.' + FILE_GENOTYPE_FIELD + ".0/1", sample_10101_id),
                                new Document(FILES_FIELD + '.' + FILE_GENOTYPE_FIELD + ".1/1", sample_10101_id)))
                                .append(FILES_FIELD + '.' + FILEID_FIELD, file_1_id),
                        new Document("$or", Arrays.asList(
                                new Document(FILES_FIELD + '.' + FILE_GENOTYPE_FIELD + ".0/1", sample_10201_id),
                                new Document(FILES_FIELD + '.' + FILE_GENOTYPE_FIELD + ".1/1", sample_10201_id)))
                                .append(FILES_FIELD + '.' + FILEID_FIELD, file_2_id)
                ))
                .append(FILES_FIELD + '.' + FILEID_FIELD, new Document("$all", Arrays.asList(file_1_id, file_3_id)));

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
                                new Document(FILES_FIELD + '.' + FILE_GENOTYPE_FIELD + ".0/1", sample_10101_id),
                                new Document(FILES_FIELD + '.' + FILE_GENOTYPE_FIELD + ".1/1", sample_10101_id)))
                                .append(FILES_FIELD + '.' + FILEID_FIELD, file_1_id),
                        new Document("$or", Arrays.asList(
                                new Document(FILES_FIELD + '.' + FILE_GENOTYPE_FIELD + ".0/1", sample_10201_id),
                                new Document(FILES_FIELD + '.' + FILE_GENOTYPE_FIELD + ".1/1", sample_10201_id)))
                                .append(FILES_FIELD + '.' + FILEID_FIELD, file_2_id)))
                .append(FILES_FIELD + '.' + FILEID_FIELD, new Document("$in", Arrays.asList(file_1_id, file_3_id)));

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
                                new Document(FILES_FIELD + '.' + FILE_GENOTYPE_FIELD + ".0/1", sample_10101_id),
                                new Document(FILES_FIELD + '.' + FILE_GENOTYPE_FIELD + ".1/1", sample_10101_id)))
                                .append(FILES_FIELD + '.' + FILEID_FIELD, file_1_id),
                        new Document("$or", Arrays.asList(
                                new Document(FILES_FIELD + '.' + FILE_GENOTYPE_FIELD + ".0/1", sample_10201_id),
                                new Document(FILES_FIELD + '.' + FILE_GENOTYPE_FIELD + ".1/1", sample_10201_id)))
                                .append(FILES_FIELD + '.' + FILEID_FIELD, file_2_id)))
                .append(FILES_FIELD + '.' + FILEID_FIELD, new Document("$nin", Arrays.asList(file_3_id, file_4_id)));

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
                                new Document(FILES_FIELD + '.' + FILE_GENOTYPE_FIELD + ".0/1", sample_10101_id),
                                new Document(FILES_FIELD + '.' + FILE_GENOTYPE_FIELD + ".1/1", sample_10101_id))),
                        new Document("$or", Arrays.asList(
                                new Document(FILES_FIELD + '.' + FILE_GENOTYPE_FIELD + ".0/1", sample_10201_id),
                                new Document(FILES_FIELD + '.' + FILE_GENOTYPE_FIELD + ".1/1", sample_10201_id)))))
                .append(FILES_FIELD + '.' + FILEID_FIELD, new Document("$all", Arrays.asList(file_1_id, file_2_id)));

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
                                new Document(FILES_FIELD + '.' + FILE_GENOTYPE_FIELD + ".0/1", sample_10101_id),
                                new Document(FILES_FIELD + '.' + FILE_GENOTYPE_FIELD + ".1/1", sample_10101_id)))
                                .append(FILES_FIELD + '.' + FILEID_FIELD, file_1_id),
                        new Document("$or", Arrays.asList(
                                new Document(FILES_FIELD + '.' + FILE_GENOTYPE_FIELD + ".0/1", sample_10201_id),
                                new Document(FILES_FIELD + '.' + FILE_GENOTYPE_FIELD + ".1/1", sample_10201_id)))
                                .append(FILES_FIELD + '.' + FILEID_FIELD, file_2_id),
                        new Document("$or", Arrays.asList(
                                new Document(FILES_FIELD + '.' + FILE_GENOTYPE_FIELD + ".0/1", sample_10301_id),
                                new Document(FILES_FIELD + '.' + FILE_GENOTYPE_FIELD + ".1/1", sample_10301_id)))
                                .append(FILES_FIELD + '.' + FILEID_FIELD, file_3_id))
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
                                new Document(FILES_FIELD + '.' + FILE_GENOTYPE_FIELD + ".0/1", sample_10101_id),
                                new Document(FILES_FIELD + '.' + FILE_GENOTYPE_FIELD + ".1/1", sample_10101_id))),
                        new Document("$or", Arrays.asList(
                                new Document(FILES_FIELD + '.' + FILE_GENOTYPE_FIELD + ".0/1", sample_10201_id),
                                new Document(FILES_FIELD + '.' + FILE_GENOTYPE_FIELD + ".1/1", sample_10201_id))),
                        new Document("$or", Arrays.asList(
                                new Document(FILES_FIELD + '.' + FILE_GENOTYPE_FIELD + ".0/1", sample_10301_id),
                                new Document(FILES_FIELD + '.' + FILE_GENOTYPE_FIELD + ".1/1", sample_10301_id))))
                )
                .append(FILES_FIELD + '.' + FILEID_FIELD, new Document("$all", Arrays.asList(file_1_id, file_2_id, file_3_id)));

        checkEqualDocuments(expected, mongoQuery);
    }

    @Test
    public void testQueryGenotypesAddFiles() {
        Bson mongoQuery = parser.parseQuery(new Query().append(STUDY.key(), "study_1").append(GENOTYPE.key(), "sample_10101" + IS + "0/1"));

        Document expected = new Document(STUDIES_FIELD + '.' + STUDYID_FIELD, 1)
                .append("$and", Collections.singletonList(
                        new Document("$or", Arrays.asList(
                                new Document(FILES_FIELD + '.' + FILE_GENOTYPE_FIELD + ".0/1", sample_10101_id)))))
                .append(FILES_FIELD + '.' + FILEID_FIELD, file_1_id);

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
                                new Document(FILES_FIELD + '.' + FILE_GENOTYPE_FIELD + ".0/1", sample_10101_id),
                                new Document(FILES_FIELD + '.' + FILE_GENOTYPE_FIELD + ".1/1", sample_10101_id)))
                                .append(FILES_FIELD + '.' + FILEID_FIELD, file_1_id),
                        new Document("$or", Arrays.asList(
                                new Document(FILES_FIELD + '.' + FILE_GENOTYPE_FIELD + ".0/1", sample_10201_id),
                                new Document(FILES_FIELD + '.' + FILE_GENOTYPE_FIELD + ".1/1", sample_10201_id)))
                                .append(FILES_FIELD + '.' + FILEID_FIELD, file_2_id)));

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
                                new Document(FILES_FIELD + '.' + FILE_GENOTYPE_FIELD + ".1/1", sample_10101_id) // $eq 1/1
                        ))))
                .append(FILES_FIELD + '.' + FILEID_FIELD, file_1_id);
        checkEqualDocuments(expected, mongoQuery);
    }

//    @Test
//    public void testQueryUnknownGenotypesNotAddFiles() {
//        // FILES filter should not be used when the genotype filter is the unknown genotype
//        Bson mongoQuery = parser.parseQuery(new Query().append(STUDY.key(), "study_1").append(GENOTYPE.key(), "sample_10101" + IS + GenotypeClass.UNKNOWN_GENOTYPE));
//
//        Document expected = new Document(STUDIES_FIELD + '.' + STUDYID_FIELD, 1)
//                .append("$and", Collections.singletonList(
//                        new Document("$or", Arrays.asList(
//                                new Document(FILES_FIELD + '.' + FILE_GENOTYPE_FIELD + ".?/?", sample_10101_id)))));
//
//        checkEqualDocuments(expected, mongoQuery);
//    }

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
                .append(FILES_FIELD + '.' + FILE_GENOTYPE_FIELD, 1)
                .append(FILES_FIELD + '.' + STUDYID_FIELD, 1)
                .append(FILES_FIELD + '.' + ALTERNATES_FIELD, 1)
                .append(FILES_FIELD + '.' + ORI_FIELD, 1)
                .append(FILES_FIELD + '.' + FILEID_FIELD, 1) // Ensure that fileId is always returned
                .append(FILES_FIELD + '.' + SAMPLE_DATA_FIELD, 1);
        //                 .append(FILES_FIELD, new Document("$elemMatch", new Document(FILEID_FIELD, new Document("$in", Arrays.asList(file_0_id, file_1_id, file_2_id, file_3_id, file_4_id)))));

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

        expected.remove(FILES_FIELD + '.' + SAMPLE_DATA_FIELD);
        projection = parser.createProjection(new Query().append(INCLUDE_SAMPLE.key(), ALL)
                .append(INCLUDE_FILE.key(), NONE).append(INCLUDE_GENOTYPE.key(), true), new QueryOptions(QueryOptions.EXCLUDE, ANNOTATION.fieldName()));
        checkEqualDocuments(expected, projection, false);

    }

    // ---- createAggregationPipeline tests ----

    private ParsedVariantQuery parsedQuery(Query query) {
        return new VariantQueryParser(null, metadataManager).parseQuery(query, new QueryOptions(), true);
    }

    /**
     * No study/file restriction in the projection → no $addFields stage, pipeline is [$match, $project].
     */
    @Test
    public void testAggregationPipelineNoFileFilter() {
        List<Bson> pipeline = parser.createAggregationPipeline(parsedQuery(new Query()), new QueryOptions());

        assertEquals(2, pipeline.size());
        assertTrue(((Document) pipeline.get(0)).containsKey("$match"));
        assertTrue(((Document) pipeline.get(1)).containsKey("$project"));
    }

    /**
     * Single specific file requested for a study → $addFields with $filter cond
     * {@code {$and: [{$eq: [$$this.sid, studyId]}, {$in: [$$this.fid, [file_1_id]]}]}}.
     */
    @Test
    public void testAggregationPipelineSingleFileFilter() {
        int studyId = metadataManager.getStudyId("study_1");
        Query query = new Query()
                .append(INCLUDE_STUDY.key(), "study_1")
                .append(INCLUDE_FILE.key(), "/data/file_1");

        List<Bson> pipeline = parser.createAggregationPipeline(parsedQuery(query), new QueryOptions());

        assertEquals(3, pipeline.size());
        assertTrue(((Document) pipeline.get(0)).containsKey("$match"));
        Document addFieldsStage = (Document) pipeline.get(1);
        assertTrue(addFieldsStage.containsKey("$addFields"));
        assertTrue(((Document) pipeline.get(2)).containsKey("$project"));

        Document cond = extractFilterCond(addFieldsStage);
        assertSidFidFilterCond(cond, studyId, Collections.singletonList(file_1_id));
    }

    /**
     * Two specific files from the same study → $addFields with $filter cond
     * {@code {$and: [{$eq: [$$this.sid, studyId]}, {$in: [$$this.fid, [file_1_id, file_2_id]]}]}}.
     */
    @Test
    public void testAggregationPipelineTwoFilesFilter() {
        int studyId = metadataManager.getStudyId("study_1");
        Query query = new Query()
                .append(INCLUDE_STUDY.key(), "study_1")
                .append(INCLUDE_FILE.key(), "/data/file_1,/data/file_2");

        List<Bson> pipeline = parser.createAggregationPipeline(parsedQuery(query), new QueryOptions());

        assertEquals(3, pipeline.size());
        Document cond = extractFilterCond((Document) pipeline.get(1));
        List<?> andList = cond.getList("$and", Object.class);
        assertEquals(2, andList.size());
        // The $in list must contain exactly the two requested file IDs
        Document inClause = (Document) andList.get(1);
        List<?> inArgs = inClause.getList("$in", Object.class);
        List<?> fidList = (List<?>) inArgs.get(1);
        assertEquals(2, fidList.size());
        assertTrue(fidList.contains(file_1_id));
        assertTrue(fidList.contains(file_2_id));
    }

    /**
     * All files of a study requested (no specific file IDs) → $addFields with sid-only condition,
     * no fid restriction.
     */
    @Test
    public void testAggregationPipelineAllFilesOfStudy() {
        int studyId = metadataManager.getStudyId("study_1");
        Query query = new Query()
                .append(INCLUDE_STUDY.key(), "study_1")
                .append(INCLUDE_FILE.key(), ALL)
                .append(INCLUDE_SAMPLE.key(), ALL);

        List<Bson> pipeline = parser.createAggregationPipeline(parsedQuery(query), new QueryOptions());

        assertEquals(3, pipeline.size());
        Document cond = extractFilterCond((Document) pipeline.get(1));
        // No file restriction — just match by study id
        Document expectedCond = new Document("$eq", Arrays.asList("$$this." + STUDYID_FIELD, studyId));
        checkEqualDocuments(expectedCond, cond);
    }

    /**
     * No file projection (INCLUDE_FILE=NONE) → files field not in projection,
     * so no $addFields stage is emitted.
     */
    @Test
    public void testAggregationPipelineExcludeFiles() {
        Query query = new Query()
                .append(INCLUDE_STUDY.key(), "study_1")
                .append(INCLUDE_FILE.key(), NONE)
                .append(INCLUDE_SAMPLE.key(), NONE);

        List<Bson> pipeline = parser.createAggregationPipeline(parsedQuery(query), new QueryOptions());

        // No $addFields when files are excluded from projection
        assertEquals(2, pipeline.size());
        assertFalse(((Document) pipeline.get(1)).containsKey("$addFields"));
    }

    /**
     * Sample-based include: file IDs are derived from the sample's file association.
     * The filter should reference the file that carries the requested sample.
     */
    @Test
    public void testAggregationPipelineSampleDerivedFileFilter() {
        int studyId = metadataManager.getStudyId("study_1");
        // sample_10101 belongs to file_1 (file_1_id)
        Query query = new Query()
                .append(INCLUDE_STUDY.key(), "study_1")
                .append(INCLUDE_SAMPLE.key(), "sample_10101");

        List<Bson> pipeline = parser.createAggregationPipeline(parsedQuery(query), new QueryOptions());

        assertEquals(3, pipeline.size());
        Document cond = extractFilterCond((Document) pipeline.get(1));
        // The cond should contain file_1_id derived from sample_10101
        assertSidFidFilterCond(cond, studyId, Collections.singletonList(file_1_id));
    }

    /**
     * Assert the aggregation $filter cond has the form
     * {@code {$and: [{$eq: ["$$this.sid", studyId]}, {$in: ["$$this.fid", fileIds]}]}}.
     * Uses direct traversal instead of checkEqualDocuments to avoid the $in sort heuristic
     * which is only valid for query-style $in (unordered set), not aggregation $in (positional).
     */
    private static void assertSidFidFilterCond(Document cond, int studyId, List<Integer> fileIds) {
        List<?> andList = cond.getList("$and", Object.class);
        assertEquals(2, andList.size());

        Document eqClause = (Document) andList.get(0);
        List<?> eqArgs = eqClause.getList("$eq", Object.class);
        assertEquals("$$this." + STUDYID_FIELD, eqArgs.get(0));
        assertEquals(studyId, eqArgs.get(1));

        Document inClause = (Document) andList.get(1);
        List<?> inArgs = inClause.getList("$in", Object.class);
        assertEquals("$$this." + FILEID_FIELD, inArgs.get(0));
        assertEquals(fileIds, inArgs.get(1));
    }

    /** Extract the {@code cond} document from an {@code $addFields} pipeline stage. */
    private static Document extractFilterCond(Document addFieldsStage) {
        Document addFields = (Document) addFieldsStage.get("$addFields");
        Document filterExpr = (Document) addFields.get(DocumentToVariantConverter.FILES_FIELD);
        Document filterDoc = (Document) filterExpr.get("$filter");
        return (Document) filterDoc.get("cond");
    }

}