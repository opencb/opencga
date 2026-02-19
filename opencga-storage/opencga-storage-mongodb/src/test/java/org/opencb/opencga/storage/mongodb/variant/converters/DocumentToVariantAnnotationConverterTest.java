/*
 * Copyright 2015-2017 OpenCB
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.opencb.opencga.storage.mongodb.variant.converters;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.databind.MapperFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectWriter;
import org.apache.avro.generic.GenericRecord;
import org.bson.BsonInt32;
import org.bson.BsonInt64;
import org.bson.Document;
import org.bson.conversions.Bson;
import org.bson.json.JsonMode;
import org.bson.json.JsonWriterSettings;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.opencb.biodata.models.variant.Variant;
import org.opencb.biodata.models.variant.avro.VariantAnnotation;
import org.opencb.cellbase.client.rest.CellBaseClient;
import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.opencga.core.api.ParamConstants;
import org.opencb.opencga.core.common.JacksonUtils;
import org.opencb.opencga.core.config.storage.CellBaseConfiguration;
import org.opencb.opencga.core.models.common.mixins.GenericRecordAvroJsonMixin;
import org.opencb.opencga.core.testclassification.duration.ShortTests;
import org.opencb.opencga.storage.core.utils.CellBaseUtils;
import org.opencb.opencga.storage.core.variant.query.VariantQueryUtils;

import java.io.File;
import java.io.InputStream;
import java.util.*;

import static java.util.Collections.singletonList;
import static org.junit.Assert.*;

/**
 * Created by fjlopez on 23/09/15.
 */
@Category(ShortTests.class)
public class DocumentToVariantAnnotationConverterTest {

    private VariantAnnotation variantAnnotation;
//    private Document dbObject;
    public static final Document ANY = new Document();
    public static final List ANY_LIST = Arrays.asList();


    @Before
    public void setUp() throws Exception {
        //Setup variant
        String resource = "/variantAnnotation-11-17427157-G-A.json";
//        String resource = "variantAnnotation-19-45411941-T-C.json";
        InputStream is = this.getClass().getResourceAsStream(resource);
        if (is != null) {
            variantAnnotation = JacksonUtils.getDefaultObjectMapper().readValue(
                    is, VariantAnnotation.class);
        } else {
            CellBaseConfiguration configuration = CellBaseUtils.validate(
                    new CellBaseConfiguration(ParamConstants.CELLBASE_URL, ParamConstants.CELLBASE_VERSION),
                    "hsapiens", "grch38", true);
            CellBaseUtils utils = new CellBaseUtils(
                    new CellBaseClient("hsapiens", "grch38",
                            configuration.getDataRelease(),
                            configuration.toClientConfiguration()));
            utils.validate();
            variantAnnotation = utils.getCellBaseClient().getVariantClient()
                    .getAnnotationByVariantIds(singletonList("11:17427157:G:A"), new QueryOptions(), true).firstResult();
            File file = new File("src/test/resources/" + resource);
            assertFalse(file.exists());
            JacksonUtils.getDefaultObjectMapper().writerWithDefaultPrettyPrinter().writeValue(file, variantAnnotation);
            assertTrue(file.exists());
        }

    }

//    @Test
//    public void testConvertToDataModelType() throws Exception {
//        DocumentToVariantAnnotationConverter documentToVariantAnnotationConverter = new DocumentToVariantAnnotationConverter();
//        VariantAnnotation convertedVariantAnnotation = documentToVariantAnnotationConverter.convertToDataModelType(dbObject, null, new Variant("11:17427157:G:A"));
//        assertEquals(convertedVariantAnnotation.getConsequenceTypes().get(2).getProteinVariantAnnotation().getReference(), "CYS");
////        assertEquals(convertedVariantAnnotation.getVariantTraitAssociation().getCosmic().get(0).getPrimarySite(), "large_intestine");
//    }

    @Test
    public void testTranscriptFlagConversion() {
        assertEquals(VariantQueryUtils.IMPORTANT_TRANSCRIPT_FLAGS.size(), DocumentToVariantAnnotationConverter.FLAG_TO_STORAGE_MAP.size());
        assertEquals(VariantQueryUtils.IMPORTANT_TRANSCRIPT_FLAGS, DocumentToVariantAnnotationConverter.FLAG_TO_STORAGE_MAP.keySet());
        assertEquals(VariantQueryUtils.IMPORTANT_TRANSCRIPT_FLAGS.size(), DocumentToVariantAnnotationConverter.FLAG_FROM_STORAGE_MAP.size());
    }

    @Test
    public void testBiotypeConversion() {
        assertEquals(DocumentToVariantAnnotationConverter.BT_TO_STORAGE_MAP.size(), DocumentToVariantAnnotationConverter.BT_FROM_STORAGE_MAP.size());
    }

    @Test
    public void testConvertBothWaysType() throws Exception {
        DocumentToVariantAnnotationConverter documentToVariantAnnotationConverter = new DocumentToVariantAnnotationConverter();
        Document convertedDBObject = documentToVariantAnnotationConverter.convertToStorageType(variantAnnotation);
        VariantAnnotation convertedVariantAnnotation = documentToVariantAnnotationConverter.convertToDataModelType(convertedDBObject, null, new Variant("11:17427157:G:A"));
        ObjectMapper objectMapper = new ObjectMapper();
        objectMapper.addMixIn(GenericRecord.class, GenericRecordAvroJsonMixin.class);
        objectMapper.setSerializationInclusion(JsonInclude.Include.NON_NULL);
        objectMapper.configure(MapperFeature.SORT_PROPERTIES_ALPHABETICALLY, true);
        ObjectWriter objectWriter = objectMapper.writerWithDefaultPrettyPrinter();
        String expected = objectWriter.writeValueAsString(variantAnnotation);
        String actual = objectWriter.writeValueAsString(convertedVariantAnnotation);
        assertEquals(expected, actual);
    }

    public static void checkEqualDocuments(Document expected, Bson actual) {
        checkEqualDocuments(expected, actual, true);
    }

    public static void checkEqualDocuments(Document expected, Bson actual, boolean toExplicitOps) {
        Document actualDocument = new Document(actual.toBsonDocument());
        Document inputExpected = expected;
        if (toExplicitOps) {
            expected = toExplicitOps(expected);
        }

//        // unflatten
//        if (actualDocument.size() == 1 && actualDocument.containsKey("$and")) {
//            List<BsonDocument> andList = (List<BsonDocument>) actualDocument.get("$and");
//            actualDocument = new Document();
//            for (BsonDocument doc : andList) {
//                actualDocument.putAll(doc);
//            }
//        }

        try {
            checkEqualObjects(expected, actualDocument, "");
        } catch (AssertionError error) {
            JsonWriterSettings settings = JsonWriterSettings.builder().indent(false).outputMode(JsonMode.SHELL).build();
            System.out.println("expected            = " + expected.toJson(settings));
//            System.out.println("expectedExplicitOps = " + expectedExplicitOps.toJson(settings));
//            System.out.println("actualDocument      = " + actualDocument.toJson(settings));
            System.out.println("actual              = " + actual.toBsonDocument().toJson(settings));
            throw error;
        }
    }

    private static Document toExplicitOps(Document expected) {
        Document expectedDocument;
        if (expected.size() > 1) {
            expectedDocument = new Document();
            List<Document> andList = new ArrayList<>();
            for (Map.Entry<String, Object> entry : expected.entrySet()) {
                Object value = entry.getValue();
                if (value instanceof Document) {
                    value = toExplicitOps((Document) value);
                } else if (value instanceof Collection) {
                    List<Object> list = new ArrayList<>();
                    for (Object o : (List<?>) value) {
                        if (o instanceof Document) {
                            list.add(toExplicitOps((Document) o));
                        } else {
                            list.add(o);
                        }
                    }
                    value = list;
                }
                andList.add(new Document(entry.getKey(), value));
            }
            expectedDocument.put("$and", andList);
        } else {
            expectedDocument = expected;
        }
        return expectedDocument;
    }

    private static void checkEqualObjects(Object expected, Object actual, String path) {
        if (expected == ANY || expected == ANY_LIST) {
            // Accept ANY field. Ignore
            return;
        }
        if (expected instanceof Map) {
            expected = new Document((Map) expected);
        }
        if (actual instanceof Map) {
            actual = new Document((Map) actual);
        }
        if (actual instanceof BsonInt32) {
            actual = ((BsonInt32) actual).getValue();
        }
        if (actual instanceof BsonInt64) {
            actual = ((BsonInt64) actual).getValue();
        }

        if (expected instanceof Document && actual instanceof Document) {
            checkEqualObjects((Document) expected, (Document) actual, path);
        } else if (expected instanceof List && actual instanceof List) {
            if (path.endsWith("$in")) {
                // For $in operator, the order of the list is not important. Sort both lists and compare.
                List<?> expectedList = new ArrayList<>((List<?>) expected);
                List<?> actualList = new ArrayList<>((List<?>) actual);
                expectedList.sort(Comparator.comparing(Object::toString));
                actualList.sort(Comparator.comparing(Object::toString));
                expected = expectedList;
                actual = actualList;
            }
            assertEquals(path + ".size", ((List) expected).size(), ((List) actual).size());
            for (int i = 0; i < ((List) expected).size(); i++) {
                checkEqualObjects(((List) expected).get(i), ((List) actual).get(i), path + '[' + i + ']');
            }
        } else {
            assertEquals("Through " + path, expected, actual);
        }
    }

    private static void checkEqualObjects(Document expected, Document actual, String path) {
        assertEquals("Through " + path, new TreeSet<>(expected.keySet()), new TreeSet<>(actual.keySet()));
        for (String key : expected.keySet()) {
            Object e = expected.get(key);
            Object a = actual.get(key);
            checkEqualObjects(e, a, path + '.' + key);
        }
    }

}