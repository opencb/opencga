/*
 * Copyright 2015 OpenCB
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

package org.opencb.opencga.storage.mongodb.variant;

import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoCursor;
import org.bson.Document;
import org.opencb.biodata.models.variant.Variant;
import org.opencb.opencga.storage.core.variant.adaptors.VariantDBIterator;
import org.opencb.opencga.storage.mongodb.variant.converters.DocumentToVariantConverter;

/**
 * Created by jacobo on 9/01/15.
 */
public class VariantMongoDBIterator extends VariantDBIterator {

    private MongoCursor<Document> dbCursor;
    private DocumentToVariantConverter documentToVariantConverter;

    VariantMongoDBIterator(FindIterable<Document> dbCursor, DocumentToVariantConverter documentToVariantConverter) { //Package protected
        this(dbCursor, documentToVariantConverter, 100);
    }

    VariantMongoDBIterator(FindIterable<Document> dbCursor, DocumentToVariantConverter documentToVariantConverter, int batchSize) { //Package protected
        this.documentToVariantConverter = documentToVariantConverter;
        if (batchSize > 0) {
            dbCursor.batchSize(batchSize);
        }
        this.dbCursor = dbCursor.iterator();
    }

    @Override
    public boolean hasNext() {
        return dbCursor.hasNext();
    }

    @Override
    public Variant next() {
        long start = System.currentTimeMillis();
        Document document = dbCursor.next();
        timeFetching += System.currentTimeMillis() - start;
        start = System.currentTimeMillis();
        Variant variant = documentToVariantConverter.convertToDataModelType(document);
        timeConverting += System.currentTimeMillis() - start;

        return variant;
    }

    @Override
    public void remove() {
        throw new UnsupportedOperationException("can't remove from a cursor");
    }

    @Override
    public void close() {
        dbCursor.close();
    }
}
