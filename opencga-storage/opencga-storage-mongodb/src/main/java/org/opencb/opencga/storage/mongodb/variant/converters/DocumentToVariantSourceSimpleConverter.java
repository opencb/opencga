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

import com.fasterxml.jackson.databind.MapperFeature;
import org.bson.Document;
import org.opencb.biodata.models.variant.VariantSource;
import org.opencb.commons.datastore.mongodb.GenericDocumentComplexConverter;

/**
 * Created on 26/04/16.
 *
 * @author Jacobo Coll &lt;jacobo167@gmail.com&gt;
 */
public class DocumentToVariantSourceSimpleConverter extends GenericDocumentComplexConverter<VariantSource> {

    public DocumentToVariantSourceSimpleConverter() {
        super(VariantSource.class);
        getObjectMapper().configure(MapperFeature.REQUIRE_SETTERS_FOR_GETTERS, true);
    }

    @Override
    public Document convertToStorageType(VariantSource object) {
        Document document = super.convertToStorageType(object);
        document.append("_id", buildId(object.getStudyId(), object.getFileId()));
        return document;
    }

    public static String buildId(String studyId, String fileId) {
        return studyId + '_' + fileId;
    }

    public static String buildId(int studyId, int fileId) {
        return String.valueOf(studyId) + '_' + fileId;
    }

}
