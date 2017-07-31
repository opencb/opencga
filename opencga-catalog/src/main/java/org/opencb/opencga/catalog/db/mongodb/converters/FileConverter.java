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

package org.opencb.opencga.catalog.db.mongodb.converters;

import org.bson.Document;
import org.opencb.commons.datastore.mongodb.GenericDocumentComplexConverter;
import org.opencb.opencga.catalog.models.File;
import org.opencb.opencga.catalog.models.Sample;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * Created by pfurio on 19/01/16.
 */
public class FileConverter extends GenericDocumentComplexConverter<File> {

    public FileConverter() {
        super(File.class);
    }

    @Override
    public File convertToDataModelType(Document document) {
        if (document.get("job") != null && document.get("job") instanceof List && ((List) document.get("job")).size() > 0) {
            document.put("job", ((List) document.get("job")).get(0));
        } else {
            document.put("job", new Document("id", -1));
        }
        if (document.get("experiment") != null && document.get("experiment") instanceof List
                && ((List) document.get("experiment")).size() > 0) {
            document.put("experiment", ((List) document.get("experiment")).get(0));
        } else {
            document.put("experiment", new Document("id", -1));
        }
        return super.convertToDataModelType(document);
    }

    @Override
    public Document convertToStorageType(File file) {
        Document document = super.convertToStorageType(file);
        document.put("id", document.getInteger("id").longValue());

        long jobId = file.getJob() != null ? (file.getJob().getId() == 0 ? -1L : file.getJob().getId()) : -1L;
        document.put("job", new Document("id", jobId));

        long experimentId = file.getExperiment() != null
                ? (file.getExperiment().getId() == 0 ? -1L : file.getExperiment().getId())
                : -1L;
        document.put("experiment", new Document("id", experimentId));

        document.put("samples", convertSamples(file.getSamples()));

        return document;
    }

    public List<Document> convertSamples(List<Sample> sampleList) {
        if (sampleList == null || sampleList.size() == 0) {
            return Collections.emptyList();
        }
        List<Document> samples = new ArrayList(sampleList.size());
        for (Sample sample : sampleList) {
            long sampleId = sample != null ? (sample.getId() == 0 ? -1L : sample.getId()) : -1L;
            if (sampleId > 0) {
                samples.add(new Document("id", sampleId));
            }
        }
        return samples;
    }
}
