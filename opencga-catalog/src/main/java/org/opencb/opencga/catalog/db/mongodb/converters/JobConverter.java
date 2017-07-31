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
import org.opencb.opencga.catalog.models.Job;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

/**
 * Created by pfurio on 19/01/16.
 */
public class JobConverter extends GenericDocumentComplexConverter<Job> {

    public JobConverter() {
        super(Job.class);
    }

    @Override
    public Document convertToStorageType(Job object) {
        Document document = super.convertToStorageType(object);
        document.put("id", document.getInteger("id").longValue());
        document.put("outDir", convertFileToDocument(object.getOutDir()));
        document.put("input", convertFilesToDocument(object.getInput()));
        document.put("output", convertFilesToDocument(object.getOutput()));
        return document;
    }

    private Document convertFileToDocument(File file) {
        if (file == null) {
            return new Document("id", -1L);
        }
        return convertFilesToDocument(Arrays.asList(file)).get(0);
    }

    private List<Document> convertFilesToDocument(List<File> fileList) {
        if (fileList == null || fileList.size() == 0) {
            return Collections.emptyList();
        }
        List<Document> files = new ArrayList(fileList.size());
        for (File file : fileList) {
            long fileId = file != null ? (file.getId() == 0 ? -1L : file.getId()) : -1L;
            if (fileId > 0) {
                files.add(new Document("id", fileId));
            }
        }
        return files;
    }
}
