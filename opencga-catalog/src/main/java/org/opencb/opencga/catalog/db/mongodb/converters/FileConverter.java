/*
 * Copyright 2015-2020 OpenCB
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

import org.apache.commons.lang3.StringUtils;
import org.bson.Document;
import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.commons.utils.ListUtils;
import org.opencb.opencga.catalog.db.api.SampleDBAdaptor;
import org.opencb.opencga.catalog.db.mongodb.FileMongoDBAdaptor;
import org.opencb.opencga.core.models.file.File;
import org.opencb.opencga.core.models.file.FileRelatedFile;
import org.opencb.opencga.core.models.sample.Sample;
import org.opencb.opencga.core.models.study.VariableSet;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

/**
 * Created by pfurio on 19/01/16.
 */
public class FileConverter extends AnnotableConverter<File> {

    public FileConverter() {
        super(File.class);
    }

    @Override
    public File convertToDataModelType(Document document, QueryOptions options) {
        File file = super.convertToDataModelType(document, options);

        List<String> sampleIds;
        if (document.get(FileMongoDBAdaptor.PRIVATE_SAMPLES) != null) {
            List<Document> sampleDocuments = document.getList(FileMongoDBAdaptor.PRIVATE_SAMPLES, Document.class);
            sampleIds = sampleDocuments.stream().map(d -> d.getString(SampleDBAdaptor.QueryParams.ID.key())).collect(Collectors.toList());
        } else {
            sampleIds = Collections.emptyList();
        }
        file.setSampleIds(sampleIds);

        return file;
    }

    public Document convertToStorageType(File file, List<Sample> samples, List<VariableSet> variableSetList) {
        List<FileRelatedFile> relatedFileList = file.getRelatedFiles();
        file.setRelatedFiles(null);

        Document document = super.convertToStorageType(file, variableSetList);
        document.remove(SampleDBAdaptor.QueryParams.ANNOTATION_SETS.key());

        document.put("uid", file.getUid());
        document.put("studyUid", file.getStudyUid());

        document.put(FileMongoDBAdaptor.PRIVATE_SAMPLES, convertSamples(samples));
        document.remove("sampleIds");

        document.put("relatedFiles", convertRelatedFiles(relatedFileList));

        document.put(FileMongoDBAdaptor.REVERSE_NAME, StringUtils.reverse(file.getName()));

        return document;
    }

    public List<Document> convertSamples(List<Sample> sampleList) {
        if (sampleList == null || sampleList.isEmpty()) {
            return Collections.emptyList();
        }
        List<Document> samples = new ArrayList(sampleList.size());
        for (Sample sample : sampleList) {
            long sampleId = sample != null ? (sample.getUid() == 0 ? -1L : sample.getUid()) : -1L;
            if (sampleId > 0) {
                samples.add(new Document()
                        .append(SampleDBAdaptor.QueryParams.UID.key(), sampleId)
                        .append(SampleDBAdaptor.QueryParams.UUID.key(), sample.getUuid())
                        .append(SampleDBAdaptor.QueryParams.ID.key(), sample.getId())
                );
            }
        }
        return samples;
    }

    public List<Document> convertRelatedFiles(List<FileRelatedFile> relatedFileList) {
        if (relatedFileList == null || relatedFileList.isEmpty()) {
            return Collections.emptyList();
        }
        List<Document> relatedFiles = new ArrayList<>();
        if (ListUtils.isNotEmpty(relatedFileList)) {
            for (FileRelatedFile relatedFile : relatedFileList) {
                relatedFiles.add(new Document()
                        .append("relation", relatedFile.getRelation().name())
                        .append("file", new Document("uid", relatedFile.getFile().getUid()))
                );
            }
        }
        return relatedFiles;
    }
}
