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

import org.bson.Document;
import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.opencga.catalog.db.api.SampleDBAdaptor;
import org.opencb.opencga.core.models.sample.Sample;
import org.opencb.opencga.core.models.study.VariableSet;

import java.util.List;
import java.util.Map;

/**
 * Created by pfurio on 19/01/16.
 */
public class SampleConverter extends AnnotableConverter<Sample> {

    private IndividualConverter individualConverter;

    public SampleConverter() {
        super(Sample.class);
        individualConverter = new IndividualConverter();
    }

    @Override
    public Document convertToStorageType(Sample object, List<VariableSet> variableSetList) {
        Document document = super.convertToStorageType(object, variableSetList);
        document.remove(SampleDBAdaptor.QueryParams.ANNOTATION_SETS.key());

        document.put("uid", object.getUid());
        document.put("studyUid", object.getStudyUid());
        document.put("individual", new Document());
        return document;
    }

    @Override
    public Sample convertToDataModelType(Document document, QueryOptions options) {
        Sample sample = super.convertToDataModelType(document, options);
        if (sample.getAttributes() != null && sample.getAttributes().containsKey("OPENCGA_INDIVIDUAL")) {
            Object individual = sample.getAttributes().get("OPENCGA_INDIVIDUAL");
            if (individual instanceof Map) {
                sample.getAttributes().put("OPENCGA_INDIVIDUAL", individualConverter.convertToDataModelType(
                        (Document) ((Document) document.get(SampleDBAdaptor.QueryParams.ATTRIBUTES.key())).get("OPENCGA_INDIVIDUAL")
                ));
            }
        }
        return sample;
    }
}
