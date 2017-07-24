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
import org.opencb.opencga.catalog.models.Family;
import org.opencb.opencga.catalog.models.Individual;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * Created by pfurio on 03/05/17.
 */
public class FamilyConverter extends GenericDocumentComplexConverter<Family> {

    public FamilyConverter() {
        super(Family.class);
    }

    @Override
    public Family convertToDataModelType(Document document) {
        return super.convertToDataModelType(document);
    }

    @Override
    public Document convertToStorageType(Family object) {
        Document document = super.convertToStorageType(object);
        document.put("id", document.getInteger("id").longValue());

        long motherId = object.getMother() != null ? (object.getMother().getId() == 0 ? -1L : object.getMother().getId()) : -1L;
        document.put("mother", new Document("id", motherId));

        long fatherId = object.getFather() != null ? (object.getFather().getId() == 0 ? -1L : object.getFather().getId()) : -1L;
        document.put("father", new Document("id", fatherId));

        if (object.getChildren() != null) {
            List<Document> children = new ArrayList();
            for (Individual individual : object.getChildren()) {
                long individualId = individual != null ? (individual.getId() == 0 ? -1L : individual.getId()) : -1L;
                if (individualId > 0) {
                    children.add(new Document("id", individualId));
                }
            }
            if (children.size() > 0) {
                document.put("children", children);
            }
        } else {
            document.put("children", Collections.emptyList());
        }

        return document;
    }

}
