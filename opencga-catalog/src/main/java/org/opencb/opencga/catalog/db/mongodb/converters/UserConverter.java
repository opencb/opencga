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

import org.apache.avro.generic.GenericRecord;
import org.bson.Document;
import org.opencb.commons.datastore.mongodb.GenericDocumentComplexConverter;
import org.opencb.opencga.core.models.common.GenericRecordAvroJsonMixin;
import org.opencb.opencga.core.models.user.User;

/**
 * Created by pfurio on 19/01/16.
 */
public class UserConverter extends OpenCgaMongoConverter<User> {

    public UserConverter() {
        super(User.class);
        getObjectMapper().addMixIn(GenericRecord.class, GenericRecordAvroJsonMixin.class);
    }

    @Override
    public User convertToDataModelType(Document document) {
        // TODO: Remove this piece of code once we are sure User contains the migrated new account type from 1.4.2
        Document account = (Document) document.get("account");
        if (account != null && account.get("authentication") == null) {
            // We make sure type is in upper case because we are now storing the enum names
            String type = account.getString("type");
            account.put("type", type.toUpperCase());

            String authOrigin = account.getString("authOrigin");
            Document authentication = new Document()
                    .append("id", authOrigin)
                    .append("application", false);
            account.put("authentication", authentication);
        }

        return super.convertToDataModelType(document);
    }

}
