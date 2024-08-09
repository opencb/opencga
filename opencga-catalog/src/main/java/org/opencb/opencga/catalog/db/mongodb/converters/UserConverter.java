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
import org.opencb.opencga.catalog.db.api.UserDBAdaptor;
import org.opencb.opencga.core.models.common.mixins.GenericRecordAvroJsonMixin;
import org.opencb.opencga.core.models.user.User;
import org.opencb.opencga.core.models.user.UserInternal;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Created by pfurio on 19/01/16.
 */
public class UserConverter extends OpenCgaMongoConverter<User> {

    protected static Logger logger = LoggerFactory.getLogger(UserConverter.class);

    public UserConverter() {
        super(User.class);
        getObjectMapper().addMixIn(GenericRecord.class, GenericRecordAvroJsonMixin.class);
    }

    @Override
    public Document convertToStorageType(User object) {
        Document userDocument = super.convertToStorageType(object);
        removeDeprecatedAccountObject(userDocument);
        return userDocument;
    }

    @Override
    public User convertToDataModelType(Document document) {
        User user = super.convertToDataModelType(document);

        restoreFromDeprecatedAccountObject(user);
        addToDeprecatedAccountObject(user);

        return user;
    }

    /**
     * Remove 'account' object from the User document so it is no longer stored in the database.
     * Remove after a few releases.
     *
     * @param userDocument User document.
     */
    @Deprecated
    private void removeDeprecatedAccountObject(Document userDocument) {
        userDocument.remove(UserDBAdaptor.QueryParams.DEPRECATED_ACCOUNT.key());
    }

    /**
     * Restores information from the account object to the corresponding internal.account object.
     * Added to maintain backwards compatibility with the deprecated account object in TASK-6494 (v3.2.1)
     * Remove after a few releases.
     *
     * @param user User object.
     */
    @Deprecated
    private void restoreFromDeprecatedAccountObject(User user) {
        if (user.getAccount() != null) {
            if (user.getInternal() == null) {
                user.setInternal(new UserInternal());
            }
            user.getInternal().setAccount(user.getAccount());
            logger.warn("Restoring user account information from deprecated account object to internal.account object. "
                    + "Please, run 'opencga-admin.sh migration run'.");
        }
    }

    private void addToDeprecatedAccountObject(User user) {
        // Add account to deprecated place
        if (user.getInternal() != null && user.getInternal().getAccount() != null && user.getAccount() == null) {
            user.setAccount(user.getInternal().getAccount());
        }
    }

}
