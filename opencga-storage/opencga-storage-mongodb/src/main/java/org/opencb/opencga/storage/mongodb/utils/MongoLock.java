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

package org.opencb.opencga.storage.mongodb.utils;

import com.mongodb.ReadPreference;
import com.mongodb.WriteConcern;
import org.apache.commons.lang3.time.StopWatch;
import org.bson.conversions.Bson;
import org.opencb.commons.datastore.mongodb.MongoDBCollection;

import java.util.Calendar;
import java.util.Date;
import java.util.concurrent.TimeoutException;

import static com.mongodb.client.model.Filters.*;
import static com.mongodb.client.model.Updates.combine;
import static com.mongodb.client.model.Updates.set;

/**
 * Concurrent lock using a MongoDB document.
 *
 * Created on 13/06/16
 *
 * see http://stackoverflow.com/questions/31064750/mongodb-implement-a-read-write-lock-mutex
 *
 * @author Jacobo Coll &lt;jacobo167@gmail.com&gt;
 */
public class MongoLock {

    private static final String LOCK_FIELD = "lock";
    private static final String WRITE_FIELD = "write";
    private final String lockWriteField;

    private final MongoDBCollection collection;

    public MongoLock(MongoDBCollection collection) {
        this(collection, LOCK_FIELD);
    }

    public MongoLock(MongoDBCollection collection, String lockField) {
        this.collection = collection;
        this.collection.withReadPreference(ReadPreference.primary())
                .withWriteConcern(WriteConcern.ACKNOWLEDGED);
        lockWriteField = lockField + '.' + WRITE_FIELD;
    }

    /**
     * Apply for the lock.
     *
     * @param id            _id the document to lock
     * @param lockDuration  Duration un milliseconds of the token. After this time the token is expired.
     * @param timeout       Max time in milliseconds to wait for the lock
     *
     * @return              Lock token
     *
     * @throws InterruptedException if any thread has interrupted the current thread.
     * @throws TimeoutException if the operations takes more than the timeout value.
     */
    public long lock(Object id, long lockDuration, long timeout)
            throws InterruptedException, TimeoutException {

        StopWatch watch = new StopWatch();
        watch.start();
        long modifiedCount;
        Date date;
        do {
            date = new Date(Calendar.getInstance().getTimeInMillis() + lockDuration);
            Date now = Calendar.getInstance().getTime();

            Bson query = and(eq("_id", id), or(eq(lockWriteField, null), lt(lockWriteField, now)));
            Bson update = combine(set(lockWriteField, date));

            modifiedCount = collection.update(query, update, null).first().getModifiedCount();

            if (modifiedCount != 1) {
                Thread.sleep(100);
                //Check if the lock is still valid
                if (watch.getTime() > timeout) {
                    throw new TimeoutException("Unable to get the lock");
                }
            }
        } while (modifiedCount == 0);


        return date.getTime();
    }


    /**
     * Releases the lock.
     *
     * @param id            _id the document to lock
     * @param lockToken     Lock token
     * @throws IllegalStateException  if the lockToken does not match with the current lockToken
     */
    public void unlock(Object id, long lockToken) {

        Date date = new Date(lockToken);
        Bson query = and(eq("_id", id), eq(lockWriteField, date));
        Bson update = set(lockWriteField, null);

        long matchedCount = collection.update(query, update, null).first().getMatchedCount();
        if (matchedCount == 0) {
            throw new IllegalStateException("Lock token " + lockToken + " not found!");
        }
    }

}
