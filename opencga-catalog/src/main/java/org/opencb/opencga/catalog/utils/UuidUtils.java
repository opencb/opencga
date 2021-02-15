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

package org.opencb.opencga.catalog.utils;

import java.nio.BufferUnderflowException;
import java.util.Date;
import java.util.Random;
import java.util.UUID;
import java.util.regex.Matcher;
import java.util.regex.Pattern;


public class UuidUtils {

    // OpenCGA uuid pattern: xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx
    //                       --------           ----
    //                       time low        installation
    //                                ----           ------------
    //                              time mid            random
    //                                     ----
    //              version (1 hex digit) + internal version (1 hex digit) + entity (2 hex digit)

    public static final Pattern UUID_PATTERN = Pattern.compile("[0-9a-f]{8}-[0-9a-f]{4}-00[0-9a-f]{2}-[0-9a-f]{4}-[0-9a-f]{12}");

    public enum Entity {
        AUDIT(0),
        PROJECT(1),
        STUDY(2),
        FILE(3),
        SAMPLE(4),
        COHORT(5),
        INDIVIDUAL(6),
        FAMILY(7),
        JOB(8),
        CLINICAL(9),
        PANEL(10),
        INTERPRETATION(11);

        private final int mask;

        Entity(int mask) {
            this.mask = mask;
        }

        public int getMask() {
            return mask;
        }
    }

    public static String generateOpenCgaUuid(Entity entity) {
        return generateOpenCgaUuid(entity, new Date());
    }

    public static String generateOpenCgaUuid(Entity entity, Date date) {
        long mostSignificantBits = getMostSignificantBits(date, entity);
        long leastSignificantBits = getLeastSignificantBits();

        String s = new UUID(mostSignificantBits, leastSignificantBits).toString();
        isOpenCgaUuid(s);

        return new UUID(mostSignificantBits, leastSignificantBits).toString();
    }

    public static boolean isOpenCgaUuid(String id) {
        if (id.length() == 36) {
            try {
                Matcher matcher = UUID_PATTERN.matcher(id);
                return matcher.find();
            } catch (BufferUnderflowException e) {
                return false;
            }
        }
        return false;
    }

    private static long getMostSignificantBits(Date date, Entity entity) {
        long time = date.getTime();

        long timeLow = time & 0xffffffffL;
        long timeMid = (time >>> 32) & 0xffffL;
        // long timeHigh = (time >>> 48) & 0xffffL;

        long uuidVersion = /*0xf &*/ 0;
        long internalVersion = /*0xf &*/ 0;
        long entityBin = 0xffL & (long)entity.getMask();

        return (timeLow << 32) | (timeMid << 16) | (uuidVersion << 12) | (internalVersion << 8) | entityBin;
    }

    private static long getLeastSignificantBits() {
        long installation = /*0xffL &*/ 0x1L;
        // 12 hex digits random
        Random rand = new Random();
        long randomNumber = 0xffffffffffffL & rand.nextLong();
        return (installation << 48) | randomNumber;
    }
}
