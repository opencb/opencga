package org.opencb.opencga.catalog.utils;

import io.jsonwebtoken.impl.Base64UrlCodec;

import java.util.Date;
import java.util.Random;
import java.util.UUID;
import java.util.regex.Matcher;
import java.util.regex.Pattern;


public class UUIDUtils {

    // OpenCGA uuid pattern: xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx
    //                       --------           ----
    //                       time low        installation
    //                                ----           ------------
    //                              time mid            random
    //                                     ----
    //              version (1 hex digit) + internal version (1 hex digit) + entity (2 hex digit)

    public static final Pattern UUID_PATTERN = Pattern.compile("[0-9a-f]{8}-[0-9a-f]{4}-0[0-9a-f]{3}-[0-9a-f]{4}-[0-9a-f]{12}");

    public enum Entity {
        PROJECT(1),
        STUDY(2),
        FILE(3),
        SAMPLE(4),
        COHORT(5),
        INDIVIDUAL(6),
        FAMILY(7),
        JOB(8),
        CLINICAL(9),
        PANEL(10);

        private final int mask;

        Entity(int mask) {
            this.mask = mask;
        }

        public int getMask() {
            return mask;
        }
    }

    public static String generateOpenCGAUUID(Entity entity) {
        return generateOpenCGAUUID(entity, new Date());
    }

    public static String generateOpenCGAUUID(Entity entity, Date date) {
        long mostSignificantBits = getMostSignificantBits(date, entity);
        long leastSignificantBits = getLeastSignificantBits();

        UUID uuid = new UUID(mostSignificantBits, leastSignificantBits);

        return new String(Base64UrlCodec.BASE64URL.encode(uuid.toString().getBytes()));
    }

    public static boolean isOpenCGAUUID(String token) {
        if (token.length() == 48) {
            String uuid = new String(Base64UrlCodec.BASE64URL.decodeToString(token));
            Matcher matcher = UUID_PATTERN.matcher(uuid);
            return matcher.find();
        }
        return false;
    }

    private static long getMostSignificantBits(Date date, Entity entity) {
        long time = date.getTime();

        long timeLow = time & 0xffffffffL;
        long timeMid = (time >>> 32) & 0xffffL;
        // long timeHigh = (time >>> 48) & 0xffffL;

        long uuidVersion = /*0xff &*/ 0;
        long internalVersion = /*0xff &*/ 0;
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
