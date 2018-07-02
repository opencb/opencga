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
    //              version (1 word) + internal version (1 word) + entity (2 words)

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

        String timeLow = Long.toBinaryString(time & Long.parseLong("ffffffff", 16));
        String timeMid = Long.toBinaryString((time >> 32) & Long.parseLong("ffff", 16));
//        String timeHigh = Long.toBinaryString((time >> 48) & Long.parseLong("ffff", 16));

        String uuidVersion = "0";
        String internalVersion = "0";
        // 2 words for the entity
        String entityBin = Integer.toBinaryString(entity.getMask());

        return Long.parseLong(String.format("%32s", timeLow).replace(" ", "0") + String.format("%16s", timeMid).replace(" ", "0")
                + String.format("%4s", uuidVersion).replace(" ", "0") + String.format("%4s", internalVersion).replace(" ", "0")
                + String.format("%8s", entityBin).replace(" ", "0"), 2);
    }

    private static long getLeastSignificantBits() {
        // 4 words installation
        String installation = "1";

        // 12 words random
        Random rand = new Random();
        String randomNumberBin = Long.toBinaryString(rand.nextLong() & Long.parseLong("ffffffffffff", 16));

        return Long.parseLong(String.format("%16s", installation).replace(" ", "0")
                + String.format("%48s", randomNumberBin).replace(" ", "0"), 2);
    }

}
