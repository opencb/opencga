package org.opencb.opencga.lib.common;

import java.nio.file.Path;
import java.nio.file.Paths;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.security.SecureRandom;
import java.util.Random;

public class StringUtils {
    private final static String characters = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789";
    private final static Random r = new Random(System.currentTimeMillis());

    public static String randomString() {
        return randomString(10);
    }

    public static String randomString(int length) {
        StringBuilder string = new StringBuilder();
        for (int i = 0; i < length; i++) {
            string.append(characters.charAt(r.nextInt(characters.length())));
        }
        return string.toString();
    }

    public static String sha1(String text) throws NoSuchAlgorithmException {
        MessageDigest sha1 = MessageDigest.getInstance("SHA1");
        byte[] digest = sha1.digest((text).getBytes());
        return bytes2String(digest);
    }

    public static String bytes2String(byte[] bytes) {
        StringBuilder string = new StringBuilder();
        for (byte b : bytes) {
            String hexString = Integer.toHexString(0x00FF & b);
            string.append(hexString.length() == 1 ? "0" + hexString : hexString);
        }
        return string.toString();
    }

    public static Path parseObjectId(String objectIdFromURL) {
        String[] tokens = objectIdFromURL.split(":");
        // if(tokens.length == 0){
        // return Paths.get(objectIdFromURL);
        // }
        Path objectPath = Paths.get("");
        for (int i = 0; i < tokens.length; i++) {
            objectPath = objectPath.resolve(Paths.get(tokens[i]));
        }
        return objectPath;
    }
}
