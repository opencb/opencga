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

package org.opencb.opencga.core.common;

import java.nio.file.Path;
import java.nio.file.Paths;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Random;

@Deprecated
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
