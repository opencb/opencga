package com.zettagenomics.opencga.enterprise.catalog.utils;

import org.apache.commons.lang3.StringUtils;
import org.opencb.commons.utils.CryptoUtils;
import org.opencb.opencga.catalog.exceptions.CatalogException;

public class SecureKeyUtils {

    public static String generateNewSecurityKey() throws CatalogException {
        try {
            return CryptoUtils.secretKeyToString(CryptoUtils.generateKey(256));
        } catch (Exception e) {
            throw new CatalogException("Could not generate a security key for the federation server.", e);
        }
    }

    public static String encodeSecureString(String secureString) {
        int length = secureString.length();
        if (length % 4 != 0) {
            // Add padding
            secureString = secureString + StringUtils.repeat("¬", 4 - (length % 4));
        }
        length = secureString.length()/4;

        // Split security key in 4 parts
        String[] parts = new String[4];
        for (int i = 0; i < 4; i++) {
            parts[i] = secureString.substring(i * length, (i + 1) * length);
        }

        // Reconstruct security key in order 2, 0, 3, 1
        String newSecurityKey = parts[2] + parts[0] + parts[3] + parts[1];

        // Change position of some odd positions
        char[] chars = newSecurityKey.toCharArray();
        for (int i = 1; i < chars.length; i += 4) {
            char aux = chars[i];
            chars[i] = chars[i - 1];
            chars[i - 1] = aux;
        }

        return new String(chars);
    }

    public static String decodeSecureString(String encodedSecureString) {
        // Change position of some odd positions
        char[] chars = encodedSecureString.toCharArray();
        for (int i = 1; i < chars.length; i += 4) {
            char aux = chars[i];
            chars[i] = chars[i - 1];
            chars[i - 1] = aux;
        }

        // Reconstruct security key in correct order
        String newSecurityKey = new String(chars);
        int length = newSecurityKey.length()/4;
        String[] parts = new String[4];
        for (int i = 0; i < 4; i++) {
            parts[i] = newSecurityKey.substring(i * length, (i + 1) * length);
        }
        String securityKey = parts[1] + parts[3] + parts[0] + parts[2];

        // Remove trailing repeated '¬' character (if any)
        return securityKey.replaceAll("¬+$", "");
    }

}
