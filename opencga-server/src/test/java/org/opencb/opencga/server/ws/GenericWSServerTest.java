package org.opencb.opencga.server.ws;

import encryption.AESCipher;
import encryption.KeyEncoding;
import encryption.KeystoreUtil;
import encryption.MainApp;
import org.junit.Test;


import java.security.Key;



public class GenericWSServerTest {


    @Test
    public void testCipherWithoutIv() {


        String keystoreFileLocation = "/home/ralonso/keystore/aes-keystore.jck";
        String storePass = "mystorepass";
        String alias = "prueba";
        String keyPass = "passprueba";
        Key keyFromKeyStore = KeystoreUtil.getKeyFromKeyStore(keystoreFileLocation, storePass, alias, keyPass);

        AESCipher cipher = new AESCipher(keyFromKeyStore);
        /** Show key **/
        showKey(cipher);

        String encryptedMessage = cipher.getEncryptedMessage("this is message");
        String decryptedMessage = cipher.getDecryptedMessage(encryptedMessage);

        System.out.println(encryptedMessage);
        System.out.println(decryptedMessage);

    }
    @Test
    public void testCipherWithIv() {

        String keystoreFileLocation = "/home/ralonso/keystore/aes-keystore.jck";
        String storePass = "mystorepass";
        String alias = "prueba";
        String keyPass = "passprueba";
        Key keyFromKeyStore = KeystoreUtil.getKeyFromKeyStore(keystoreFileLocation, storePass, alias, keyPass);

        AESCipher cipher = new AESCipher(keyFromKeyStore);

        /** Show key **/
        showKey(cipher);

        AESCipher cipherWithIv = new AESCipher(keyFromKeyStore, "0123456789012345".getBytes());
        String encryptedMessage = cipherWithIv.getEncryptedMessage("this is message");
        String decryptedMessage = cipherWithIv.getDecryptedMessage(encryptedMessage);

        System.out.println(encryptedMessage);
        System.out.println(decryptedMessage);

    }
    private static void showKey(AESCipher cipher) {

        System.out.println("\n\nPrint SecretPrivateKey from JCEKS Keystore\n===========================================");
        System.out.println("Key (Base64 Encoded): " + cipher.getKey(KeyEncoding.BASE64));
        System.out.println("Key (Hex Encoded): " + cipher.getKey(KeyEncoding.HEX));
        System.out.println("Key (Base32 Encoded): " + cipher.getKey(KeyEncoding.BASE32));

    }

}