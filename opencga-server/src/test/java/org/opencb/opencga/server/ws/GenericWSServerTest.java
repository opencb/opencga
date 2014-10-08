package org.opencb.opencga.server.ws;

import com.fasterxml.jackson.core.JsonProcessingException;
import encryption.AESCipher;
import encryption.KeyEncoding;
import encryption.KeystoreUtil;
import org.junit.Test;


import java.io.FileNotFoundException;
import java.io.IOException;
import java.security.Key;
import java.util.Properties;
import static org.junit.Assert.*;



public class GenericWSServerTest {
    String keyStoreFileLocation, storePass, alias, keyPass, iv;
    String message = "this is message";

//    @Test
    public void testCipherWithoutIv() {

        loadCredentials();
        Key keyFromKeyStore = KeystoreUtil.getKeyFromKeyStore(keyStoreFileLocation, storePass, alias, keyPass);

        AESCipher cipher = new AESCipher(keyFromKeyStore);
        /** Show key **/
        showKey(cipher);

        String encryptedMessage = cipher.getEncryptedMessage(message);
        String decryptedMessage = cipher.getDecryptedMessage(encryptedMessage);

        System.out.println(encryptedMessage);
        System.out.println(decryptedMessage);
        assertEquals(message, decryptedMessage);
    }
//    @Test
    public void testCipherWithIv() {

        loadCredentials();
        Key keyFromKeyStore = KeystoreUtil.getKeyFromKeyStore(keyStoreFileLocation, storePass, alias, keyPass);

        AESCipher cipher = new AESCipher(keyFromKeyStore);

        /** Show key **/
        showKey(cipher);

        AESCipher cipherWithIv = new AESCipher(keyFromKeyStore, iv.getBytes());
        String encryptedMessage = cipherWithIv.getEncryptedMessage(message);
        String decryptedMessage = cipherWithIv.getDecryptedMessage(encryptedMessage);

        System.out.println(encryptedMessage);
        System.out.println(decryptedMessage);
        assertEquals(message, decryptedMessage);

    }
    private void loadCredentials(){
        try{
            keyStoreFileLocation = getClass().getClassLoader().getResource("aes-keystore.jck").getFile();

            Properties prop = new Properties();
            prop.load(getClass().getClassLoader().getResourceAsStream("aes.properties"));
            storePass = prop.get("storepass").toString();
            alias = prop.get("alias").toString();
            keyPass = prop.get("keypass").toString();
            iv = prop.get("IV").toString();

        } catch (JsonProcessingException e) {
            e.printStackTrace();
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private void showKey(AESCipher cipher) {

        System.out.println("\n\nPrint SecretPrivateKey from JCEKS Keystore\n===========================================");
        System.out.println("Key (Base64 Encoded): " + cipher.getKey(KeyEncoding.BASE64));
        System.out.println("Key (Hex Encoded): " + cipher.getKey(KeyEncoding.HEX));
        System.out.println("Key (Base32 Encoded): " + cipher.getKey(KeyEncoding.BASE32));

    }

}