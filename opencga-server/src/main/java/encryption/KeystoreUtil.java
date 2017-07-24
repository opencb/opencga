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

package encryption;

import com.google.common.io.BaseEncoding;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.security.*;
import java.security.cert.CertificateException;

public class KeystoreUtil {

    private static final Logger LOG = LoggerFactory.getLogger(KeystoreUtil.class);

    public static Key getKeyFromKeyStore(final String keystoreLocation, final String keystorePass, final String alias, final String keyPass) {
        try {

            InputStream keystoreStream = new FileInputStream(keystoreLocation);

            KeyStore keystore = KeyStore.getInstance("JCEKS");

            keystore.load(keystoreStream, keystorePass.toCharArray());

            LOG.debug("Keystore with alias {} found == {}", alias, keystore.containsAlias(alias));
            if (!keystore.containsAlias(alias)) {
                throw new RuntimeException("Alias for key not found");
            }

            Key key = keystore.getKey(alias, keyPass.toCharArray());
            LOG.debug("Key Found {} -> {}", key.getAlgorithm(), BaseEncoding.base64().encode(key.getEncoded()));

            return key;

        } catch (UnrecoverableKeyException | KeyStoreException | CertificateException | IOException | NoSuchAlgorithmException e) {
            throw new RuntimeException(e);
        }
    }


}
