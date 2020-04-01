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

import io.jsonwebtoken.impl.Base64UrlCodec;
import org.junit.Test;

import java.nio.ByteBuffer;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Random;
import java.util.UUID;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class UuidUtilsTest {
	
	@Test
	public void checkSingleCGAUUID() throws ParseException {		
		SimpleDateFormat formatter = new SimpleDateFormat("dd/MM/yyyy zzz");
		Date date = formatter.parse("28/02/1953 GMT");    	
	    UuidUtils.Entity entity = UuidUtils.Entity.INDIVIDUAL;
	    String uuid = UuidUtils.generateOpenCgaUuid(entity, date);
	    String uuidStr = decodeOpenCGAUUIDtoUUID(uuid);
	    String uuidStrToCompare = "43537c00-ff84-0006-0001-";
	    assertEquals(uuidStr.substring(0, uuidStrToCompare.length()), uuidStrToCompare);
    }

    @Test
    public void checkRandomCGAUUID() {
        Random random = new Random();
        final int NUM_TRIALS = 10000;
        for (int i=0; i<NUM_TRIALS; ++i) {
            Date randomDate = new Date(random.nextInt());
            UuidUtils.Entity entity = UuidUtils.Entity.SAMPLE;
            String timeStr = Long.toHexString(randomDate.getTime());
            timeStr = String.format("%1$16s", timeStr).replace(' ', '0');            
            String timeStrLow = timeStr.substring(timeStr.length()-8);
            String timeStrMid = timeStr.substring(timeStr.length()-12, timeStr.length()-8);
            String uuidStrToCompare = timeStrLow + "-" + timeStrMid + "-" + "0004-0001-";
            String uuid = UuidUtils.generateOpenCgaUuid(entity, randomDate);
            String uuidStr = decodeOpenCGAUUIDtoUUID(uuid);
            assertEquals(uuidStr.substring(0, 24), uuidStrToCompare);
        }
    }

    private String decodeOpenCGAUUIDtoUUID(String opencgaUUID) {
        ByteBuffer buffer = ByteBuffer.allocate(Long.BYTES * 2);
        buffer.put(Base64UrlCodec.BASE64URL.decode(opencgaUUID));
        buffer.flip(); //need flip
        long mostSignificantBits = buffer.getLong();
        long leastSignificantBits = buffer.getLong();

        return new UUID(mostSignificantBits, leastSignificantBits).toString();
    }

    @Test
    public void generateOpenCGAUUID() {

        String s = UuidUtils.generateOpenCgaUuid(UuidUtils.Entity.SAMPLE);
        assertTrue(UuidUtils.isOpenCgaUuid(s));
        System.out.println("s = " + s);
        s = UuidUtils.generateOpenCgaUuid(UuidUtils.Entity.SAMPLE);
        assertTrue(UuidUtils.isOpenCgaUuid(s));
        System.out.println("s = " + s);
        s = UuidUtils.generateOpenCgaUuid(UuidUtils.Entity.SAMPLE);
        assertTrue(UuidUtils.isOpenCgaUuid(s));
        System.out.println("s = " + s);
        s = UuidUtils.generateOpenCgaUuid(UuidUtils.Entity.SAMPLE);
        assertTrue(UuidUtils.isOpenCgaUuid(s));
        System.out.println("s = " + s);
        s = UuidUtils.generateOpenCgaUuid(UuidUtils.Entity.SAMPLE);
        assertTrue(UuidUtils.isOpenCgaUuid(s));
        System.out.println("s = " + s);
        s = UuidUtils.generateOpenCgaUuid(UuidUtils.Entity.SAMPLE);
        assertTrue(UuidUtils.isOpenCgaUuid(s));
        System.out.println("s = " + s);
        s = UuidUtils.generateOpenCgaUuid(UuidUtils.Entity.SAMPLE);
        assertTrue(UuidUtils.isOpenCgaUuid(s));
        System.out.println("s = " + s);
        s = UuidUtils.generateOpenCgaUuid(UuidUtils.Entity.SAMPLE);
        assertTrue(UuidUtils.isOpenCgaUuid(s));
        System.out.println("s = " + s);
        s = UuidUtils.generateOpenCgaUuid(UuidUtils.Entity.SAMPLE);
        assertTrue(UuidUtils.isOpenCgaUuid(s));
        System.out.println("s = " + s);
        s = UuidUtils.generateOpenCgaUuid(UuidUtils.Entity.SAMPLE);
        assertTrue(UuidUtils.isOpenCgaUuid(s));
        System.out.println("s = " + s);
        s = UuidUtils.generateOpenCgaUuid(UuidUtils.Entity.SAMPLE);
        assertTrue(UuidUtils.isOpenCgaUuid(s));
        System.out.println("s = " + s);

    }
}
