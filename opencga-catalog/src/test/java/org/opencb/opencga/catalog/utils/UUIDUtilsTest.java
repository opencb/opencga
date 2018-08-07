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

public class UUIDUtilsTest {
	
	@Test
	public void checkSingleCGAUUID() throws ParseException {		
		SimpleDateFormat formatter = new SimpleDateFormat("dd/MM/yyyy zzz");
		Date date = formatter.parse("28/02/1953 GMT");    	
	    UUIDUtils.Entity entity = UUIDUtils.Entity.INDIVIDUAL;
	    String uuid = UUIDUtils.generateOpenCGAUUID(entity, date);
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
            UUIDUtils.Entity entity = UUIDUtils.Entity.SAMPLE;
            String timeStr = Long.toHexString(randomDate.getTime());
            timeStr = String.format("%1$16s", timeStr).replace(' ', '0');            
            String timeStrLow = timeStr.substring(timeStr.length()-8);
            String timeStrMid = timeStr.substring(timeStr.length()-12, timeStr.length()-8);
            String uuidStrToCompare = timeStrLow + "-" + timeStrMid + "-" + "0004-0001-";
            String uuid = UUIDUtils.generateOpenCGAUUID(entity, randomDate);
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

        String s = UUIDUtils.generateOpenCGAUUID(UUIDUtils.Entity.SAMPLE);
        assertTrue(UUIDUtils.isOpenCGAUUID(s));
        System.out.println("s = " + s);
        s = UUIDUtils.generateOpenCGAUUID(UUIDUtils.Entity.SAMPLE);
        assertTrue(UUIDUtils.isOpenCGAUUID(s));
        System.out.println("s = " + s);
        s = UUIDUtils.generateOpenCGAUUID(UUIDUtils.Entity.SAMPLE);
        assertTrue(UUIDUtils.isOpenCGAUUID(s));
        System.out.println("s = " + s);
        s = UUIDUtils.generateOpenCGAUUID(UUIDUtils.Entity.SAMPLE);
        assertTrue(UUIDUtils.isOpenCGAUUID(s));
        System.out.println("s = " + s);
        s = UUIDUtils.generateOpenCGAUUID(UUIDUtils.Entity.SAMPLE);
        assertTrue(UUIDUtils.isOpenCGAUUID(s));
        System.out.println("s = " + s);
        s = UUIDUtils.generateOpenCGAUUID(UUIDUtils.Entity.SAMPLE);
        assertTrue(UUIDUtils.isOpenCGAUUID(s));
        System.out.println("s = " + s);
        s = UUIDUtils.generateOpenCGAUUID(UUIDUtils.Entity.SAMPLE);
        assertTrue(UUIDUtils.isOpenCGAUUID(s));
        System.out.println("s = " + s);
        s = UUIDUtils.generateOpenCGAUUID(UUIDUtils.Entity.SAMPLE);
        assertTrue(UUIDUtils.isOpenCGAUUID(s));
        System.out.println("s = " + s);
        s = UUIDUtils.generateOpenCGAUUID(UUIDUtils.Entity.SAMPLE);
        assertTrue(UUIDUtils.isOpenCGAUUID(s));
        System.out.println("s = " + s);
        s = UUIDUtils.generateOpenCGAUUID(UUIDUtils.Entity.SAMPLE);
        assertTrue(UUIDUtils.isOpenCGAUUID(s));
        System.out.println("s = " + s);
        s = UUIDUtils.generateOpenCGAUUID(UUIDUtils.Entity.SAMPLE);
        assertTrue(UUIDUtils.isOpenCGAUUID(s));
        System.out.println("s = " + s);

    }
}
