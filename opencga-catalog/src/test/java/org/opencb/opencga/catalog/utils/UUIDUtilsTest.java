package org.opencb.opencga.catalog.utils;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Random;

import org.junit.Test;

import io.jsonwebtoken.impl.Base64UrlCodec;

public class UUIDUtilsTest {
	
	@Test
	public void checkSingleCGAUUID() throws ParseException {
	    Date date = (new SimpleDateFormat("dd/MM/yyyy")).parse("28/02/1953");
	    UUIDUtils.Entity entity = UUIDUtils.Entity.INDIVIDUAL;
	    String uuid = UUIDUtils.generateOpenCGAUUID(entity, date);
	    String uuidStr = new String(Base64UrlCodec.BASE64URL.decodeToString(uuid));
	    String uuidStrToCompare = "431c8d80-ff84-0006-0001-";
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
            String uuidStr = new String(Base64UrlCodec.BASE64URL.decodeToString(uuid));            
            assertEquals(uuidStr.substring(0, 24), uuidStrToCompare);
        }
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
