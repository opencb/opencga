package org.opencb.opencga.catalog.utils;

import org.junit.Test;
import org.opencb.opencga.catalog.exceptions.CatalogException;

import java.io.UnsupportedEncodingException;

import static org.junit.Assert.*;

public class UUIDUtilsTest {

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