package org.opencb.opencga.storage.alignment;

import org.junit.Test;
import org.opencb.commons.bioformats.alignment.Alignment;
import org.opencb.commons.test.GenericTest;

import java.util.HashMap;
import java.util.Map;

/**
 * Created with IntelliJ IDEA.
 * User: jacobo
 * Date: 9/04/14
 * Time: 11:15
 * To change this template use File | Settings | File Templates.
 */
public class AlignmentRegionSummaryTest extends GenericTest {

    @Test
    public void summaryTest(){
        AlignmentRegionSummary summary = new AlignmentRegionSummary(2);


        Alignment alignment = new Alignment();
        Map<String, Object> att = new HashMap<>();

        alignment.setFlags(50);
        alignment.setLength(100);
        alignment.setMateReferenceName("=");
        alignment.setAttributes(new HashMap<String, Object>());
        alignment.addAttribute("uno", "1a");
        alignment.addAttribute("dos", "2");
        summary.addAlignment(alignment);

        alignment.setFlags(50);
        alignment.setLength(100);
        alignment.setMateReferenceName("=");
        alignment.setAttributes(new HashMap<String, Object>());
        alignment.addAttribute("uno", "1b");
        alignment.addAttribute("tres", "3");
        summary.addAlignment(alignment);

        alignment.setFlags(60);
        alignment.setLength(60);
        alignment.setMateReferenceName("=");
        alignment.setAttributes(new HashMap<String, Object>());
        alignment.addAttribute("uno", "1c");
        alignment.addAttribute("dos", "2");
        alignment.addAttribute("tres", "3");
        summary.addAlignment(alignment);

        alignment.setFlags(70);
        alignment.setLength(70);
        alignment.setMateReferenceName("X");
        alignment.setAttributes(new HashMap<String, Object>());
        alignment.addAttribute("uno", "1d");
        alignment.addAttribute("dos", "2");
        summary.addAlignment(alignment);


        System.out.println("Close");
        summary.printHistogram();
        summary.close();
        System.out.println("Solutions: ");

        System.out.println("Expected Length : " + 100 + " vs " + summary.getDefaultLen());
        System.out.println("Expected Flag : " + 50 + " vs " + summary.getDefaultFlag());
        System.out.println("Expected RNext : " + "=" + " vs " + summary.getDefaultRNext());




    }

}
