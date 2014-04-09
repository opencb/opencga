package org.opencb.opencga.storage.alignment;

import org.junit.Test;
import org.opencb.commons.bioformats.alignment.Alignment;
import org.opencb.commons.test.GenericTest;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.assertEquals;

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
        AlignmentRegionSummary summary = new AlignmentRegionSummary(42);

        Alignment[] alignments = new Alignment[4];


        alignments[0] = new Alignment();
        alignments[0].setFlags(50);
        alignments[0].setLength(100);
        alignments[0].setMateReferenceName("=");
        alignments[0].setAttributes(new HashMap<String, Object>());
        alignments[0].addAttribute("AS", 1);
        alignments[0].addAttribute("NM", "asdfasdfasdf");
        summary.addAlignment(alignments[0]);

        alignments[1] = new Alignment();
        alignments[1].setFlags(50);
        alignments[1].setLength(100);
        alignments[1].setMateReferenceName("=");
        alignments[1].setAttributes(new HashMap<String, Object>());
        alignments[1].addAttribute("AS", 2);
        alignments[1].addAttribute("NH", 'C');
        summary.addAlignment(alignments[1]);

        alignments[2] = new Alignment();
        alignments[2].setFlags(60);
        alignments[2].setLength(60);
        alignments[2].setMateReferenceName("=");
        alignments[2].setAttributes(new HashMap<String, Object>());
        alignments[2].addAttribute("AS", 2);
        alignments[2].addAttribute("NM", "fdsafsdf");
        alignments[2].addAttribute("NH", 'c');
        summary.addAlignment(alignments[2]);

        alignments[3] = new Alignment();
        alignments[3].setFlags(70);
        alignments[3].setLength(70);
        alignments[3].setMateReferenceName("X");
        alignments[3].setAttributes(new HashMap<String, Object>());
        alignments[3].addAttribute("AS", 3);
        alignments[3].addAttribute("NM", "64asdf321asdfXX");
        summary.addAlignment(alignments[3]);


        System.out.println("Close");
        summary.printHistogram();
        summary.close();
        System.out.println("Solutions: ");

        assertEquals("Expected Length", 100, summary.getDefaultLen());
        assertEquals("Expected Flag", 50, summary.getDefaultFlag());
        assertEquals("Expected RNext", "=", summary.getDefaultRNext());


        for(int i = 0; i < alignments.length; i++){
            assertEquals(
                    summary.getTagsFromList(summary.getIndexTagList(alignments[i].getAttributes())),
                    alignments[i].getAttributes());
        }

    }

}
