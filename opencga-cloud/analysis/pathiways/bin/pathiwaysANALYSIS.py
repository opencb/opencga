#!/usr/bin/python
from optparse import OptionParser
from time import localtime, strftime
from resultFile import resultFile
import sys, os, commands


def execute(cmd):
    status, output = commands.getstatusoutput(cmd)
    print(str(status)+" -> "+output+" - "+cmd)
    

def main():
    """
    main function
    """
    opts = get_options()
    
    homePath = os.path.dirname(sys.argv[0])
    
    #WATCH OUT with this parameter, indicates that the nomalizematrix param is not a normalized matrix is a CEL compressed file.
    #It must be build from the zip file provided in the nomalizematrix param
    if "true" in opts.celfileFlag:
      print(opts.celfileFlag)
      print(opts.celornormalizedmatrix)
      matrixcelOutdir = opts.outdir+"matrixcelOutdir"
      celfileName = os.path.splitext(os.path.basename(opts.celornormalizedmatrix))[0]
      normMatrix = celfileName+"_normmatrix.txt"
      normMatrixImage = celfileName+"_normmatrix.png"
      matrixDir = opts.outdir
           
      execute("/httpd/bioinfo/babelomics4.3/babelomics.sh --tool affy-expression-normalization -o "+matrixcelOutdir+" --compressed-file "+opts.celornormalizedmatrix+" --compressed-file-tags affymetrix --rma")
      execute("cp "+matrixcelOutdir+"/rma.summary.txt "+matrixDir+""+normMatrix)
      execute("cp "+matrixcelOutdir+"/rma.summary.png "+matrixDir+""+normMatrixImage)
      execute("rm -rf "+matrixcelOutdir)
      
      opts.celornormalizedmatrix = matrixDir+""+normMatrix;
      print(opts.celornormalizedmatrix);

    command = "/opt/R/R-2.15.2/bin/Rscript "+homePath+"/pathiwaysMAIN.r "+homePath+" "+opts.pathways+" "+opts.celornormalizedmatrix+" "+opts.summ+" "+opts.experimentaldesign+" "+opts.control+" "+opts.disease+" "+opts.outdir+" "+opts.platform+" "+opts.expname+" "+opts.test+" "+opts.paired
    execute(command)

    # Get date
    today = strftime("%Y-%m-%d", localtime())
    today_status = strftime("%a %b %d %H:%M:%S CET %Y", localtime())
    today_xml = strftime("%Y-%m-%d", localtime())

    # Create result.xml
    xml = resultFile()
    xml.addMetadataItem("version", "SDK version", "MESSAGE", "", "", "", "", "1.0")
    xml.addMetadataItem("date", "Job date", "MESSAGE", "", "", "", "", today_xml)
    xml.addInputItem("tool", "Tool name", "MESSAGE", "", "", "", "", "Pathiways")
    #~ xml.addInputItem("input", "Input file", "MESSAGE", "", "", "", "", "input _file_name")
    #~ xml.addInputItem("option1", "Option 1", "MESSAGE", "", "", "", "", "option1_value")

    pathwayDict = {
    '03320':'PPAR SIGNALING PATHWAY',
    '04010':'MAPK SIGNALING PATHWAY',
    '04012':'ERBB SIGNALING PATHWAY',
    '04020':'CALCIUM SIGNALING PATHWAY',
    '04060':'CITOKINE-CYTOKINE RECEPTOR INTERACTION',
    '04062':'CHEMOKINE SIGNALING PATHWAY',
    '04080':'NEUROACTIVE LIGAND-RECEPTOR INTERACTION',
    '04110':'CELL CYCLE',
    '04115':'p53 SIGNALING PATHWAY',
    '04150':'mTOR SIGNALING PATHWAY',
    '04210':'APOPTOSIS',
    '04310':'WNT SIGNALING PATHWAY',
    '04330':'NOTCH SIGNALING PATHWAY',
    '04340':'HEDGEHOG SIGNALING PATHWAY',
    '04350':'TGF-BETA SIGNALING PATHWAY',
    '04370':'VEGF SIGNALING PATHWAY',
    '04510':'FOCAL ADHESION',
    '04512':'ECM-RECEPTOR INTERACTION',
    '04514':'CELL ADHESION MOLECULES',
    '04520':'ADHERENS JUNCTION',
    '04530':'TIGHT JUNCTION',
    '04540':'GAP JUNCTION',
    '04610':'COMPLEMENT AND COAGULATION CASCADES',
    '04612':'ANTIGEN PROCESING AND PRESENTATION',
    '04620':'TOLL-LIKE RECEPTOR SIGNALING PATHWAY',
    '04630':'JAK-STAT SIGNALING PATHWAY',
    '04650':'NATURAL CELL MEDIATED CYTOTOXICITY',
    '04660':'T CELL RECEPTOR SIGNALING PATHWAY',
    '04662':'B CELL RECEPTOR SIGNALING PATHWAY',
    '04664':'Fc EPSILON RI SIGNALING PATHWAY',
    '04670':'LEUKOCYTE TRANSENDOTHELIAL MIGRATION',
    '04720':'LONG-TERM POTENTIATION',
    '04730':'LONG-TERM DEPRESSION',
    '04910':'INSULIN SIGNALING PATHWAY',
    '04912':'GnRH SIGNALING PATHWAY',
    '04916':'MELANOGENESIS',
    '04920':'ADIPOCYTOKINE SIGNALING PATHWAY'
    }
    fileList = os.listdir(opts.outdir)
    fileList = sorted(fileList, key = lambda x: x[:-4])
    for fileItem in fileList:
        if opts.expname in fileItem:
            name = fileItem.split('.')[0]
            group = ""
            pathwayKey = name[len(opts.expname)+4:]
            if pathwayKey in pathwayDict:
                group = pathwayDict[pathwayKey]
            if "ALL.txt" in fileItem:
                #~ addOutputItem(self, name, title, type, tags, style, group, context, content)
                xml.addOutputItem("output", "Summary", "FILE", "TABLE,PATHIWAY_SUMMARY_TABLE", "", "Summary", "", fileItem)
                with open(opts.outdir+fileItem, "r+") as f:
                    old = f.read()
                    f.seek(0)
                    f.write("#" + old)
                    f.close()
                continue
            if ".txt" in fileItem:
                xml.addOutputItem("output", "Individual", "FILE", "TABLE,PATHIWAY_INDIVIDUAL_TABLE", "", group, "", fileItem)
                with open(opts.outdir+fileItem, "r+") as f:
                    old = f.read()
                    f.seek(0)
                    f.write("#" + old)
                    f.close()
                continue
            if ".jpeg" in fileItem:
                xml.addOutputItem("output", "Image", "IMAGE", "", "", group, "", fileItem)

    xml.save(opts.outdir)

def get_options():
    parser = OptionParser(
        version='Pathiways 1.0',
        usage="%prog [options]")
    parser.add_option('--pathways', dest='pathways', metavar="STRING",
                      help='list of pathways')
    parser.add_option('--norm-matrix', dest='celornormalizedmatrix', metavar="FILE",
                      help='path to normalized matrix')
    parser.add_option('--cel-compressed-file', dest='celfileFlag', metavar="FILE",
                      help='cell files must be converted in to normalized file')
    parser.add_option('--summ', dest='summ', metavar="STRING",
                      help='summ value')
    parser.add_option('--exp-design', dest='experimentaldesign', metavar="FILE",
                      help='path to experimental design')
    parser.add_option('--control', dest='control', metavar="STRING",
                      help='control value')
    parser.add_option('--disease', dest='disease', metavar="STRING",
                      help='disease value')
    parser.add_option('--outdir', dest='outdir', metavar="PATH",
                      help='output directory')
    parser.add_option('--platform', dest='platform', metavar="STRING",
                      help='platform value')
    parser.add_option('--exp-name', dest='expname', metavar="STRING",
                      help='experiment name')
    parser.add_option('--test', dest='test', metavar="STRING",
                      help='Comparison tests')
    parser.add_option('--paired', dest='paired', metavar="STRING",
                      help='Wilcoxon comparison test can be paired or not')

    opts = parser.parse_args()[0]

    #~ if len(args)<10:
        #~ exit(parser.print_help())

    if not opts.outdir.endswith('/'):
        opts.outdir += '/'

    return opts


if __name__ == "__main__":
    exit(main())
