package org.opencb.opencga.storage.datamanagers;

import org.opencb.opencga.storage.TabixReader;

import java.io.File;
import java.io.IOException;

public class FeatureManager {
    private static StringBuilder directory;
    private static StringBuilder tmpDir;
    private static StringBuilder fileName;
    Runtime execute = Runtime.getRuntime();
    Process process;

    @SuppressWarnings("static-access")
    public FeatureManager(String directory, String fileName) {
        this.directory = new StringBuilder(directory);
        this.tmpDir = new StringBuilder(directory).append("tmp/");
        this.fileName = new StringBuilder(fileName.substring(0, fileName.lastIndexOf(".")));
    }

    public String getByRegion(final String fileName, final String chr, final int start, final int end)
            throws IOException {
        // FileUtils.checkFile(fileName);
        if (fileName.endsWith("gff") || fileName.toString().endsWith("gff.gz")) {
            return getByRegionGff(fileName, chr, start, end);
        }

        if (fileName.endsWith("bed") || fileName.toString().endsWith("bed.gz")) {

            return getByRegionBed(fileName, chr, start, end);
        }

        return "Format unknown";
    }

    private String getByRegionBed(String fileName, final String chr, final int start, final int end) {
        StringBuilder infoChrom = new StringBuilder(chr).append(":").append(start).append(":").append(end);

        if (!new File(new StringBuilder(fileName.substring(0, fileName.lastIndexOf("."))).append("sort.gz.tbi")
                .toString()).exists()) {
            if (fileName.toString().endsWith("gz")) {
                if (descGz(/* fileName */)) {
                    if (process("bed"/* fileName */))
                        moveTempToDir(/* fileName */);
                }
            } else {
                if (process("bed"/* fileName */))
                    moveTempToDir(/* fileName */);
            }
        }

        return "";// tabixFind(infoChrom.toString()/* , fileName */);
    }

    private boolean process(String typeFile/* String fileName */) {
        System.out.println("ESTAMOS EN PROCESSGFF");
        boolean processOK = true;

        try {
            // Sorted
            System.out.println("ESTAMOS ORDENANDO");
            String operation = null;
            if (typeFile.equals("gff")) {
                operation = "sort -k1,1 -k4,4n ";
            } else {
                operation = "sort -k1,1 -k2,2n ";
            }
            StringBuilder commandToExecute = new StringBuilder(operation).append(tmpDir).append(fileName).append(" > ")
                    .append(tmpDir).append(fileName).append(".sort");
            String[] command = {"/bin/bash", "-c", commandToExecute.toString()};
            System.out.println(commandToExecute.toString());

            process = execute.exec(command);

            process.waitFor();
            System.out.println("HA SALIDO CON " + process.exitValue());
            if (process.exitValue() != 0)
                processOK = false;

            // remove
            System.out.println("ESTAMOS ORDENANDO");
            commandToExecute = new StringBuilder("rm -f ").append(tmpDir).append(fileName);
            command[2] = commandToExecute.toString();
            System.out.println(commandToExecute.toString());

            process = execute.exec(command);

            process.waitFor();
            System.out.println("HA SALIDO CON " + process.exitValue());
            if (process.exitValue() != 0)
                processOK = false;

            // compress
            System.out.println("ESTAMOS COMPRIMIENDO");
            commandToExecute = new StringBuilder("/opt/tabix/bgzip ").append(tmpDir).append(fileName).append(".sort");
            command[2] = commandToExecute.toString();
            System.out.println(commandToExecute.toString());
            process = execute.exec(command);
            process.waitFor();
            System.out.println("HA SALIDO CON " + process.exitValue());
            if (process.exitValue() != 0)
                processOK = false;

            // Index
            System.out.println("ESTAMOS INDEXANDO");
            commandToExecute = new StringBuilder("/opt/tabix/tabix -p ").append(typeFile).append(" ").append(tmpDir)
                    .append(fileName).append(".sort.gz");
            command[2] = commandToExecute.toString();
            System.out.println(commandToExecute.toString());
            process = execute.exec(command);
            process.waitFor();
            System.out.println("HA SALIDO CON " + process.exitValue());
            if (process.exitValue() != 0)
                processOK = false;

            // rename Index
            // commandToExecute = new StringBuilder("cp ").append(directory)
            // .append(fileName).append(".ord.gz.tbi ").append(directory)
            // .append(fileName).append(".ord.gz.idx");
            // command[2] = commandToExecute.toString();
            // System.out.println(commandToExecute.toString());
            // process = execute.exec(command);
            // process.waitFor();
            // if (process.exitValue() != 0)
            // processOK = false;

            // remove tbi
            // commandToExecute = new StringBuilder("rm ").append(directory)
            // .append(fileName).append(".ord.gz.tbi");
            // command[2] = commandToExecute.toString();
            // System.out.println(commandToExecute.toString());
            // process = execute.exec(command);
            // process.waitFor();
            // if (process.exitValue() != 0)
            // processOK = false;

        } catch (IOException e) {
            e.printStackTrace();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

        return processOK;
    }

    private boolean descGz(/* String fileName */) {
        System.out.println("ESTAMOS EN DESCGZ");
        boolean processOK = true;
        // StringBuilder tempDir = directory.append("tmp/");
        try {
            // descomprime
            StringBuilder commandToExecute = new StringBuilder("gzip -cd ").append(tmpDir).append(fileName)
                    .append(".gz").append(" > ").append(tmpDir).append(fileName);
            String[] command = {"/bin/bash", "-c", commandToExecute.toString()};

            System.out.println(commandToExecute.toString());

            process = execute.exec(command);

            process.waitFor();
            System.out.println("HA SALIDO CON " + process.exitValue());
            if (process.exitValue() != 0)
                processOK = false;

        } catch (IOException e) {
            e.printStackTrace();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

        return processOK;
    }

    private boolean moveTempToDir(/* String fileName */) {
        System.out.println("ESTAMOS EN MOVETEMTODIR");
        // check file
        boolean fileNotExist = true;
        StringBuilder allFiles = new StringBuilder();
        try {
            // StringBuilder tmpdir = directory.append("tmp");
            File f = new File(tmpDir.toString());
            System.out.println(tmpDir.toString());
            File[] ficheros = f.listFiles();
            for (int i = 0; i < ficheros.length; i++) {
                System.out.println("VIENDO SI EXISTE -->" + ficheros[i] + " "
                        + new File(new StringBuilder(directory.toString() + ficheros[i]).toString()).exists());
                if (new File(new StringBuilder(directory.toString() + ficheros[i]).toString()).exists()) {
                    fileNotExist = false;
                    break;
                } else
                    allFiles.append(ficheros[i]).append(" ");
            }

            if (fileNotExist) {
                // move to dir
                System.out.println("LOS FICHEROS SON: " + allFiles.toString());
                StringBuilder commandToExecute = new StringBuilder("mv ").append(allFiles).append(directory);

                String[] command = {"/bin/bash", "-c", commandToExecute.toString()};
                System.out.println(commandToExecute.toString());
                process = execute.exec(command);

                process.waitFor();

                if (process.exitValue() != 0)
                    fileNotExist = false;

            }

        } catch (InterruptedException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }

        return true;
    }

    private String tabixFind(String infoChrom/* , String fileName */) {
        System.out.println("ESTAMOS EN TABIXFIND");
        StringBuilder strbuild = new StringBuilder();
        StringBuilder tabixFile = new StringBuilder(directory.toString() + fileName.toString() + ".sort.gz");
        try {
            TabixReader tb = new TabixReader(tabixFile.toString());

            TabixReader.Iterator iter = tb.query(infoChrom);

            String linea = "";
            while (iter != null && (linea = iter.next()) != null) {
                strbuild.append(linea).append("\n");
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
        return strbuild.toString();
    }

    private String getByRegionGff(String fileName, final String chr, final int start, final int end) {
        StringBuilder infoChrom = new StringBuilder(chr).append(":").append(start).append(":").append(end);

        if (!new File(new StringBuilder(fileName.substring(0, fileName.lastIndexOf("."))).append("sort.gz.tbi")
                .toString()).exists()) {
            if (fileName.toString().endsWith("gz")) {
                if (descGz(/* fileName */)) {
                    if (process("gff"/* fileName */))
                        moveTempToDir(/* fileName */);
                }
            } else {
                if (process("gff"/* fileName */))
                    moveTempToDir(/* fileName */);
            }
        }

        return "";// tabixFind(infoChrom.toString()/* , fileName */);
    }

}
