package org.opencb.opencga.common;

import org.bioinfo.commons.io.utils.FileUtils;

import java.io.*;
import java.nio.charset.Charset;
import java.nio.file.*;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.ArrayList;
import java.util.List;
import java.util.regex.Pattern;
import java.util.zip.ZipEntry;
import java.util.zip.ZipOutputStream;

public class IOUtils {

    public static void deleteDirectory(Path path) throws IOException {

        Files.walkFileTree(path, new SimpleFileVisitor<Path>() {
            @Override
            public FileVisitResult visitFile(Path file, BasicFileAttributes attrs) throws IOException {
                Files.delete(file);
                return FileVisitResult.CONTINUE;
            }

            @Override
            public FileVisitResult visitFileFailed(Path file, IOException exc) throws IOException {
                // try to delete the file anyway, even if its attributes
                // could not be read, since delete-only access is
                // theoretically possible
                Files.delete(file);
                return FileVisitResult.CONTINUE;
            }

            @Override
            public FileVisitResult postVisitDirectory(Path dir, IOException exc) throws IOException {
                if (exc == null) {
                    Files.delete(dir);
                    return FileVisitResult.CONTINUE;
                } else {
                    // directory iteration failed; propagate exception
                    throw exc;
                }
            }
        });
    }

    public static List<String> head(Path path, int numLines) throws IOException {
        BufferedReader br = Files.newBufferedReader(path, Charset.defaultCharset());
        List<String> lines = new ArrayList<String>(numLines);
        String line;
        int cont = 0;
        while ((line = br.readLine()) != null && cont++ < numLines) {
            lines.add(line);
        }
        br.close();
        return lines;
    }

    public static void prependString(Path filePath, String text) throws IOException {
        Path tempPath = Paths.get(filePath + ".prepend.tmp");
        text = text.concat(System.lineSeparator());
        Files.createFile(tempPath);
        Files.write(tempPath, text.getBytes(), StandardOpenOption.APPEND);
        Files.write(tempPath, Files.readAllBytes(filePath), StandardOpenOption.APPEND);
        Files.move(tempPath, filePath, StandardCopyOption.REPLACE_EXISTING);
    }

    public static List<String> grep(BufferedReader bufferedReader, String pattern, boolean ignoreCase)
            throws IOException {
        Pattern pat;
        if (ignoreCase) {
            pat = Pattern.compile(pattern, Pattern.CASE_INSENSITIVE);
        } else {
            pat = Pattern.compile(pattern);
        }
        List<String> lines = new ArrayList<String>();
        String line = "";
        while ((line = bufferedReader.readLine()) != null) {
            if (pat.matcher(line).matches()) {
                lines.add(line);
            }
        }
        bufferedReader.close();
        return lines;
    }

    public static List<String> grep(Path path, String pattern) throws IOException {
        return grep(path, pattern, false);
    }

    public static List<String> grep(Path path, String pattern, boolean ignoreCase) throws IOException {
        return grep(Files.newBufferedReader(path, Charset.defaultCharset()), pattern, ignoreCase);
    }

    public static List<String> column(Path path, int numColumn, String fieldSeparatorRegEx, String pattern)
            throws IOException {
        return column(path, numColumn, fieldSeparatorRegEx, pattern, false);
    }

    public static List<String> column(Path path, int numColumn, String fieldSeparatorRegEx, String pattern,
                                      boolean ignoreCase) throws IOException {
        Pattern pat = null;
        if (pattern != null && !pattern.equalsIgnoreCase("")) {
            if (ignoreCase) {
                pat = Pattern.compile(pattern, Pattern.CASE_INSENSITIVE);
            } else {
                pat = Pattern.compile(pattern);
            }
        }
        BufferedReader br = Files.newBufferedReader(path, Charset.defaultCharset());
        List<String> list = new ArrayList<String>();
        String line = "";
        String[] fields;
        while ((line = br.readLine()) != null) {
            line = line.trim();
            if (!line.equals("") && pat != null && pat.matcher(line).matches()) {
                fields = line.split(fieldSeparatorRegEx, -1);
                if (numColumn >= fields.length) {
                    list.add(null);
                } else {
                    list.add(fields[numColumn]);
                }
            }
        }
        br.close();
        return list;
    }

    public static void zipFile(File file, File dest) throws IOException {
        zipFiles(new File[]{file}, dest);
    }

    public static void zipFiles(File[] files, File dest) throws IOException {
        FileUtils.checkDirectory(dest.getParentFile(), true); //
        // /mnt/commons/test/job.zip ---> ¿/mnt/commons/test exists?
        BufferedInputStream origin = null;
        int BUFFER_SIZE = 1024;
        byte[] data = new byte[BUFFER_SIZE];

        OutputStream destination = new FileOutputStream(dest);
        ZipOutputStream out = new ZipOutputStream(new BufferedOutputStream(destination));

        for (int i = 0; i < files.length; i++) {
            if (files[i].isFile()) {
                FileUtils.checkFile(files[i]);
                String filename = files[i].getAbsolutePath();
                files[i].getName();
                System.out.println("Adding: " + filename);
                FileInputStream fi = new FileInputStream(filename);
                origin = new BufferedInputStream(fi, BUFFER_SIZE);
                // Setup the entry in the zip file
                ZipEntry zipEntry = new ZipEntry(filename.substring(filename.lastIndexOf("/")));
                out.putNextEntry(zipEntry);
                // Read data from the source file and write it out to the zip
                // file
                int count;
                while ((count = origin.read(data, 0, BUFFER_SIZE)) != -1) {
                    out.write(data, 0, count);
                }
                // Close the source file
                origin.close();
            }
        }
        // Close the zip file
        out.close();
    }

    // zip avoiding some files
    public static void zipDirectory(File directory, File dest, ArrayList<String> avoidingFiles) throws IOException {
        ZipOutputStream zos = new ZipOutputStream(new FileOutputStream(dest));
        zipDirectoryRecursive(directory, directory.getName(), zos, avoidingFiles);
        zos.close();
    }

    private static void zipDirectoryRecursive(File directory, String zipPath, ZipOutputStream zos,
                                              ArrayList<String> avoidingFiles) throws IOException {
        try {
            System.out.println(directory.getAbsolutePath() + "::" + zipPath);
            // get a listing of the directory content
            String[] dirList = directory.list();
            byte[] readBuffer = new byte[2156];
            int bytesIn = 0;
            // loop through dirList, and zip the files
            for (int i = 0; i < dirList.length; i++) {
                File f = new File(directory, dirList[i]);
                System.err.println("avoiding: " + avoidingFiles);
                System.err.println("file: " + f.getName());
                System.err.println("and so: " + avoidingFiles.contains(f.getName()));
                if (!avoidingFiles.contains(f.getName())) {
                    if (f.isDirectory()) {
                        // if the File object is a directory, call this
                        // function again to add its content recursively
                        // String filePath = f.getPath();
                        // File file = new File(zipPath+"/"+f.getName());
                        zipDirectoryRecursive(new File(f.getPath()), zipPath + "/" + f.getName(), zos, avoidingFiles);
                        continue;
                    }
                    // if we reached here, the File object f was not a directory
                    // create a FileInputStream on top of f
                    FileInputStream fis = new FileInputStream(f);
                    // create a new zip entry
                    // ZipEntry anEntry = new ZipEntry(f.getPath());
                    ZipEntry anEntry = new ZipEntry(zipPath + "/" + f.getName());
                    // place the zip entry in the ZipOutputStream object
                    zos.putNextEntry(anEntry);
                    // now write the content of the file to the ZipOutputStream
                    while ((bytesIn = fis.read(readBuffer)) != -1) {
                        zos.write(readBuffer, 0, bytesIn);
                    }
                    fis.close();

                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    // zip all files/directories
    public static void zipDirectory(File directory, File dest) throws IOException {
        ZipOutputStream zos = new ZipOutputStream(new FileOutputStream(dest));
        zipDirectoryRecursive(directory, directory.getName(), zos);
        zos.close();
    }

    private static void zipDirectoryRecursive(File directory, String zipPath, ZipOutputStream zos) throws IOException {
        try {
            System.out.println(directory.getAbsolutePath() + "::" + zipPath);
            // get a listing of the directory content
            String[] dirList = directory.list();
            byte[] readBuffer = new byte[2156];
            int bytesIn = 0;
            // loop through dirList, and zip the files
            for (int i = 0; i < dirList.length; i++) {
                File f = new File(directory, dirList[i]);
                if (f.isDirectory()) {
                    // if the File object is a directory, call this
                    // function again to add its content recursively
                    // String filePath = f.getPath();
                    // File file = new File(zipPath+"/"+f.getName());
                    zipDirectoryRecursive(new File(f.getPath()), zipPath + "/" + f.getName(), zos);
                    continue;
                }
                // if we reached here, the File object f was not a directory
                // create a FileInputStream on top of f
                FileInputStream fis = new FileInputStream(f);
                // create a new zip entry
                // ZipEntry anEntry = new ZipEntry(f.getPath());
                ZipEntry anEntry = new ZipEntry(zipPath + "/" + f.getName());
                // place the zip entry in the ZipOutputStream object
                zos.putNextEntry(anEntry);
                // now write the content of the file to the ZipOutputStream
                while ((bytesIn = fis.read(readBuffer)) != -1) {
                    zos.write(readBuffer, 0, bytesIn);
                }
                fis.close();

            }

        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public static String getExtension(String fileName) {
        String extension = null;
        int dotPos = fileName.lastIndexOf(".");
        if (dotPos != -1) {
            extension = fileName.substring(dotPos);
        }
        return extension;
    }

    public static String removeExtension(String fileName) {
        String filePathName = null;
        int dotPos = fileName.lastIndexOf(".");
        if (dotPos != -1) {
            filePathName = fileName.substring(0, dotPos);
        }
        return filePathName;
    }

    public static String toString(File file) throws IOException {
        BufferedReader bufferedReader = new BufferedReader(new FileReader(file));
        StringBuilder result = new StringBuilder();
        String line = "";
        while ((line = bufferedReader.readLine()) != null) {
            result.append(line).append(System.getProperty("line.separator"));
        }
        bufferedReader.close();
        return result.toString().trim();
    }
}
