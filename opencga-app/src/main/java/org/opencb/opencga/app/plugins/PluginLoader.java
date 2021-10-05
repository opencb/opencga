package org.opencb.opencga.app.plugins;

import java.io.File;
import java.lang.reflect.Method;
import java.net.URL;
import java.net.URLClassLoader;
import java.util.*;
import java.util.jar.JarEntry;
import java.util.jar.JarFile;

public class PluginLoader {

    public void load(String path) {

        String[] pathnames;

        // Creates a new File instance by converting the given pathname string
        // into an abstract pathname
        File f = new File(path);

        // Populates the array with names of files and directories
        pathnames = f.list();
        Map<String, Integer> classes = new HashMap<>();
        List<String> classLoaded = new ArrayList<>();

        for (String pathname : pathnames) {
            // Print the names of jar files
            System.out.println(pathname);
            loadJar(path + pathname, classLoaded);
            //  break;
        }
    }

    private int loadJar(String pathToJar, List<String> classLoaded) {

        try {
            JarFile jarFile = new JarFile(pathToJar);
            Enumeration<JarEntry> e = jarFile.entries();
            URL[] urls = {new URL("jar:file:" + pathToJar + "!/")};
            // URLClassLoader cl = URLClassLoader.newInstance(urls);
            // ClassLoader cls = cl.getClass().getClassLoader();
            URLClassLoader cls = new URLClassLoader(urls, this.getClass().getClassLoader());
            while (e.hasMoreElements()) {
                JarEntry je = e.nextElement();
                if (je.isDirectory() || !je.getName().endsWith(".class")) {
                    continue;
                }
                // -6 because of .class
                String className = je.getName().substring(0, je.getName().length() - 6);
                className = className.replace('/', '.');
                className = className.replace("\'", "");

                try {
                    if (!classLoaded.contains(className)) {
                        System.out.println("className " + className);
                        Class c = Class.forName(className, true, cls);
                        if (c != null) {
                            classLoaded.add(className);
                            Method method = c.getDeclaredMethod("execute");
                            Object instance = c.newInstance();
                            if (method != null && instance != null) {
                                method.invoke(instance);
                            } else {
                                System.out.println("method:::: " + method + " \n instance:::: " + instance);
                            }

                            System.out.println("LOADED:::: " + className);
                        } else {
                            System.out.println("NOT LOADED NULL :::: " + className);
                        }
                    }
                } catch (Throwable cle) {
                    cle.printStackTrace();
                    System.err.println("NOT LOADED:::: " + className);
                }
            }
        } catch (Exception ex) {
            ex.printStackTrace();
        }
        return classLoaded.size();
    }
}
