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

package org.opencb.opencga.analysis;

import org.opencb.commons.utils.URLUtils;

import java.io.File;
import java.io.IOException;
import java.net.URL;
import java.nio.file.Path;
import java.util.LinkedList;
import java.util.List;

public class ResourceUtils {

    public static final String URL = "http://resources.opencb.org/opencb/opencga/";

    public static File downloadThirdParty(URL url, Path outDir) throws IOException {
        return URLUtils.download(url, outDir);
    }

    public static File downloadAnalysis(String analysisId, String resouceName, Path outDir, Path openCgaHome) throws IOException {
        Path path = null;
        String filename = "analysis/" + analysisId + "/" + resouceName;
        if (openCgaHome != null) {
            path = openCgaHome.resolve(filename);
        }
        if (path != null && path.toFile().exists()) {
            return path.toFile();
        } else {
            return URLUtils.download(new URL(URL + filename), outDir);
        }
    }

    public static DownloadedRefGenome downloadRefGenome(Species species, Assembly assembly, Authority authority, Path outDir,
                                                        Path openCgaHome) throws IOException {

        // Sanity check
        if (species != Species.hsapiens) {
            throw new IOException("Species '" + species + "' not supported yet");
        }
        if (assembly != Assembly.GRCh37 && assembly != Assembly.GRCh38) {
            throw new IOException("Assembly '" + assembly + "' not supported");
        }
        if (authority != Authority.Ensembl) {
            throw new IOException("Authority '" + authority + "' not supported");
        }

        // Download files
        File gzFile = null;
        File faiFile = null;
        File gziFile = null;

        // Get files to downloadAnalysis
        List<String> filenames = new LinkedList<>();
        filenames.add("Homo_sapiens." + assembly + ".dna.primary_assembly.fa.gz");
        filenames.add("Homo_sapiens." + assembly + ".dna.primary_assembly.fa.gz.fai");
        filenames.add("Homo_sapiens." + assembly + ".dna.primary_assembly.fa.gz.gzi");

        Path path = null;
        for (String filename : filenames) {
            File file;

            if (openCgaHome != null) {
                path = openCgaHome.resolve("analysis/commons/reference-genomes/" + filename);
            }
            if (path != null && path.toFile().exists()) {
                file = path.toFile();
            } else {
                URL url = new URL(URL + "analysis/commons/reference-genomes/" + filename);
                file = URLUtils.download(url, outDir);
                if (file == null) {
                    // Something wrong happened, remove downloaded files
                    cleanRefGenome(filenames, outDir);
                    return null;
                }
            }
            if (filename.endsWith("gz")) {
                gzFile = file;
            } else if (filename.endsWith("fai")) {
                faiFile = file;
            } else if (filename.endsWith("gzi")) {
                gziFile = file;
            }

            path = null;
        }
        return new DownloadedRefGenome(species, assembly, authority, gzFile, faiFile, gziFile);
    }

    //-------------------------------------------------------------------------
    // Support for downloading reference genomes
    //-------------------------------------------------------------------------

    public enum Authority {
        Ensembl, NCBI
    }

    public enum Species {
        hsapiens
    }

    public enum Assembly {
        GRCh37, GRCh38
    }

    public static class DownloadedRefGenome {
        private Species species;
        private Assembly assembly;
        private Authority authority;
        private File gzFile;
        private File faiFile;
        private File gziFile;

        public DownloadedRefGenome(Species species, Assembly assembly, Authority authority, File gzFile, File faiFile, File gziFile) {
            this.species = species;
            this.assembly = assembly;
            this.authority = authority;
            this.gzFile = gzFile;
            this.faiFile = faiFile;
            this.gziFile = gziFile;
        }

        public Species getSpecies() {
            return species;
        }

        public DownloadedRefGenome setSpecies(Species species) {
            this.species = species;
            return this;
        }

        public Assembly getAssembly() {
            return assembly;
        }

        public DownloadedRefGenome setAssembly(Assembly assembly) {
            this.assembly = assembly;
            return this;
        }

        public Authority getAuthority() {
            return authority;
        }

        public DownloadedRefGenome setAuthority(Authority authority) {
            this.authority = authority;
            return this;
        }

        public File getGzFile() {
            return gzFile;
        }

        public DownloadedRefGenome setGzFile(File gzFile) {
            this.gzFile = gzFile;
            return this;
        }

        public File getFaiFile() {
            return faiFile;
        }

        public DownloadedRefGenome setFaiFile(File faiFile) {
            this.faiFile = faiFile;
            return this;
        }

        public File getGziFile() {
            return gziFile;
        }

        public DownloadedRefGenome setGziFile(File gziFile) {
            this.gziFile = gziFile;
            return this;
        }
    }

    //-------------------------------------------------------------------------
    // P R I V A T E     M E T H O D S
    //-------------------------------------------------------------------------

    private static void cleanRefGenome(List<String> links, Path outDir) {
        for (String link : links) {
            String name = new File(link).getName();
            File file = outDir.resolve(name).toFile();
            if (file.exists()) {
                file.delete();
            }
        }
    }
}
