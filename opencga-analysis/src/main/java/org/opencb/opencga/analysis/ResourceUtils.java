package org.opencb.opencga.analysis;

import org.opencb.commons.utils.URLUtils;

import java.io.File;
import java.io.IOException;
import java.net.URL;
import java.nio.file.Path;
import java.util.LinkedList;
import java.util.List;

public class ResourceUtils {

    public static final String URL = "http://bioinfo.hpc.cam.ac.uk/opencb/opencga/analysis/";

    public enum Authority {
        Ensembl, NCBI
    }

    public enum Species {
        hsapiens
    }

    public enum Assembly {
        Grch37, Grch38
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


    public static File download(URL url, Path outDir) throws IOException {
        return URLUtils.download(url, outDir);
    }

    public static File download(String analysisId, String resouceName, Path outDir) throws IOException {
        return URLUtils.download(new URL(URL + analysisId + "/" + resouceName), outDir);
    }

    public static DownloadedRefGenome downloadRefGenome(Species species, Assembly assembly, Authority authority, Path outDir)
            throws IOException {

        // Sanity check
        if (species != Species.hsapiens) {
            throw new IOException("Species '" + species + "' not supported yet");
        }
        if (assembly != Assembly.Grch37 && assembly != Assembly.Grch38) {
            throw new IOException("Assembly '" + assembly + "' not supported");
        }
        if (authority != Authority.Ensembl) {
            throw new IOException("Authority '" + authority + "' not supported");
        }

        // Get files to download
        List<String> links = new LinkedList<>();
        if (authority == Authority.Ensembl) {
            switch (assembly) {
                case Grch37:
                    links.add("Homo_sapiens.GRCh37.dna.primary_assembly.fa.gz");
                    links.add("Homo_sapiens.GRCh37.dna.primary_assembly.fa.gz.fai");
                    links.add("Homo_sapiens.GRCh37.dna.primary_assembly.fa.gz.gzi");
                    break;
                default:
                    links.add("Homo_sapiens.GRCh38.dna.primary_assembly.fa.gz");
                    links.add("Homo_sapiens.GRCh38.dna.primary_assembly.fa.gz.fai");
                    links.add("Homo_sapiens.GRCh38.dna.primary_assembly.fa.gz.gzi");
                    break;
            }
        }

        // Download files
        File gzFile = null;
        File faiFile = null;
        File gziFile = null;

        for (String link : links) {
            URL url = new URL(URL + "resources/reference-genomes/" + link);
            File file = download(url, outDir);
            if (file == null) {
                // Something wrong happened, remove downloaded files
                cleanRefGenome(links, outDir);
                return null;
            }
            if (link.endsWith("gz")) {
                gzFile = file;
            } else if (link.endsWith("fai")) {
                faiFile = file;
            } else if (link.endsWith("gzi")) {
                gziFile = file;
            }
        }
        return new DownloadedRefGenome(species, assembly, authority, gzFile, faiFile, gziFile);
    }

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
