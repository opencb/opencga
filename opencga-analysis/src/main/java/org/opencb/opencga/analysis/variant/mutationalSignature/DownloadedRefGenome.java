package org.opencb.opencga.analysis.variant.mutationalSignature;

import java.io.File;

public class DownloadedRefGenome {
    private String assembly;
    private File gzFile;
    private File faiFile;
    private File gziFile;

    public DownloadedRefGenome(String assembly, File gzFile, File faiFile, File gziFile) {
        this.assembly = assembly;
        this.gzFile = gzFile;
        this.faiFile = faiFile;
        this.gziFile = gziFile;
    }

    public String getAssembly() {
        return assembly;
    }

    public DownloadedRefGenome setAssembly(String assembly) {
        this.assembly = assembly;
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
