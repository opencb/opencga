package org.opencb.opencga.core.models.wrapper.deseq2;

public class DESeq2Params {

    protected DESeq2Input input;
    protected DESeq2Analysis analysis;
    protected DESeq2Output output;

    public DESeq2Params() {
        this.input = new DESeq2Input();
        this.analysis = new DESeq2Analysis();
        this.output = new DESeq2Output();
    }

    public DESeq2Params(DESeq2Input input, DESeq2Analysis analysis, DESeq2Output output) {
        this.input = input;
        this.analysis = analysis;
        this.output = output;
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("DESeq2Params{");
        sb.append("input=").append(input);
        sb.append(", analysis=").append(analysis);
        sb.append(", output=").append(output);
        sb.append('}');
        return sb.toString();
    }

    public DESeq2Input getInput() {
        return input;
    }

    public DESeq2Params setInput(DESeq2Input input) {
        this.input = input;
        return this;
    }

    public DESeq2Analysis getAnalysis() {
        return analysis;
    }

    public DESeq2Params setAnalysis(DESeq2Analysis analysis) {
        this.analysis = analysis;
        return this;
    }

    public DESeq2Output getOutput() {
        return output;
    }

    public DESeq2Params setOutput(DESeq2Output output) {
        this.output = output;
        return this;
    }
}
