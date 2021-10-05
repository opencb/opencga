package com.zetta.opencga.plugins.sample;

import com.zetta.opencga.plugins.OpenCgaPlugin;

public class SamplePlugin extends OpenCgaPlugin {

    @Override
    protected void run() throws Exception {

        System.out.println("Ejecutando el metodo run de SamplePlugin");
    }

    public void execute() {
        try {
            run();
        } catch (Exception e) {
            System.out.println("");
        }
    }
}
