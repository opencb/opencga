package org.opencb.opencga.storage.hadoop.app;

import org.apache.commons.lang3.RandomStringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.util.ReflectionUtils;
import org.opencb.commons.datastore.core.ObjectMap;
import org.opencb.opencga.core.common.IOUtils;
import org.opencb.opencga.storage.hadoop.utils.MapReduceOutputFile;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Collections;
import java.util.Map;

public class HadoopMain extends AbstractMain {


    @Override
    protected void run(String[] args) throws Exception {
        new HadoopCommandExecutor().exec(args);
    }


    public static class HadoopCommandExecutor extends NestedCommandExecutor {
//        private HBaseManager hBaseManager;
        private Configuration conf;

        public HadoopCommandExecutor() {
            this("");
        }

        public HadoopCommandExecutor(String context) {
            super(context);
            addSubCommand(Arrays.asList("hdfs-ls", "ls"),
                    " [-f <path>] [-D key=value] : List the content of an hdfs path", this::hdfsLs);
            addSubCommand(Arrays.asList("hdfs-info", "info", "st"),
                    " [-f <path>] [-D key=value] : FS information", this::info);
            addSubCommand(Collections.singletonList("codec-info"),
                    " [-c <codec-name>] [-D key=value] : Codec information", this::codecInfo);
        }

        @Override
        protected void setup(String command, String[] args) throws Exception {
            conf = new Configuration();
        }

        @Override
        protected void cleanup(String command, String[] args) throws Exception {
        }

        private void hdfsLs(String[] args) throws Exception {
            ObjectMap map = getArgsMap(args, "f", "D");
            String path = map.getString("f", FileSystem.getDefaultUri(conf).toString());
            addDynamic(map);

            try (FileSystem fs = FileSystem.get(new Path(path).toUri(), conf)) {
                RemoteIterator<LocatedFileStatus> iterator = fs.listFiles(new Path(path), true);
                while (iterator.hasNext()) {
                    LocatedFileStatus file = iterator.next();
                    println("- " + file.getPath().toUri() + " : " + IOUtils.humanReadableByteCount(file.getLen(), false));
                }
            }
        }

        private void info(String[] args) throws Exception {
            ObjectMap map = getArgsMap(args, "f", "D");
            String path = map.getString("f", FileSystem.getDefaultUri(conf).toString());
            addDynamic(map);

            try (FileSystem fs = FileSystem.get(new Path(path).toUri(), conf)) {
                info(fs);
            }
        }

        private void addDynamic(ObjectMap map) {
            Map<String, Object> dynamic = map.getMap("D", Collections.emptyMap());
            if (dynamic != null) {
                for (Map.Entry<String, Object> entry : dynamic.entrySet()) {
                    conf.set(entry.getKey(), entry.getValue().toString());
                }
            }
        }

        private void info(FileSystem fs) throws Exception {
            println("fs.getScheme() = " + fs.getScheme());
            println("fs.getUri() = " + fs.getUri());
            println("fs.getHomeDirectory() = " + fs.getHomeDirectory());
            println("fs.getWorkingDirectory() = " + fs.getWorkingDirectory());
            println("fs.getConf() = " + fs.getConf());
            println("fs.getCanonicalServiceName() = " + fs.getCanonicalServiceName());
            FsStatus status = fs.getStatus();
            println("status.getCapacity() = " + IOUtils.humanReadableByteCount(status.getCapacity(), false));
            println("status.getRemaining() = " + IOUtils.humanReadableByteCount(status.getRemaining(), false));
            println("status.getUsed() = " + IOUtils.humanReadableByteCount(status.getUsed(), false));
        }

        private void codecInfo(String[] args) throws Exception {
            ObjectMap map = getArgsMap(args, "c", "D");
            String codecName = map.getString("c", "deflate");
            addDynamic(map);

            CompressionCodec codec;
            try {
                Class<?> aClass = Class.forName(codecName);
                codec = (CompressionCodec) ReflectionUtils.newInstance(aClass, conf);
            } catch (ClassNotFoundException | ClassCastException e) {
                codec = MapReduceOutputFile.getCompressionCodec(codecName, conf);
            }
            println("Codec name : " + codecName);
            if (codec == null) {
                println("Codec not found!");
            } else {
                println("Codec class : " + codec.getClass());
                println("Default extension : " + codec.getDefaultExtension());
                println("Compressor type : " + codec.getCompressorType());
                println("Decompressor type : " + codec.getDecompressorType());
                int rawSize = 1024 * 1024 * 10;
                InputStream is = new ByteArrayInputStream(RandomStringUtils.randomAlphanumeric(rawSize).getBytes(StandardCharsets.UTF_8));
                ByteArrayOutputStream byteOs = new ByteArrayOutputStream(rawSize);
                OutputStream os = codec.createOutputStream(byteOs);
                org.apache.commons.io.IOUtils.copy(is, os);
                int compressedSize = byteOs.size();

                println("Compression rate : "
                        + IOUtils.humanReadableByteCount(rawSize, false) + "(" + rawSize + "B) "
                        + "-> "
                        + IOUtils.humanReadableByteCount(compressedSize, false) + "(" + compressedSize + "B) "
                        + String.format("%.3f", ((double) compressedSize) / ((double) rawSize)));
                os.close();
            }
        }
    }
}
