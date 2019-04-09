package org.poem;

import org.apache.commons.io.IOUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
public class HdfsUtils {

    private static final String UTF_8 = "UTF-8";

    /**
     * 上传本地文件到 HDFS
     * @param source
     * @param dest
     */
    public static void uploadFile(String source, String dest) throws IOException {
        Configuration configuration = new Configuration();
        configuration.set("fs.defaultFS", "hdfs://192.168.24.227:9000");
        configuration.set("fs.hdfs.impl","org.apache.hadoop.hdfs.DistributedFileSystem");
        Path sourcePath = new Path(source);
        Path destPath = new Path(dest);
        FileSystem fileSystem = null;
        FSDataOutputStream outputStream = null;
        try {
            fileSystem = FileSystem.get(configuration);
            if (!fileSystem.exists(destPath)){
                fileSystem.mkdirs(destPath);
            }else{
                fileSystem.deleteOnExit(destPath);
                fileSystem.mkdirs(destPath);
            }
            String filename = source.substring(source.lastIndexOf('/') + 1);
            outputStream = fileSystem.append(destPath);
            outputStream.getWrappedStream().write(IOUtils.toByteArray(new FileInputStream(new File(source))));
            System.out.println("File " + filename + " copied to " + dest);

        } catch (IOException e) {
            e.printStackTrace();
        }finally {
            if (fileSystem != null){
                fileSystem.close();
            }
            if (outputStream != null) {
                outputStream.close();
            }
        }
    }

    public static void main(String[] args) throws IOException {
        uploadFile("C:\\Users\\Administrator\\Desktop\\test.txt","/test");
    }
}
