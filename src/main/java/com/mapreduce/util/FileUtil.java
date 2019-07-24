package com.mapreduce.util;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.IOException;

/**
 * @program: MY_MapReduce
 * @description: 工具类,操作文件夹
 * @author: xuyao
 * @create: 2019/07/24 15:21
 */
public class FileUtil {

    public static void deleteDir(String pathStr, Configuration configuration) throws IOException {
        Path path = new Path(pathStr);
        FileSystem fileSystem = path.getFileSystem(configuration);
        if (fileSystem.exists(path)) {
            fileSystem.delete(path, true);
        }
    }

}