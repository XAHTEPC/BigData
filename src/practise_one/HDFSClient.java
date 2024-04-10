package practise_one;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.FileUtil;

import java.io.IOException;

public class HDFSClient {

    public void createDirectory(String directoryPath) throws IOException {
        Path dirPath = new Path(Main.HDFS_URI + directoryPath);
        Main.fs.mkdirs(dirPath);
        System.out.println("Directory created: " + directoryPath);
    }

    public void uploadFile(String localFilePath, String hdfsFilePath) throws IOException {
        Path srcPath = new Path(Main.LOCAL_FS_URI + localFilePath);
        Path destPath = new Path(Main.HDFS_URI + hdfsFilePath);
        Main.fs.copyFromLocalFile(srcPath, destPath);
        System.out.println("File uploaded: " + localFilePath + " -> " + hdfsFilePath);
    }

    public void downloadFile(String hdfsFilePath, String localFilePath) throws IOException {
        Path srcPath = new Path(Main.HDFS_URI + hdfsFilePath);
        Path destPath = new Path(Main.LOCAL_FS_URI + localFilePath);
        Main.fs.copyToLocalFile(srcPath, destPath);
        System.out.println("File downloaded: " + hdfsFilePath + " -> " + localFilePath);
    }

    public void concatenateFile(String hdfsFilePath, String localFilePath) throws IOException {
        Path srcPath1 = new Path(Main.HDFS_URI + hdfsFilePath);
        Path srcPath2 = new Path(Main.LOCAL_FS_URI + localFilePath);
        Path destPath = new Path(Main.HDFS_URI + hdfsFilePath);
//        FileUtil.copyMerge(Main.fs, srcPath1, Main.fs, srcPath2, destPath, false, new Configuration(), null);
        System.out.println("Files concatenated: " + hdfsFilePath + " + " + localFilePath);
    }

    public void deleteFile(String hdfsFilePath) throws IOException {
        Path filePath = new Path(Main.HDFS_URI + hdfsFilePath);
        Main.fs.delete(filePath, false);
        System.out.println("File deleted: " + hdfsFilePath);
    }

    public void listFiles(String directoryPath) throws IOException {
        Path dirPath = new Path(Main.HDFS_URI + directoryPath);
//        FileUtil.copyMerge(Main.fs, dirPath, Main.fs, new Path(Main.LOCAL_FS_URI + "temp.txt"), false, new Configuration(), null);
        // Здесь можно реализовать вывод содержимого файла temp.txt
        System.out.println("Listing files in directory: " + directoryPath);
    }

    public void changeDirectory(String directoryPath) throws IOException {
        // Реализация перехода в каталог
        System.out.println("Changing directory to: " + directoryPath);
    }

    public void listLocalFiles(String localDirectoryPath) {
        // Реализация вывода содержимого текущего локального каталога
        System.out.println("Listing files in local directory: " + localDirectoryPath);
    }

    public void changeLocalDirectory(String localDirectoryPath) {
        // Реализация перехода в локальный каталог
        System.out.println("Changing local directory to: " + localDirectoryPath);
    }
}