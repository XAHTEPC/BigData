package practise_one;

import java.io.IOException;
import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;
import java.util.Scanner;

public class Main {

    public static final String HDFS_URI = "hdfs://localhost:9000";
    public static final String LOCAL_FS_URI = "file:///";

    public static FileSystem fs;

    public static void main(String[] args) {
        try {
            Configuration conf = new Configuration();
            fs = FileSystem.get(URI.create(HDFS_URI), conf);
            Scanner scanner = new Scanner(System.in);
            HDFSClient client = new HDFSClient();
            String command;
            do {
                System.out.println("Enter command (mkdir/upload/download/concat/delete/ls/cd/lls/lcd/exit): ");
                command = scanner.next();
                switch (command) {
                    case "mkdir":
                        String directoryPath = scanner.next();
                        client.createDirectory(directoryPath);
                        break;
                    case "upload":
                        String localFilePath = scanner.next();
                        String hdfsFilePath = scanner.next();
                        client.uploadFile(localFilePath, hdfsFilePath);
                        break;
                    case "download":
                        String hdfsFileDownloadPath = scanner.next();
                        String localFileDownloadPath = scanner.next();
                        client.downloadFile(hdfsFileDownloadPath, localFileDownloadPath);
                        break;
                    case "concat":
                        String hdfsConcatFilePath = scanner.next();
                        String localConcatFilePath = scanner.next();
                        client.concatenateFile(hdfsConcatFilePath, localConcatFilePath);
                        break;
                    case "delete":
                        String hdfsFilePathToDelete = scanner.next();
                        client.deleteFile(hdfsFilePathToDelete);
                        break;
                    case "ls":
                        String directoryPathToLs = scanner.next();
                        client.listFiles(directoryPathToLs);
                        break;
                    case "cd":
                        String directoryPathToCd = scanner.next();
                        client.changeDirectory(directoryPathToCd);
                        break;
                    case "lls":
                        String localDirectoryPathToLs = scanner.next();
                        client.listLocalFiles(localDirectoryPathToLs);
                        break;
                    case "lcd":
                        String localDirectoryPathToCd = scanner.next();
                        client.changeLocalDirectory(localDirectoryPathToCd);
                        break;
                    case "exit":
                        break;
                    default:
                        System.out.println("Invalid command");
                }
            } while (!command.equals("exit"));
            scanner.close();
            fs.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
