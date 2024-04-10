package practise_three.methods;

import java.io.*;

public class ManageFile {
    public static void transform3(String inputFilePath1,String inputFilePath2, String outputFilePath) throws IOException {
        try (BufferedReader reader = new BufferedReader(new FileReader(inputFilePath1));
             BufferedReader reader2 = new BufferedReader(new FileReader(inputFilePath2));
             BufferedWriter writer = new BufferedWriter(new FileWriter(outputFilePath))) {

            String line;
            while ((line = reader.readLine()) != null) {
                String[] parts = line.split(",");
                if (parts.length == 2) {
                    String id = parts[0];
                    String name = parts[1];
                    writer.write("Transport," + id + "," + name + "\n");
                }
            }
            while ((line = reader2.readLine()) != null) {
                String[] parts = line.split(",");
                if (parts.length == 2) {
                    String id = parts[0];
                    String name = parts[1];
                    writer.write("City," + id + "," + name + "\n");
                }
            }
        }
    }

    public static void transform2(String inputFilePath1,String inputFilePath2, String outputFilePath) throws IOException {
        try (BufferedReader reader = new BufferedReader(new FileReader(inputFilePath1));
             BufferedReader reader2 = new BufferedReader(new FileReader(inputFilePath2));
             BufferedWriter writer = new BufferedWriter(new FileWriter(outputFilePath))) {

            String line;
            while ((line = reader.readLine()) != null) {
                String[] parts = line.split(",");
                if (parts.length == 2) {
                    String id = parts[0];
                    String name = parts[1];
                    writer.write("Transport," + id + "," + name + "\n");
                }
            }
            while ((line = reader2.readLine()) != null) {
                String[] parts = line.split(",");
                if (parts.length == 2) {
                    String id = parts[0];
                    String name = parts[1];
                    writer.write("Color," + id + "," + name + "\n");
                }
            }
        }
    }
    public static void transform1(String inputFilePath1,String inputFilePath2, String outputFilePath) throws IOException {
        try (BufferedReader reader = new BufferedReader(new FileReader(inputFilePath1));
             BufferedReader reader2 = new BufferedReader(new FileReader(inputFilePath2));
             BufferedWriter writer = new BufferedWriter(new FileWriter(outputFilePath))) {

            String line;
            while ((line = reader.readLine()) != null) {
                String[] parts = line.split(",");
                if (parts.length == 2) {
                    String id = parts[0];
                    String name = parts[1];
                    writer.write("Transport," + id + "," + name + "\n");
                }
            }
            while ((line = reader2.readLine()) != null) {
                String[] parts = line.split(",");
                if (parts.length == 2) {
                    String id = parts[0];
                    String name = parts[1];
                    writer.write("Owner," + id + "," + name + "\n");
                }
            }
        }
    }

    public static void task3(){
        String inputFilePath1 = "/home/caxapa/inputData/transport.txt";
        String inputFilePath2 = "/home/caxapa/inputData/city.txt";
        String outputFilePath = "/home/caxapa/inputData/transport_city.txt";
        try {
            transform3(inputFilePath1,inputFilePath2, outputFilePath);
            System.out.println("Transformation completed successfully.");
        } catch (IOException e) {
            System.err.println("Error occurred during transformation: " + e.getMessage());
            e.printStackTrace();
        }
    }

    public static void task2(){
        String inputFilePath1 = "/home/caxapa/inputData/transport.txt";
        String inputFilePath2 = "/home/caxapa/inputData/color.txt";
        String outputFilePath = "/home/caxapa/inputData/transport_color.txt";
        try {
            transform2(inputFilePath1,inputFilePath2, outputFilePath);
            System.out.println("Transformation completed successfully.");
        } catch (IOException e) {
            System.err.println("Error occurred during transformation: " + e.getMessage());
            e.printStackTrace();
        }
    }

    public static void task1(){
        String inputFilePath1 = "/home/caxapa/inputData/transport.txt";
        String inputFilePath2 = "/home/caxapa/inputData/owner.txt";
        String outputFilePath = "/home/caxapa/inputData/transport_owner.txt";
        try {
            transform1(inputFilePath1,inputFilePath2, outputFilePath);
            System.out.println("Transformation completed successfully.");
        } catch (IOException e) {
            System.err.println("Error occurred during transformation: " + e.getMessage());
            e.printStackTrace();
        }
    }
}