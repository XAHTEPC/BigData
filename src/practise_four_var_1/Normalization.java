package practise_four_var_1;
import org.apache.hadoop.io.Text;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;

public class Normalization {
    public static ArrayList<String> key;
    public static ArrayList<Double> auth;
    public static ArrayList<Double> score;
    public static ArrayList<String> links;

    public static void start(){
        key = new ArrayList<>();
        auth = new ArrayList<>();
        score = new ArrayList<>();
        links = new ArrayList<>();
    }
    public static void addElements(String key_, double auth_, double score_, String links_){
        key.add(key_);
        auth.add(auth_);
        score.add(score_);
        links.add(links_);
    }

    public static void makeNormalization(){
        // Создадим файл для нормализации
        String filePath = "/home/caxapa/outputDataHITS/normalization.txt";

        try {
            // Создаем новый объект File
            File file = new File(filePath);

            // Проверяем, существует ли файл уже
            if (file.createNewFile()) {
                System.out.println("Файл создан: " + file.getName());
            } else {
                System.out.println("Файл уже существует.");
            }
        } catch (IOException e) {
            System.out.println("Возникла ошибка при создании файла.");
            e.printStackTrace();
        }

        // Подсчет нормализации
        double quadratic_sum_auth = 0;
        double quadratic_sum_hub = 0;
        for(int i=0;i<auth.size();i++){
            quadratic_sum_auth += auth.get(i).doubleValue()*auth.get(i).doubleValue();
            quadratic_sum_hub += score.get(i).doubleValue()*score.get(i).doubleValue();
        }
        //  Запись в строку
        String content = "";

        //вывод в файл
        for(int i=0;i<key.size();i++) {
            double local_hub = score.get(i).doubleValue() / Math.sqrt(quadratic_sum_hub);
            double local_auth = auth.get(i).doubleValue() / Math.sqrt(quadratic_sum_auth);
            content =content + key.get(i).toString() + "\tHUB:" + local_hub +
                    "\tAUTH:" + local_auth + "\tLINKS:" + links.get(i).toString() + "\n";
        }
        try {
            // Создаем объект FileWriter с указанием пути к файлу
            FileWriter fileWriter = new FileWriter(filePath);

            // Создаем объект BufferedWriter для записи строк
            BufferedWriter bufferedWriter = new BufferedWriter(fileWriter);

            // Записываем строку в файл
            bufferedWriter.append(content);

            // Закрываем поток BufferedWriter
            bufferedWriter.close();

            System.out.println("Строка успешно записана в файл.");
        } catch (IOException e) {
            System.out.println("Возникла ошибка при записи в файл.");
            e.printStackTrace();
        }
    }
}

