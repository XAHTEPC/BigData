package practise_four_var_1;

import java.io.IOException;
import java.util.HashMap;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class HITSReducer extends Reducer<Text, Text, Text, Text> {
    public static HashMap<String,String> ourPage;
    public static void start(){
        ourPage = new HashMap<>();
    }
    public static void add(String id,String id2){
        ourPage.put(id,id2);
    }

    public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        double hubSum = 0;
        double authSum = 0;
        String links = "";

        // Суммируем оценки хабов и авторитетов
        for (Text val : values) {
            System.out.println(key+" val:"+val);
            String[] parts = val.toString().split(":");
            if (parts[0].equals("HUB")) {
                String mainPage = parts[2];
                String currentPage = key.toString();
                if(ourPage.containsKey(currentPage)&&ourPage.containsValue(mainPage)){
                    hubSum += Double.parseDouble(parts[1]);
                    System.out.println(key+" hubEl:"+parts[1]);
                }
            } else if (parts[0].equals("AUTH")) {
                authSum += Double.parseDouble(parts[1]);
                System.out.println(key+" authEl:"+parts[1]);
            } else if (parts[0].equals("LINKS")) {
                links = parts[1];
                System.out.println(key+" LINKS:"+parts[1]);
            }
        }

        // Нормализуем оценки
        Normalization.addElements(key.toString(),hubSum,authSum,links);
//        System.out.println(key+" : "+hubSum+" : "+ authSum);
        // Выводим результаты
        context.write(key, new Text("HUB:" + hubSum + ", AUTH:" + authSum + ", LINKS:" + links));

    }
}