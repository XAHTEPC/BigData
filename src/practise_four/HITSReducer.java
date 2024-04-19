package practise_four;

import java.io.IOException;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class HITSReducer extends Reducer<Text, Text, Text, Text> {

    public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        double hubSum = 0;
        double authSum = 0;
        String links = "";

        // Суммируем оценки хабов и авторитетов
        for (Text val : values) {
            String[] parts = val.toString().split(":");
            if (parts[0].equals("HUB")) {
                hubSum += Double.parseDouble(parts[1]);
            } else if (parts[0].equals("AUTH")) {
                authSum += Double.parseDouble(parts[1]);
            } else if (parts[0].equals("LINKS")) {
                links = parts[1];
            }
        }

        // Нормализуем оценки
        Normalization.addElements(key,hubSum,authSum,links);
//        System.out.println(key+" : "+hubSum+" : "+ authSum);
        // Выводим результаты
        context.write(key, new Text("HUB:" + hubSum + ", AUTH:" + authSum + ", LINKS:" + links));

    }
}