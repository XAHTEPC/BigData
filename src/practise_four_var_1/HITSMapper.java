package practise_four_var_1;

import java.io.IOException;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class HITSMapper extends Mapper<Object, Text, Text, Text> {

    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        String[] parts = value.toString().split(":");
        String page = parts[0].trim();
        String[] links = parts[1].split(",");
        double hubScore = Double.parseDouble(parts[2].trim());
        double authScore = Double.parseDouble(parts[3].trim());
        for (String link : links) {
            HITSReducer.add(page,link);
            context.write(new Text(link), new Text("AUTH:" + hubScore));
//            context.write(new Text(link), new Text("HUB:" + authScore));
            context.write(new Text(link), new Text("HUB:" + authScore+":"+page));
        }
        // Отправляем список связанных страниц и их оценки
        context.write(new Text(page), new Text("LINKS:" + parts[1]));
//        System.out.println("MAP: ok");
    }
}