package practise_four;

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
        for (int i = 0; i < links.length; i++) {
            context.write(new Text(links[i].trim()), new Text("HUB:" + authScore));
            context.write(new Text(page), new Text("AUTH:" + hubScore));
        }
        context.write(new Text(page), new Text("LINKS:" + String.join(",", links)));
//        System.out.println("MAP: ok");
    }
}