package practise_four_var_2.hub;


import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
public class HITSMapper_hub extends Mapper<Object, Text, Text, Text> {
    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        /* парсим входящую строку на ключ (текущий узел), значения (список узлов, на которые ссылается текущий узел)
           и величины (оценка авторитетности и оценка посредничества)
           входящие строки смотреть в файле input */
        String[] kv = String.valueOf(value).split("\t");
        String key_node = kv[0];
        String[] values = kv[1].split("\\|");
        String nodes = values[0];
        String auth = values[1];
        String hub = values[2];
        String[] arr_nodes = nodes.split(":");
        if(arr_nodes.length>1) {
            String[] arr_node = arr_nodes[1].split(",");
            for (String node : arr_node) {
                    /*
                     так как посредничество расчитывается по значениям авторитетности узлов, на которые ссылается текущий узел
                     но так как графы у нас сейчас инвертированы, мы проинвертируем их ещё раз и получим нужный результат
                     отправляем на reducer инвертированные значения графов
                     то есть, было b:a,c:a
                     стало a:b,c
                     также, отправляем оценку авторитетности текущего узла
                     */
                context.write(new Text(node), new Text(key_node+"|"+auth));
            }
        }
        // отправляем ключом текущий узел, а значением - величины этого узла, для составления выходного файла
        context.write(new Text(key_node), new Text(auth+"|"+hub));
    }
}

