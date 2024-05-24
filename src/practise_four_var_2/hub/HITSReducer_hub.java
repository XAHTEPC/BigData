package practise_four_var_2.hub;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class HITSReducer_hub extends Reducer<Text, Text, Text, Text> {

    public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            /*
                Обновляем оценку посредничества и составляем выходной файл того же формата,
                что и входной, но с обратно инвертированными графами
                то есть уже с прямыми графами
             */

        double auth=0;
        double hub=0;
        String nodes = "nodes:";
        for (Text v : values){
            String[] one = v.toString().split("\\|");
            String[] two = one[0].split(":");
                /*
                 проверка на то, какое значение для узла пришло
                 "a|auth:1" или "auth:1|hub:1"
                 если первый вариант, то записываем в список узлов новый узел и обновляем оценку посредничества
                 иначе записываем в оценку авторитетности значение из входного файла
                 */

            if(two.length==1){
                nodes+=two[0]+",";
                two = one[1].split(":");
                hub+=Double.valueOf(two[1]);
            }else{
                two=one[0].split(":");
                auth=Double.valueOf(two[1]);
            }
        }
            /*
            формируем выходную строку и отправляем (записываем в файл)
            получим исходный файл, но обновлёнными величинами
             */
        context.write(new Text(key),new Text(nodes+"|auth:"+auth+"|hub:"+hub));
    }
}
