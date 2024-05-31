№ т.к. нужно выбрать ток англичан, то поиграемся и посмотрим как сделать запрос нужный
>>> result = spark.sql("SELECT DISTINCT user_reported_location FROM tw")
>>> result.show()
+----------------------+
|user_reported_location|
+----------------------+
|             Волгоград|
|                  Utah|
|             Volgograd|
|   Santa Barbara, C...|
|          Richmond, VA|
|          Florida, USA|
|               Amerika|
|  San Diego, United...|
|                Russia|
|               Иркутск|
|               Phoenix|
|         Delaware, USA|
|              Тольятти|
|  Набережные Челны,...|
|         Missouri, USA|
|             Hollywood|
|       Druid Hills, GA|
|        North Port, FL|
|                    AZ|
|                 Псков|
+----------------------+
only showing top 20 rows

>>> result = spark.sql("""
...     SELECT DISTINCT user_reported_location 
...     FROM tw 
...     WHERE user_reported_location RLIKE '^[a-zA-Z0-9 ]*$'
... """)
>>> result.show()
+----------------------+
|user_reported_location|
+----------------------+
|                  Utah|
|             Volgograd|
|               Amerika|
|                Russia|
|               Phoenix|
|             Hollywood|
|                    AZ|
|                    ZM|
|             Minnesota|
|               alabama|
|                    us|
|                    LA|
|                  Ohio|
|                    MN|
|               MSK SAO|
|        New York City |
|            Greensboro|
|              Cromwell|
|                Jersey|
|                Italia|
+----------------------+
>>> spark.sql("""
...     SELECT DISTINCT user_reported_location 
...     FROM tw 
...     WHERE user_reported_location RLIKE '^[a-zA-Z0-9 ]*$'
...     AND user_reported_location != 'Russia'
... """)

>>> result.show()

#сам код

df = spark.read.format("csv").options(header="true",inferSchema="true").load("file:///root/ira_tweets_csv_hashed.csv")
df = spark.read.options(header="true",inferSchema="true").csv("/root/ira_tweets_csv_hashed.csv")
df.createOrReplaceTempView("tw")
cleaned_tweets = spark.sql("SELECT * FROM tw "
                           "WHERE in_reply_to_userid RLIKE '^.{64}$'"
                           "AND user_reported_location RLIKE '^[a-zA-Z0-9 ]*$'"
                           "AND user_reported_location != 'Russia' "
                           "AND userid RLIKE '^.{64}$'")
cleaned_tweets.createOrReplaceTempView("ctw")
people = spark.sql("SELECT DISTINCT userid AS id FROM ctw")
edge = spark.sql("SELECT DISTINCT userid AS src, in_reply_to_userid AS dst FROM ctw")
from graphframes import *
g = GraphFrame(people, edge)
sc.setCheckpointDir("/tmp")
components = g.connectedComponents()
components.show()
components.createOrReplaceTempView("comp")
spark.sql("SELECT component, COUNT(*) AS count FROM comp GROUP BY component ORDER BY count DESC").show()