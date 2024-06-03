df = spark.read.format("csv").options(header="true",inferSchema="true").load("file:///root/ira_tweets_csv_hashed.csv")
df = spark.read.options(header="true",inferSchema="true").csv("/root/ira_tweets_csv_hashed.csv")
df.createOrReplaceTempView("tw")
cleaned_tweets = spark.sql("SELECT * FROM tw WHERE in_reply_to_tweetid IS NULL OR in_reply_to_tweetid RLIKE '^\\\d{18}$'")
cleaned_tweets.createOrReplaceTempView("tweets")
#вершины - id твитов
v = spark.sql("SELECT tweetid AS id FROM tweets")
#ребра - ответы твиты с нужным id
e = spark.sql("SELECT tweetid AS src, in_reply_to_tweetid AS dst FROM tweets")
from graphframes import *
g = GraphFrame(v, e)
sc.setCheckpointDir("/tmp")
components = g.connectedComponents()
components.createOrReplaceTempView("comp")
#необходимая цепочка (т.е. n=2 - вторая по виличине цепочка)
n = 2
target_component = spark.sql(f"""SELECT component FROM 
                            (SELECT component, COUNT(*) AS count FROM comp GROUP BY component ORDER BY count DESC LIMIT {n})
                            ORDER BY count ASC LIMIT 1""")
id_component = target_component.collect()[0]["component"]
#находим набор твитов которые являются нашей частью
tweet_id_in_target_component = spark.sql(f"SELECT id FROM comp WHERE component = {id_component}")
tweet_id_in_target_component.createOrReplaceTempView("tweets_id")
e.createOrReplaceTempView("e")
#находим твит без начального твита (т.е. не имеет ответов)
target_tweetid = spark.sql("SELECT * FROM tweets_id LEFT JOIN e ON tweets_id.id = e.src WHERE e.dst IS NULL").collect()[0]["id"]
#Находим владельца этого твита
target_user = spark.sql(f"SELECT userid, user_display_name, user_screen_name FROM tweets WHERE tweetid = {target_tweetid}")