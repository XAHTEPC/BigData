# n - процент сообщений
n=0.7
# m - минимум ответов
m = 10.0
m = round(m);

df = spark.read.option("header", False).option("delimeter", '","').csv("/root/ira_tweets_csv_hashed.csv")
df.createOrReplaceTempView("tweets")
result_df = spark.sql("""
    SELECT _c1 AS userid,
           COUNT(_c1) AS total_messages,
           SUM(CASE WHEN _c25 >= """+m+""" THEN 1 ELSE 0 END) AS filtered_messages,
           SUM(CASE WHEN _c25 >= """+m+""" THEN 1 ELSE 0 END) / COUNT(_c1) AS percentage_filtered_messages
    FROM tweets
    GROUP BY _c1
    HAVING percentage_filtered_messages >=
    """ + n )
result_df.show()