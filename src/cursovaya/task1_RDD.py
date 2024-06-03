def float_convert(s):
    try:
        return float(s)
    except:
        return 0.0

text = sc.textFile('/root/ira_tweets_csv_hashed.csv')
text1 = text.filter(lambda row: row.find('"tweetid"') != 0)
def cleaner(s):
    list = s.split('","')
    list[24] = float_convert(list[24])
    return list[0:30]
# n - процент сообщений
n=0.7
# m - минимум ответов
m = 10.0
text2 = text1.map(cleaner)
text3 = text2.filter(lambda row: row[24] >= m)
total_messages_per_user = text2.map(lambda x: (x[1], 1)).reduceByKey(lambda a, b: a + b)
filtered_messages_per_user = text3.map(lambda x: (x[1], 1)).reduceByKey(lambda a, b: a + b)
joined_data = total_messages_per_user.join(filtered_messages_per_user)
percentage_filtered_messages = joined_data.map(lambda x: (x[0], float_convert(x[1][1]) / float_convert(x[1][0])))
result = percentage_filtered_messages.filter(lambda x: x[1] >= n)
result.collect()