politicians = [
    'Байден',      # Joe Biden
    'Трамп',       # Donald Trump
    'Меркель',     # Angela Merkel
    'Макрон',      # Emmanuel Macron
    'Джонсон',     # Boris Johnson
    'Трюдо',       # Justin Trudeau
    'Моди',        # Narendra Modi
    'Джонс',       # Jacinda Ardern
    'Си',          # Xi Jinping (Си Цзиньпин)
    'Ким',         # Kim Jong-un
    'Зеленский',   # Volodymyr Zelensky
    'Эрдоган',     # Recep Tayyip Erdogan
    'Нетаньяху',   # Benjamin Netanyahu
    'Папа',        # Pope Francis (Папа Франциск)
    'Сальвини',    # Matteo Salvini
    'Санчес',      # Pedro Sánchez
    'Мадуро',      # Nicolás Maduro
    'Рютте',       # Mark Rutte
    'Орбан',       # Viktor Orbán
    'Моравецкий'   # Mateusz Morawiecki
]

politicians_str = '|'.join(politicians)
df = spark.read.option("header", True).option("delimeter", '","').csv("/root/ira_tweets_csv_hashed.csv")
df.createOrReplaceTempView("tw")
result_df = spark.sql(f"""
    SELECT userid, COUNT(*) as count_tweet 
    FROM tw 
    WHERE account_language = 'ru' 
        AND tweet_language = 'ru' 
        AND tweet_text RLIKE '({politicians_str})' 
    GROUP BY userid 
    ORDER BY count_tweet DESC
    """)
result_df.show()