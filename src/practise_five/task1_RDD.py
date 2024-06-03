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


text = sc.textFile('/root/ira_tweets_csv_hashed.csv')
text1 = text.filter(lambda row: row.find('"tweetid"') != 0)
def cleaner(s):
    list = s.split('","')
    return list[0:30]


text2 = text1.map(cleaner)

def filter_func(row):
    if row[10] == 'ru' and row[11] == 'ru':
        for politician in politicians:
            if politician in row[12]:
                return True
    return False


text3 = text2.filter(filter_func)
text3.map(lambda row: (row[1], 1)).reduceByKey(lambda x, y: x + y).sortBy(lambda x: x[1], ascending=False).take(5)