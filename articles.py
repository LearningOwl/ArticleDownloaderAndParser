import dbconnect
import newspaper
import pandas as pd
import time
import datetime
from newspaper import Article as atcl, news_pool
from urllib.parse import urlparse
# from timedecorator import timeit
from SingleSource import SingleSource
# import timeit 
from multiprocessing.dummy import Pool as ThreadPool

connection = dbconnect.creteconnect()
cursor = connection.cursor()
basestmt = 'insert into news_articles( source_url , headline_text ) values '

# print(cursor.description)


# publish_timeapprox time of publish yyyyMMddHHmm IST timezone
# feed_codeunique source identifier
# source_urlurl of article
# headline_textheadline text in UTF8 (any language)

# artic = atcl('http://www.nydailynews.com/news/politics/mcconnell-ryan-launch-damage-control-trump-rally-article-1.3436581')
# artic.download()
# artic.parse()
# print(artic.text)

data = pd.read_csv('./datasets/news-week-aug24.csv')
print(data.columns)

print(datetime.datetime.now())
urls = data.iloc[:, 2]

# cursor.execute('select source_url from articles.news_articles')
# savedURls = cursor.fetchall()
# savedURLs = pd.DataFrame(data = savedURLs, columns=['URL'], index=['URL'])




# global domains
# domains = dict()

# def getdomain(url):
#     parsed = urlparse(url)
#     result = parsed.scheme+'://'+parsed.netloc
#     if result in domains:
#         domains[result].append(result+parsed.path)
#     else:
#         domains[result] = [result + parsed.path]

# # Creating key-value pairs where key = domain name and value = list of article urls from that domain
# for x in urls:
#     getdomain(x)



# sources = [SingleSource(articleURL=u) for u in urls[:100]]

# newspaper.news_pool.set(sources)
# newspaper.news_pool.join()

# multi=[]
# i=0
# print(len(sources))
# print(sources[:2])

# for s in sources:
#     i += 1
#     s.download_articles()
#     s.parse_articles()
#     print(s.description)
#     print(len(s.articles))
#     for art in s.articles:
#         print(len(art.text), art.title)
#     break
    # i+=1
    # try:
    #     (s.articles[0]).parse()
    #     txt = (s.articles[0]).text
    #     multi.append(txt)
    # except:
    #     pass

# article = atcl(url)
# article.download()
# article.parse()

articleText = dict()
inserts = ""

def getTxt(url):
    article = atcl(url, fetch_images = False)
    try:
        article.download()
        article.parse()
        articleText[url] = article.text if article.text is not None else "Empty article"
        data = "( '"+url+"', '"
        data += article.text.replace("'","").replace("\n","") if article.text is not None else "Empty article" 
        data += "' ) ,"
        return data
        # print("inserts : ", stmt+ inserts)
    except:
        return "(NULL, NULL) ,"

# for url in urls[:5]:
#     inserts += getTxt(url)
def parallelRead(n,m):
    pool = ThreadPool(25)

    # open the urls in their own threads
    # and return the results
    results=""
    for value in pool.map(getTxt, urls[n:m]):
        results += value

    # close the pool and wait for the work to finish 
    pool.close() 
    pool.join()
    return results

# with open('query.txt', '+w') as file:
#     file.write(basestmt)
for i in range(0,len(urls), 500):
    rows = cursor.execute(basestmt+parallelRead(i, i+500)[:-1])
    connection.commit()
    print("Batch Index : ", i , datetime.datetime.now(), "Rows Inserted : ", rows)


connection.close()


# for key, value in articleText.items():
#     print(key, '############ ', (value[:50] if value is not None else None ) )

