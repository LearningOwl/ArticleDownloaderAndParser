import pymysql

dbname = 'articles'
un = 'root'
pwd = 'A7105DFC-EC5B-2FBE-F849-81E089ACEB40'
host = '127.0.0.1'

def creteconnect():
    conn = pymysql.connect(host=host, port=3306, user=un, passwd=pwd, db=dbname)
    return conn





