import psycopg2


conn = psycopg2.connect("dbname=news")
cur = conn.cursor()

#First Q 

print("What are the most popular three articles of all time?")
cur.execute(''' select articles.title, count(*) as views from articles,log
where concat('/article/',articles.slug) = log.path
group by title order by views desc limit 3
''')

for indx,row in enumerate(cur.fetchall()):
    print(indx+1,"-",row[0]," has been viewed: ",row[1]," times")

#Second Q
print("Who are the most popular article authors of all time?")
conn1= psycopg2.connect("dbname=news")
cur1= conn1.cursor()
cur1.execute('''
select authors.name, count(articles.author) as view from articles,authors,log 
where concat('/article/',articles.slug) = log.path
and authors.id = articles.author
group by authors.name
order by view desc''')


for indx,row in enumerate(cur1.fetchall()):
    print(indx+1,"-", row[0]," has ",row[1]," views" )



#Third Q
print("On which days did more than 1% of requests lead to errors?")

cur1.execute('''
CREATE  VIEW e404 as
select date(time) as day, count(*) as req4 from log 
where status LIKE '%404%' group by day;


create view allof as
select date(time) as day, count(*) as req from log group by day;

select allof.day, round(   (e404.req4*1.0/ allof.req),3) as perc from e404 join allof ON allof.day=e404.day where ( round(((e404.req4*1.0)/allof.req),3)>0.01)order by perc desc limit 1
''')
for indx in(cur1.fetchall()):
    print(indx[0], " with ", indx[1]*100,"% Errors")