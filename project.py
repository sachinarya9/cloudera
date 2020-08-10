import re
from pyspark import  SparkContext,SparkConf
import os
from pyspark.sql import SQLContext,Row, HiveContext
os.environ["SPARK_HOME"] = "/usr/lib/spark"

conf = SparkConf()

sc = SparkContext(conf= conf)
sqlcontext = SQLContext(sc)
text_data = sc.textFile('file:/home/cloudera/Desktop/log_file.txt')

# print(text_data.collect())

split_rdd = text_data.map(lambda a:a.split('- -'))
# print(split_rdd.collect())


def clean_data(each_list):
    # print('ll',each_list)
    data_list = []
    date_time = ''
    get_column = ''
    status_code = ''
    site_info = ''
    we_browser_name = ''
    compatiblity = ''
    site_name = ''
    website_name = ''
    etl_web = ''
    etl1= ''
    # print(each_list)
    temp_list = each_list[1].split('"-"')
    # print('Temp list is',temp_list)
    match = re.search('\[[\w\/\:\s\+]+\]',temp_list[0])
    if match:
        # print('Match is',match.group())
        # print('Temp list at 0th position is', temp_list[0])
        temp_site_info = temp_list[0].replace(match.group(),'')
        # print('Temp site info is', temp_site_info)
        match_info = re.search('\"[\w\s\/\-\.\?\=\%\@\,\&]+\"',temp_site_info)
        if match_info:
            # print('Sachin')
            status_code = temp_site_info.replace(match_info.group(),'')
            etl_website = re.search('\"[a-zA-Z0-9\_\.\:\/\?\=\-\%\,\&]+', status_code)
            if etl_website:
                if 'http://etlhive.com/' in etl_website.group():
                    etl_web = 'http://etlhive.com/'
                    etl_site = etl_website.group().replace('http://etlhive.com/','').strip()
                else:
                    etl_site = etl_website.group()
                status_code = status_code.replace(etl_website.group(),'')
                etl1 = etl_site
            else:
                etl1 = ''
            # print(status_code)
            site_info = match_info.group()
        colon_split = match.group().split(':')
        time = colon_split[1]

        date = colon_split[0].replace('[','')
    match_site = re.search('\([\w\s\;\.\+\:\/\)]+',temp_list[1])
    if match_site:
        we_browser_name = temp_list[1].replace(match_site.group(),'').strip()
        we_browser_list = we_browser_name.split('/')
        we_browser_name = we_browser_list[0]
        # print(we_browser_name.strip())
        if ';' in match_site.group():
            temp_list2 = match_site.group().split(';')
            # print('Temp list is',temp_list2)
            compatiblity = temp_list2[0].replace('(','')
            site_name = temp_list2[1]
            if len(temp_list2) >= 3:
                website_name = temp_list2[2].replace(')','')
            else:
                website_name = ''
        else:
            temp_list2 = ''
            compatiblity = ''
            site_name = ''
            website_name = match_site.group()


    data_list.append(each_list[0].strip().strip('"'))
    data_list.append(date.strip())

    data_list.append(site_info.strip())
    data_list.append(status_code.strip())
    data_list.append(etl1.strip())
    we_browser_name = we_browser_name.strip(' ')
    data_list.append(we_browser_name.strip('"'))

    data_list.append(compatiblity.strip())
    data_list.append(site_name.strip())
    data_list.append(website_name.strip())
    data_list.append(etl_web)
    data_list.append(time.strip())
    return data_list

# data = split_rdd.map(lambda each_list:clean_data(each_list))
data = split_rdd.map(clean_data)
infer_data = data.map(lambda a:Row(ip = a[0],date = a[1],http_address = a[2],status_code =a[3],etl_website = a[4],browser_name = a[5],compatibility =a[6], version = a[7],wesite_name =a[8], etl_web = a[9], time = a[10]))
# print('Data is',data.collect())
infer_data.persist()
df = sqlcontext.createDataFrame(infer_data)
df.registerTempTable('site_data')
# print(infer_data.collect())
df.write.parquet('hdfs:/site_data.parquet', mode = 'overwrite')

# df.write.textFile('file:/home/cloudera/Desktop/site_data.txt')

# sqlcontext.read.save('file:/home/cloudera/Desktop/mycsv.csv')

# df.toPandas().to_csv('file:/home/cloudera/Desktop/mycsv.csv')

# df.write.save('/home/cloudera/Desktop/site_data.csv', format = 'csv')

# web_browser = sqlcontext.sql("select distinct(etl_website) from site_data")

hivecontext = HiveContext(sc)

# web_browser.write.parquet('file:/home/cloudera/Desktop/etl_sites', mode = 'overwrite')
# print('browser name',web_browser.collect())
# trending_technology = sqlcontext.sql("select etl_website, count(etl_website) from site_data group by etl_website")

# page_not_found = sqlcontext.sql("select browser_name ,count(browser_name) as  br_nb from site_data group by browser_name having br_nb = max(br_nb)")

# page_not_found = sqlcontext.sql("select browser_name ,count(browser_name) as br from site_data group by browser_name order by br desc limit 1")

# page_not_found = sqlcontext.sql("select browser_name, max(val) from (select count(browser_name) as val from site_data group by browser_name)")

# page_not_found = sqlcontext.sql("select browser_name ,count(browser_name) as br from site_data group by browser_name having max(br)")

# trending_technology.show()

trending_products = sqlcontext.sql("select date, etl_website, count(etl_website) as trend from site_data group by date,etl_website order by trend desc")

customer_trending = sqlcontext.sql("select date,ip,etl_web, count(etl_web) from site_data group by date, ip, etl_web having etl_web = 'http://etlhive.com/'")

peak_hours_of_click = sqlcontext.sql("select time, etl_web, count(time) from site_data group by time,etl_web having etl_web = 'http://etlhive.com/'")

# page_not_found.show()

# peak_hours_of_click.show()

# jdbcDF = sqlcontext.read \
#     .format("jdbc") \
#     .option("url", "jdbc:postgresql:dbserver") \
#     .option("dbtable", "schema.tablename") \
#     .option("user", "username") \
#     .option("password", "connect") \
#     .load()


hivecontext.sql("CREATE TABLE IF NOT EXISTS first_table (ip string,date string,http_address string,\
status_code string,etl_website string,browser_name string,compatibility string, version double,wesite_name string, etl_web string, time string) row format delimited fields terminated by ','")

hivecontext.sql("load data inpath 'hdfs:/site_data.parquet' overwrite into table first_table")
 # row format delimited fields terminated by ',' stored as text file")

hivecontext.sql("create table if not exists partition_table(ip string,date string,http_address string,\
status_code string,etl_website string,compatibility string, version double,wesite_name string, etl_web string, time string) partitioned by\
(browser_name string) row format delimited fields terminated by ','")

hivecontext.setConf("hive.exec.dynamic.partition", "true")
hivecontext.setConf("hive.exec.dynamic.partition.mode", "nonstrict")

# hivecontext.sql('from first_table ft insert overwrite partition_table PARTITION (browser_name) select ft.ip,ft.date,ft.http_address,\
# ft.status_code,ft.etl_website,ft.compatibility, ft.version ,ft.wesite_name, ft.etl_web, ft.time, ft.browser_name ')
df.write.partitionBy("browser_name").save("people_bucketed")