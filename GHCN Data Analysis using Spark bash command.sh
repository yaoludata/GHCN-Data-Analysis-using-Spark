# Processing
# Q1
# (a)
# Determine how the data is structured in /data/ghcnd/

hdfs dfs -ls /data/ghcnd/
hdfs dfs -ls /data/ghcnd/daily | head
hdfs dfs -ls /data/ghcnd/daily | tail

# count the how many files in daily

hdfs dfs -ls hdfs:///data/ghcnd/daily |wc -l

# (c)
# check the size of data and daily

hdfs dfs -du -s hdfs:///data/ghcnd
hdfs dfs -du -h hdfs:///data/ghcnd

# Q3

# create an output directory

hdfs dfs -mkdir hdfs:///user/ylu60/output/ghcnd/

# Analysis
# Q3
# (a)
# determine the default blocksize of HDFS  Total size:    134217728

hdfs getconf -confkey "dfs.blocksize"

# block for daily climate summaries for 2017: 1

hdfs fsck hdfs:///data/ghcnd/daily/2017.csv.gz -blocks

# block for daily climate summaries for 2010: 2
# the individual block sizes for the year 2010: 103590865 B

hdfs fsck hdfs:///data/ghcnd/daily/2010.csv.gz -blocks

# Q4
# (d)
# copy the output from HDFS

hdfs dfs -copyToLocal  hdfs:///user/ylu60/output/ghcnd/dailynz_temperaturemean.csv

#count the number of rows in the part file using wc -l bash command 447017

ls
ls  dailynz_temperaturemean.csv
wc -l dailynz_temperaturemean.csv/part-00000-ecd84e4f-6e44-4a49-8639-ccc2860a7e9e-c000.csv 

# (e)
#copy the output from HDFS

hdfs dfs -copyToLocal  hdfs:///user/ylu60/output/ghcnd/daily_rainfull.csv

# Challenges
# Q1
# copy the output from HDFS

hdfs dfs -copyToLocal  hdfs:///user/ylu60/output/ghcnd/daily_ch_snow.csv
