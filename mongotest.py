__author__ = 'Robert Stanley'
import pymongo
import pytz
from bson.son import SON
import datetime
from datetime import timedelta
from django.conf import settings

#connection = pymongo.MongoClient('brm-sdb-1.tribaltech.com',27017)
connection = pymongo.MongoClient('10.40.1.110',27017)
db = connection.brm



#lookup last process time, plug into start date parameter
last_time = db.sentiment_report_processing.find_one({"action":"last_processed_date"},{'date':1})
lrd = last_time.get('date')

#update the last_processed_date, for the next run
d = datetime.datetime.utcnow()
db.sentiment_report_processing.update(
    { '_id': 1, 'action':"last_processed_date"},
    {"$set":{'date': d}},
    upsert=True)

#comparing last_update_time to last process time, get a list of the dates where there are changed records
results = db.brm_social_post.aggregate([
    {"$match":{'post_date':{'$exists': True},'last_update_time':{'$exists': True,"$gte":lrd}}},
    {
        "$project" : {
            'year_posted' : {"$year": "$post_date"}, 'month_posted' : {"$month": "$post_date"},'day_posted' : {"$dayOfMonth": "$post_date"}
        }
    },
    {"$group": {"_id": {'year': "$year_posted",'month':"$month_posted",'day':"$day_posted"}}}
])

for result_obj in results['result']:
        date_dict = result_obj['_id']
        startdate = datetime.datetime(date_dict['year'], date_dict['month'], date_dict['day'])
        enddate = startdate + datetime.timedelta(days=1)
        #print(startdate, enddate)
        #loop through the dates where there are new or changed records
        #one at a time pull the counts for those dates
        #save the counts into a new file with name based on date
        #move the file to hdfs (overwrite existing files)
        #nightly will rebuild table in hadapt, based on files in hdfs
        #PROBLEM SEEMS TO BE IN THE MATCH CLAUSE
        daily_totals = db.brm_social_post.aggregate([
            {"$match":{"post_date":{"$gte":startdate,"$lte":enddate}}},
            {
                "$project" : {
                    "tags": 1, "source": 1,'year_posted' : {"$year": "$post_date"}, 'month_posted' : {"$month": "$post_date"},'day_posted' : {"$dayOfMonth": "$post_date"},
                    'Neutral'  : {"$cond": [{"$eq": ["$salience.content.sentiment", 0]}, 1, 0]},
                    'Negative' : {"$cond": [{"$and":[{"$lt": ["$salience.content.sentiment", 0]},{"$gt": ["$salience.content.sentiment", -999]}]}, 1, 0]},
                    'Positive' : {"$cond": [{"$gt": ["$salience.content.sentiment", 0]}, 1, 0]},
                    }
            },
            {"$group": {"_id": {'source':"$source",'tags':"$tags",'year': "$year_posted",'month':"$month_posted",'day':"$day_posted"}, "count": {"$sum": 1},"countNegative":{"$sum":"$Negative"},"countNeutral":{"$sum":"$Neutral"},"countPositive":{"$sum":"$Positive"}}},
            {"$sort": SON([("count", -1), ("_id", -1)])}
        ])
            #use reportdate for the filename
        filename = startdate.strftime('%Y-%m-%d')
        #print(filename)
        for result_obj in daily_totals['result']:
            data_dict = result_obj['_id']
            date = (str(data_dict['year']) + "-" +  str(data_dict['month']) + "-" + str(data_dict['day']))
            tag = data_dict['tags']
            source = data_dict['source']
            count = result_obj['count']
            countPositive = result_obj['countPositive']
            countNegative = result_obj['countNegative']
            countNeutral = result_obj['countNeutral']
            data = (str(date) + "|" + str(tag) + "|" + str(source) + "|" + str(count) + "|" + str(countPositive) + "|" + str(countNegative) + "|" + str(countNeutral)+'\n')
            #print(data)
            #hdfs_path = '/app_figures_analytics/sales/' + filename
            hdfs_path = settings.HDFS_HOST_NAME + ':' + settings.HDFS_PORT + settings.HDFS_ROOT_FOLDER + '/socialmedia/sentiment' + filename
            logger.info('HDFS file path: %s' % hdfs_path)