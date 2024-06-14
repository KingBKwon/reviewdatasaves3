import pymongo
from dotenv import load_dotenv
import os
from load_file.get_txt import upload_s3
load_dotenv()

MONGO_USERNAME = os.getenv("MONGO_USERNAME")
MONGO_PASSWORD = os.getenv("MONGO_PASSWORD")
MONGO_HOST = os.getenv("MONGO_HOST")
MONGO_PORT = os.getenv("MONGO_PORT")
MONGO_DB = os.getenv("MONGO_DB")

MONGO_DB_URL = f"mongodb://{MONGO_USERNAME}:{MONGO_PASSWORD}@{MONGO_HOST}:{MONGO_PORT}"
# MongoDB에 접속
client = pymongo.MongoClient(MONGO_DB_URL)

# 데이터베이스 접속 (mydatabase)
db = client['hellody']

collection = db['REVIEW']
input_data = ''
reviews = list(collection.find({}, {'_id':0, 'REVIEW_ID':0, 'REVIEW_WDATE':0, 'REVIEW_MDATE':0}))
for review in reviews:
    print(review)
    vod_collection = db['VOD']
    vod_id = review['VOD_ID']
    vod_type = vod_collection.find_one({'VOD_ID':vod_id}, {'_id':0, 'TYPE':1})
    if vod_type['TYPE'] == '영화':
        movie_collection = db['MOVIES']
        movie_id = movie_collection.find_one({'VOD_ID': vod_id}, {'_id':0, 'MOVIE_ID':1})
        movie_id = movie_id['MOVIE_ID']
        comment = review['COMMENT']
        rating = review['RATING']
        user_id = review['USER_ID']
        input_data += f'{user_id},{vod_id},{movie_id},{comment},{rating}\n'
upload_s3(input_data)

print(input_data)