#This file is used to add restaurant info in to dynamodb
import boto3
from configs import ACCESS_KEY, ACCESS_ID
from constants import SUPPORTED_CUISINES, DYNAMO_DB_TABLE_NAME
import json

def get_db():
    """
    get db client
    :return:
    """
    db = boto3.resource('dynamodb', region_name='us-east-1', aws_access_key_id=ACCESS_ID,
                        aws_secret_access_key=ACCESS_KEY)
    return db

def load_cuisine_record(cuisine):
    file_content = open(cuisine+'_restaurant_data.json')
    cuisine_data = json.load(file_content)
    cuisine_dict = {}
    for restaurant, restaurant_info in cuisine_data.items():
        individual_restaurant_info = {'id': restaurant, 'cuisine': cuisine, 'name': restaurant_info['name'],
                                      'rating': int(restaurant_info['rating']), 'num_of_reviews': restaurant_info['num_of_reviews'],
                                      'address': restaurant_info['address'], 'zip_code': restaurant_info['zip_code'],
                                      'co-ordinates': json.dumps(restaurant_info['coordinates']),
                                      'insertedAtTimestamp': restaurant_info['insertedAtTimestamp']}
        cuisine_dict[restaurant] = json.dumps(individual_restaurant_info)
    return cuisine_dict

def load_data_into_dynamo_db(row, db):
    table = db.Table(DYNAMO_DB_TABLE_NAME)
    #print("Loading into Dynamodb:{}".format(row))
    count = 0
    for each_entry in row:
        try:
            response = table.put_item(Item=each_entry)
            if count % 50 == 0:
                print(count)
            count+=1
        except Exception as e:
            print('Failed to load the entry:{}. Error is:{}'.format(each_entry, e))

def main():
    dynamo_db_client = get_db()
    for cuisine in SUPPORTED_CUISINES:
        print("Loading data for cuisine:{}".format(cuisine))
        cuisine_dict = load_cuisine_record(cuisine=cuisine)
        print("Length of {}cuisine_dict: {}".format(cuisine, len(cuisine_dict)))
        restaurant_info_row = []
        for value in cuisine_dict.values():
            restaurant_info_row.append(eval(value))
        load_data_into_dynamo_db(restaurant_info_row, db=dynamo_db_client)


if __name__ == '__main__':
    main()
