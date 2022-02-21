#the same as yelp_api_json.py

from configs import BUSINESS_SEARCH_PATH, DEFAULT_LOCATION, LIMIT, BUSINESS_PATH
#from requests_handler import make_get_request_call
import json
import requests
import datetime
import string
from constants import SUPPORTED_CUISINES
import sys
from urllib.error import HTTPError
import requests
from configs import YELP_API_AUTHENTICATION_TOKEN, HOST_NAME, GET_CALL

num = 0
def make_get_request_call(path, parameter_dictionary=None):
    try:
        req = requests.request(method=GET_CALL, url=HOST_NAME + path, headers={
            'Authorization': 'Bearer %s' % YELP_API_AUTHENTICATION_TOKEN,
        }, params=parameter_dictionary)

        return req.json()

    except HTTPError as error:
        sys.exit(
            'Encountered HTTP error {0} on {1}:\n {2}\nAbort program.'.format(
                error.code,
                error.url,
                error.read(),
            )
        )


def scrape_all_restaurants(cuisine, offset=0):
    query_params = {'term': cuisine, 'location': DEFAULT_LOCATION, 'limit': LIMIT, 'offset': offset}
    businesses = make_get_request_call(path=BUSINESS_SEARCH_PATH, parameter_dictionary=query_params)
    return businesses.get('businesses')

def get_proper_name(restaurant_name):
    printable = set(string.printable)
    name = ''.join(filter(lambda x: x in printable, restaurant_name))
    return name

def scrape_restaurants(cuisine):
    global num
    cuisine_info_dict = {}
    count = 0
    while len(cuisine_info_dict) <= 1000:
        print("Current number is:{}".format(count))
        restaurants = scrape_all_restaurants(cuisine=cuisine, offset=count)
        if not restaurants:
            print('No {} restaurants found.'.format(cuisine))
            break

        for restaurant in restaurants:
            restaurant_info_dict = {}
            opensearch_info_dict = {}
            opensearch_dict = {}
            index_info_dict = {}
            index_sub_dict = {}
            restaurant_info_dict['id'] = restaurant['id']
            restaurant_info_dict['name'] = get_proper_name(restaurant.get('name', None))
            restaurant_info_dict['rating'] = restaurant.get('rating', None)
            restaurant_info_dict['num_of_reviews'] = restaurant.get('review_count', None)
            restaurant_info_dict['address'] = str(restaurant['location'].get('address1', None)) + str(
                restaurant['location'].get('address2', None))
            restaurant_info_dict['zip_code'] = restaurant['location'].get('zip_code', None)
            restaurant_info_dict['coordinates'] = restaurant.get('coordinates', None)
            restaurant_info_dict['insertedAtTimestamp'] = datetime.datetime.now().strftime("%m/%d/%Y, %H:%M:%S.%f")

            # for opensearch
            index_sub_dict['_index'] = 'restaurants'
            index_sub_dict['_type'] = 'Restaurant'
            index_sub_dict['_id'] = num
            index_info_dict['index'] = index_sub_dict

            opensearch_info_dict['id'] = restaurant['id']
            opensearch_info_dict['cuisine'] = cuisine
            opensearch_dict['Restaurant'] = opensearch_info_dict
            with open('opensearch.json', 'a') as openfile1:
                json.dump(index_info_dict, openfile1)
                openfile1.write("\n")
                json.dump(opensearch_dict, openfile1)
                openfile1.write("\n")
            cuisine_info_dict[restaurant['id']] = restaurant_info_dict
            num+=1

        count = count + 50
    return cuisine_info_dict

def main():
    for cuisine in SUPPORTED_CUISINES:
        print("Running for cuisine: {}".format(cuisine))
        cuisine_wise_restaurant_info = scrape_restaurants(cuisine)
        with open(cuisine + '_restaurant_data.json', 'w') as openfile:
            json.dump(cuisine_wise_restaurant_info, openfile)
    return

if __name__ == "__main__":
    main()





















