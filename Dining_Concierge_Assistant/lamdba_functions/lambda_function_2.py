import json
import random
import boto3
import requests


# from requests_aws4auth import AWS4Auth
# sqs msg format：{"city": null, "cuisine": null, "date": null, "time": null, "num_people": null, "PhoneNo": null}

def lambda_handler(event, context):
	# Polling from queue
	sqs_client = boto3.client('sqs')

	queue_url = "queue_url"
	client = boto3.client('sqs')
	queues = client.list_queues(QueueNamePrefix='DiningMessage')
	test_queue_url = queues['QueueUrls'][0]
	response = client.receive_message(
		QueueUrl=test_queue_url,
		AttributeNames=[
			'All'
		],
		MaxNumberOfMessages=10,
		MessageAttributeNames=[
			'All'
		],
		VisibilityTimeout=30,
		WaitTimeSeconds=0
	)
	if 'Messages' not in response:
		print("There are no messages in SQS!")

	else:
		print("Received {} messages".format(len(response['Messages'])))

		for message in response['Messages']:
			print("Processing message: {}".format(message))
			body = message['Body']
			print(body)

	# Getting required information from message
	if type(body) != dict:
		body = json.loads(body)

	cuisine = body['cuisine']
	location = body['city']
	no_of_people = body['num_people']
	dining_time = body['time']
	dinint_date = body['date']
	user_phone_number = body['PhoneNo']

	response = sqs_client.delete_message(QueueUrl=queue_url, ReceiptHandle=response['Messages'][0]['ReceiptHandle'])
	print("Message deleted successfully ")

    # Get restaurant id from open search
	open_search_host = 'open_search_host_url'
	index = 'restaurants'
	url = open_search_host + '/' + index + '/_search'
	aws_auth = ('', '')
	headers = {"Content-Type": "application/json"}
	# print("The url is: " + str(url))

	params = {
		"query": {
			"match": {
				"Restaurant.cuisine": cuisine
			}
		}

	}

	r = requests.get(url, auth=aws_auth, headers=headers, data=json.dumps(params))

	total_results = len(r.json()['hits']['hits'])
	# print("Total results:{}***********".format(total_results))

	if total_results < 5:
		no_of_suggestions = total_results
	else:
		no_of_suggestions = 5

	list_of_restaurant_ids = []
	for i in range(0, no_of_suggestions):
		list_of_restaurant_ids.append(r.json()['hits']['hits'][i]['_source']['Restaurant']['id'])

	# print("List of restaurant ids:{}".format(list_of_restaurant_ids))

	# Dynamo DB
	db = boto3.resource('dynamodb')
	table = db.Table('yelp-restaurants')
	each_restaurant_info = {}
	random_restaurants = random.sample(list_of_restaurant_ids, no_of_suggestions)
	counter = 0

	for i in range(0, no_of_suggestions):
		response = table.get_item(Key={'id': random_restaurants[i]})
		# print("printing dynamo db response: {}".format(response))
		if 'Item' in response.keys():
			restaurant_info = response['Item']
			each_restaurant_info[counter] = {'id': restaurant_info['id'],
											 'name': restaurant_info['name'],
											 'rating': float(restaurant_info['rating']),
											 'address': restaurant_info['address'],
											 'num_of_reviews': float(restaurant_info['num_of_reviews']),
											 'zip_code': restaurant_info['zip_code']}
			counter = counter + 1
		else:
			print("Item key not found.")

	# Message formation:
	opening_message = " Hello! Here are my {} restaurant suggestions for {} people, for the date {} at time {}: ".format(
		cuisine, no_of_people, dinint_date, dining_time)
	restaurant_message = ""
	for i in range(0, counter):
		restaurant_message = restaurant_message + "{}. {}, located at {}, ".format(i + 1,
																				   each_restaurant_info[i]['name'],
																				   each_restaurant_info[i]['address'])
	restaurant_message = restaurant_message[:-2]
	total_message = opening_message + restaurant_message.replace("None", "") + ". Enjoy your meal!"
	print("restaurant_message is")
	print("FINAL MESSAGE:{}".format(total_message))

	# Storing Previous Recommendations:
	table_name = "user-information"
	db_client = boto3.resource('dynamodb')
	user_table = db_client.Table(table_name)

	# push restaurant search info to dynamodb
	row_to_be_added = {'previous_message': restaurant_message, 'location': location,
					   'cuisine': cuisine}
	response = user_table.put_item(Item=row_to_be_added)
	print("Pushed user records to db with response:{}".format(response))

	# SNS Notification

	"""
    total_message: 
    "Hello! Here are my mexican restaurant suggestions for 13 people, for today at 20:00: 
    1. The Maze, located at 32 W 32nd StFl 3. Enjoy your meal!"
    """
	sns_client = boto3.client('sns', region_name="us-east-1")
	user_phone_number = "1" + str(user_phone_number)
	print(total_message)
	sns_response = sns_client.publish(PhoneNumber=user_phone_number, Message=total_message, MessageAttributes={
		'AWS.SNS.SMS.SMSType': {
			'DataType': 'String',
			'StringValue': 'Transactional'
		}
	})
	print("sns_request response:{}".format(sns_response))

