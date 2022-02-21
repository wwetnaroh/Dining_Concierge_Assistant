"""
This sample demonstrates an implementation of the Lex Code Hook Interface
in order to serve a sample bot which manages reservations for hotel rooms and car rentals.
Bot, Intent, and Slot models which are compatible with this sample can be found in the Lex Console
as part of the 'BookTrip' template.

For instructions on how to set up and test this bot, as well as additional samples,
visit the Lex Getting Started documentation http://docs.aws.amazon.com/lex/latest/dg/getting-started.html.
"""
import boto3
import json
import datetime
import time
import os
import dateutil.parser
import logging
import math
logger = logging.getLogger()
logger.setLevel(logging.DEBUG)

allowed_places = ['manhattan', 'midtown', 'uptown', 'downtown', 'times square']
cuisines = ['chinese', 'thai', 'american', 'indian',  'japanese', 'cuban', 'french', 'greek', 'indonesian']

# --- Helpers that build all of the responses ---

def push_msg_to_sqs(sqs_name, restaurant_info):
    client = boto3.client('sqs')
    url = client.get_queue_url(QueueName=sqs_name,)['QueueUrl']
    logging.info(url)

    try:
        send_response = client.send_message(QueueUrl=url, MessageBody=json.dumps(restaurant_info))
        print("Response from SQS after pushing: ", send_response)
    except ClientError as e:
        print("[ERROR]", e)
        return None

    return send_response


def elicit_slot(session_attributes, intent_name, slots, slot_to_elicit, message):
    return {
        'sessionAttributes': session_attributes,
        'dialogAction': {
            'type': 'ElicitSlot',
            'intentName': intent_name,
            'slots': slots,
            'slotToElicit': slot_to_elicit,
            'message': message
        }
    }

def close(session_attributes, fulfillment_state, message):
    response = {
        'sessionAttributes': session_attributes,
        'dialogAction': {
            'type': 'Close',
            'fulfillmentState': fulfillment_state,
            'message': message
        }
    }

    return response


def delegate(session_attributes, slots):
    return {
        'sessionAttributes': session_attributes,
        'dialogAction': {
            'type': 'Delegate',
            'slots': slots
        }
    }


# --- Helper Functions ---
def get_slots(intent_request):
    return intent_request['currentIntent']['slots']

def parse_int(n):
    try:
        return int(n)
    except ValueError:
        return float('nan')

def safe_int(n):
    """
    Safely convert n value to int.
    """
    try:
        return int(n)
    except ValueError:
        return float('nan')


def try_ex(func):
    """
    Call passed in function in try block. If KeyError is encountered return None.
    This function is intended to be used to safely access dictionary.

    Note that this function would have negative impact on performance.
    """

    try:
        return func()
    except KeyError:
        return None


def isvalid_date(date):
    try:
        dateutil.parser.parse(date)
        return True
    except ValueError:
        return False

def get_day_difference(later_date, earlier_date):
    later_datetime = dateutil.parser.parse(later_date).date()
    earlier_datetime = dateutil.parser.parse(earlier_date).date()
    return abs(later_datetime - earlier_datetime).days


def add_days(date, number_of_days):
    new_date = dateutil.parser.parse(date).date()
    new_date += datetime.timedelta(days=number_of_days)
    return new_date.strftime('%Y-%m-%d')


def build_validation_result(isvalid, violated_slot, message_content):
    return {
        'isValid': isvalid,
        'violatedSlot': violated_slot,
        'message': {'contentType': 'PlainText', 'content': message_content}
    }



#
def validate_dining_suggestion(city, cuisine, num_people, date, time, phone, info):
    print("I am running validate_dining_suggestion intern!!!!!!!!!!!!!!!!!!")
    print("time is ")
    print(time)
    #print()
    if city is not None:
        if city.lower() not in allowed_places:
            return build_validation_result(False,
                                           'City',
                                           'Sorry, the allowed places are only in Manhattan area. Please try again.')
        info["city"] = city

    if cuisine is not None and cuisine.lower() not in cuisines:
        return build_validation_result(False,
                                       'Cuisine',
                                       'Cuisine not available. Please try another.')
    else:
        info["cuisine"] = cuisines

    if date is not None:
        if not isvalid_date(date):
            return build_validation_result(False, 'Date',
                                           'I did not understand that, what date would you like to book?')
        elif datetime.datetime.strptime(date, '%Y-%m-%d').date() <= datetime.date.today():
            return build_validation_result(False, 'Date', 'You can come tomorrow. What time is suitable?')
        info["date"] = date

    if time is not None:
        if len(time) != 5:
            # Not a valid time; use a prompt defined on the build-time model.
            return build_validation_result(False, 'Time', None)

        hour, minute = time.split(':')
        hour = parse_int(hour)
        minute = parse_int(minute)
        if math.isnan(hour) or math.isnan(minute):
            # Not a valid time; use a prompt defined on the build-time model.
            return build_validation_result(False, 'Time', 'Not a valid time')

        if hour < 10 or hour > 16:
            # Outside of business hours
            return build_validation_result(False, 'Time',
                                           'Our business hours are from ten a m. to five p m. Can you specify a time during this range?')
        info["time"] = time

    if num_people is not None:
        num_people = int(num_people)
        if num_people > 20 or num_people < 0:
            return build_validation_result(False,
                                           'People',
                                           'Maximum 20 people allowed. Try again')
        info["num_people"] = num_people

    if phone is not None:
        if len(phone) != 10 or (not phone.isnumeric()):
            return build_validation_result(False, 'Phone', 'It is not a valid phone number. Please type again.')
        else:
            print("I have phone Number !! \n")
            info["PhoneNo"] = phone

    return build_validation_result(True, None, None)

""" --- Functions that control the bot's behavior --- """


# --- Intents ---
def greeting(intent_request):
    session_attributes = intent_request['sessionAttributes'] if intent_request['sessionAttributes'] is not None else {}
    slots = intent_request['currentIntent']['slots']
    name = slots['name']
    if name == None:
        rest = 'Hi, can I know your name? '
    else:
        rest = 'Hi '+str(name)+', how can I help?'
    return close(
        session_attributes,
        'Fulfilled',
        {
            'contentType': 'PlainText',
            'content':rest
        }
    )

def thankYou(intent_request):
    return {
        'dialogAction': {
            "type": "ElicitIntent",
            'message': {
                'contentType': 'PlainText',
                'content': 'You are welcome!'}
        }
    }

def dining_suggestion_intent(intent_request):
    print("I am running dining_suggestion intern!!!!!!!!!!!!!!!!!!")
    restaurant_info = dict.fromkeys(["city", "cuisine", "date", "time", "num_people", "PhoneNo"])

    city = get_slots(intent_request)["City"]
    cuisine = get_slots(intent_request)["Cuisine"]
    date = get_slots(intent_request)["Date"]
    time = get_slots(intent_request)["Time"]
    num_people = get_slots(intent_request)["People"]
    phone = get_slots(intent_request)["Phone"]
    source = intent_request['invocationSource']

    if source == 'DialogCodeHook':
        slots = get_slots(intent_request)
        if slots is not None:
            validation_result = validate_dining_suggestion(city, cuisine, num_people, date, time, phone, restaurant_info)
            if not validation_result['isValid']:
                slots[validation_result['violatedSlot']] = None
                print("Export from if not validation_result!!!!!!!!!!!!!")
                return elicit_slot(intent_request['sessionAttributes'],
                                   intent_request['currentIntent']['name'],
                                   slots,
                                   validation_result['violatedSlot'],
                                   validation_result['message'])

            if intent_request['sessionAttributes'] is not None:
                output_session_attributes = intent_request['sessionAttributes']
            else:
                output_session_attributes = {}
            print("Export because source == 'DialogCodeHook':!!!!!!!!!!!!!")
            return delegate(output_session_attributes, get_slots(intent_request))
    restaurant_info['city'] = city
    restaurant_info['cuisine'] = cuisine
    restaurant_info['date'] = date
    restaurant_info['time'] = time
    restaurant_info['num_people'] = num_people
    restaurant_info['PhoneNo'] = phone
    # print("I have estaurant_info to sqs!!!!!!!!!!!!!!!!!\n")
    # print(restaurant_info)
    response = push_msg_to_sqs('DiningMessage', restaurant_info)
    # print("I have pushsed message to sqs!!!!!!!!!!!!!!!!!")
    if response:
        print("Restaurant Info from search has been pushed to SQS.")
    else:
        print("[ERROR] SQSError: Failed to push restaurant info to SQS!")
    return close(intent_request['sessionAttributes'],
                 'Fulfilled',
                 {'contentType': 'PlainText',
                  'content': 'Thank you! You will recieve suggestion shortly'})


def dispatch(intent_request):
    """
    Called when the user specifies an intent for this bot.
    """
    intent_name = intent_request['currentIntent']['name']

    # Dispatch to your bot's intent handlers

    if intent_name == 'GreetingIntent':
        return greeting(intent_request)
    elif intent_name == 'ThankYouIntent':
        return thankYou(intent_request)
    elif intent_name == 'DiningSuggestionsIntent':
        #return diningSuggestion(intent_request)
        return dining_suggestion_intent(intent_request)
    raise Exception('Intent with name ' + intent_name + ' not supported')


# --- Main handler ---


def lambda_handler(event, context):
    """
    Route the incoming request based on intent.
    The JSON body of the request is provided in the event slot.
    """
    os.environ['TZ'] = 'America/New_York'
    time.tzset()
    logger.debug('event.bot.name={}'.format(event['bot']['name']))
    return dispatch(event)
