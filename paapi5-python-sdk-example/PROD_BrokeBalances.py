# -*- coding: utf-8 -*-

"""
 Copyright 2019 Amazon.com, Inc. or its affiliates. All Rights Reserved.

 Licensed under the Apache License, Version 2.0 (the "License").
 You may not use this file except in compliance with the License.
 A copy of the License is located at

     http://www.apache.org/licenses/LICENSE-2.0

 or in the "license" file accompanying this file. This file is distributed
 on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 express or implied. See the License for the specific language governing
 permissions and limitations under the License.
"""

"""
 ProductAdvertisingAPI

 https://webservices.amazon.com/paapi5/documentation/index.html

"""

"""
This sample code snippet is for ProductAdvertisingAPI 5.0's GetItems API

For more details, refer:
https://webservices.amazon.com/paapi5/documentation/get-items.html

"""

from paapi5_python_sdk.api.default_api import DefaultApi
from paapi5_python_sdk.models.condition import Condition
from paapi5_python_sdk.models.get_variations_request import GetVariationsRequest
from paapi5_python_sdk.models.get_variations_resource import GetVariationsResource
from paapi5_python_sdk.models.get_items_request import GetItemsRequest
from paapi5_python_sdk.models.get_items_resource import GetItemsResource
from paapi5_python_sdk.models.partner_type import PartnerType
from paapi5_python_sdk.rest import ApiException
import pyodbc
import pandas as pd
import random
from random import randint
from pandas import DataFrame
import json
import facebook as fb
import tweepy
import requests
from datetime import datetime
import datetime
import time
import shutil
from time import sleep


def tweet():

    # main twitter acc, using for testing
    twitter_auth_keys = {
        "consumer_key": "",
        "consumer_secret": "",
        "access_token": "",
        "access_token_secret": ""
    }

    auth = tweepy.OAuthHandler(
        twitter_auth_keys['consumer_key'],
        twitter_auth_keys['consumer_secret']
    )
    auth.set_access_token(
        twitter_auth_keys['access_token'],
        twitter_auth_keys['access_token_secret']
    )
    api = tweepy.API(auth)


    random.seed
    query = """SELECT * 
            FROM dbo
            WHERE CAST(Saving_Percentage as float) > 15 and Saving_Percentage IS NOT NULL
            """

    data = pd.read_sql(query,conn)
    try:
        index = random.randint(0,len(data))
        if data['Twitter_Posted_Date'][index] is None:

            url = str(data['Image_Link'][index])
            res = requests.get(url, stream = True)

            if res.status_code == 200:
                with open("image.jpg", 'wb') as f:
                    shutil.copyfileobj(res.raw,f)
                print("Image successfully Downloaded:")
            if data['Alias_Title'][index] is not None:
                title = str(data['Alias_Title'][index])
            else:
                title = str(data['Title'][index])
            media = api.media_upload("image.jpg")
            tweet = title + " \nCurrently: $" + str(data['Discounted_Price'][index]) + "\nRetails for: $" + str(data['Original_Price'][index]) + ' #Deals \n' + str(data['Affiliate_Link'][index])
            # post_result = api.update_status(status=tweet, media_ids=[media.media_id])
            fb_client.put_photo(open("image.jpg", "rb"), message = tweet)
            my_sql_insert_query = '''
                            UPDATE dbo.Amazon_Structure
                            SET Twitter_Posted_Date = ?
                            WHERE ASIN = ?
                            '''
            cursor.execute(my_sql_insert_query, datetime.datetime.now().strftime("%d/%m/%Y %H:%M"), data['ASIN'][index])
            conn.commit()


        elif data['Twitter_Posted_Date'][index] is not None:
            b = datetime.datetime.now() - (datetime.datetime.strptime(data['Twitter_Posted_Date'][index], '%d/%m/%Y %H:%M'))
            # print("b is: ", b)
            minutes = divmod(b.total_seconds(), 60)
            # print("minutes is: ", int(minutes[0]))
            if int(minutes[0]) > 1440:

                url = str(data['Image_Link'][index])
                res = requests.get(url, stream=True)

                if res.status_code == 200:
                    with open("image.jpg", 'wb') as f:
                        shutil.copyfileobj(res.raw, f)
                    print("Image successfully Downloaded:")
                if data['Alias_Title'][index] is not None:
                    title = str(data['Alias_Title'][index])
                else:
                    title = str(data['Title'][index])
                media = api.media_upload("image.jpg")
                tweet = title + " \n\nGoing For $" + str(data['Discounted_Price'][index]) + ", was: $" + str(
                    data['Original_Price'][index]) + '! #Deals \n' + str(data['Affiliate_Link'][index])

                fb_client.put_photo(open("image.jpg", "rb"), message=tweet)
                my_sql_insert_query = '''
                                UPDATE dbo.Amazon_Structure
                                SET Twitter_Posted_Date = ?
                                WHERE ASIN = ?
                                '''
                cursor.execute(my_sql_insert_query, datetime.datetime.now().strftime("%d/%m/%Y %H:%M"),
                               data['ASIN'][index])
                conn.commit()

            elif int(minutes[0]) < 1440:
                print(data['ASIN'][index])
                print("has been less than 24 hours, finding next item")
                pass

    except Exception as E:
        print("Exception Error is: ", E, E.args)
        pass
        # while True:
    # pass

def get_variations(list_holder, list_asin):

    """ Following are your credentials """
    """ Please add your access key here """
    access_key = ""

    """ Please add your secret key here """
    secret_key = ""

    """ Please add your partner tag (store/tracking id) here """
    partner_tag = ""

    """ PAAPI host and region to which you want to send request """
    """ For more details refer: https://webservices.amazon.com/paapi5/documentation/common-request-parameters.html#host-and-region"""
    host = "webservices.amazon.com"
    region = "us-east-1"

    """ API declaration """
    default_api = DefaultApi(
        access_key=access_key, secret_key=secret_key, host=host, region=region
    )

    """ Request initialization"""

    """ Specify ASIN """
    list_holder.append(list_asin)
    asin = list_asin.replace("\n","")
    # print("asin is: ", asin)
    """ Specify language of preference """
    """ For more details, refer https://webservices.amazon.com/paapi5/documentation/locale-reference.html"""
    languages_of_preference = ["en_US"]

    """ Choose resources you want from GetVariationsResource enum """
    """ For more details, refer: https://webservices.amazon.com/paapi5/documentation/get-variations.html#resources-parameter """
    get_variations_resources = [
        GetVariationsResource.ITEMINFO_TITLE,
        GetVariationsResource.OFFERS_LISTINGS_PRICE,
        GetVariationsResource.VARIATIONSUMMARY_VARIATIONDIMENSION,
    ]

    """ Forming request """
    try:
        get_variations_request = GetVariationsRequest(
            partner_tag=partner_tag,
            partner_type=PartnerType.ASSOCIATES,
            marketplace="www.amazon.com",
            languages_of_preference=languages_of_preference,
            asin=asin,
            resources=get_variations_resources,
        )
    except ValueError as exception:
        print("Error in forming GetVariationsRequest: ", exception)
        return

    try:
        """ Sending request """
        response = default_api.get_variations(get_variations_request)

        print("API called Successfully")

        """ Parse response """
        if response.variations_result is not None:
            if (
                response.variations_result.variation_summary is not None
                and response.variations_result.variation_summary.variation_count
                is not None
            ):
                print(
                    "VariationCount: ",
                    response.variations_result.variation_summary.variation_count,
                )

            for i in range(len(response.variations_result.items)):
                if response.variations_result.items[i] is not None:
                    if response.variations_result.items[i].asin is not None:
                        print("ASIN: ", response.variations_result.items[i].asin)
                    if response.variations_result.items[i].detail_page_url is not None:
                        print("DetailPageURL: ", response.variations_result.items[i].detail_page_url)
                        list_holder.append(response.variations_result.items[i].asin)

        if response.errors is not None:
            print("\nPrinting Errors:\nPrinting First Error Object from list of Errors")
            print("Error code", response.errors[0].code)
            print("Error message", response.errors[0].message)

    except ApiException as exception:
        print("Error calling PA-API 5.0!")
        print("Status code:", exception.status)
        print("Errors :", exception.body)
        print("Request ID:", exception.headers["x-amzn-RequestId"])

    except TypeError as exception:
        print("TypeError :", exception)

    except ValueError as exception:
        print("ValueError :", exception)

    except Exception as exception:
        print("Exception :", exception)


def update_asin_list():
    query = """SELECT ASIN 
            FROM dbo.Amazon_Structure
            """
    cursor.execute(query)
    database_asin_list = cursor.fetchall()
    asin_list = [str(asin[0]) for asin in database_asin_list]
    with open('new_list.txt', 'r') as file:
        variation_asin_list = []
        for line in file:
            time.sleep(1)
            get_variations(variation_asin_list, line)
            for asin in variation_asin_list:
                try:
                    if asin.rstrip('\n') not in asin_list:
                        # print("here")
                        current = [None] * 9
                        current[0] = asin.rstrip('\n')
                        my_sql_insert_query = '''
                                        INSERT INTO dbo.Amazon_Structure (ASIN, Affiliate_Link, Title,
                                        Discounted_Price, Saving_Amount, Saving_Percentage, Original_Price, Image_Link,
                                        Data_Ingested_On)
                                        VALUES (?,?,?,?,?,?,?,?,?)
                                        '''
                        cursor.execute(my_sql_insert_query, current)
                        conn.commit()
                except Exception as E:
                    pass
    file.close()

    file = open('new_list.txt', 'w')
    file.close()


def parse_response (item_response_list):
    """
    The function parses GetItemsResponse and creates a dict of ASIN to Item object
    :param item_response_list: List of Items in GetItemsResponse
    :return: Dict of ASIN to Item object
    """
    mapped_response = {}
    for item in item_response_list:
        mapped_response[item.asin] = item
    return mapped_response


def divide_chunks (l, n):
    for i in range(0, len(l), n):
        yield l[i:i + n]


def get_items (set_of_ten_asins):
    """ Following are your credentials """
    """ Please add your access key here """
    access_key = ""

    """ Please add your secret key here """
    secret_key = ""

    """ Please add your partner tag (store/tracking id) here """
    partner_tag = ""

    """ PAAPI host and region to which you want to send request """
    """ For more details refer: https://webservices.amazon.com/paapi5/documentation/common-request-parameters.html#host-and-region"""
    host = "webservices.amazon.com"
    region = "us-east-1"

    """ API declaration """
    default_api = DefaultApi(
        access_key=access_key, secret_key=secret_key, host=host, region=region
    )

    """ Request initialization"""

    """ Choose item id(s) """

    """ Choose resources you want from GetItemsResource enum """
    """ For more details, refer: https://webservices.amazon.com/paapi5/documentation/get-items.html#resources-parameter """
    get_items_resource = [
        GetItemsResource.ITEMINFO_TITLE,
        GetItemsResource.OFFERS_LISTINGS_PRICE,
        GetItemsResource.IMAGES_PRIMARY_LARGE,
    ]

    """ Forming request """

    try:
        get_items_request = GetItemsRequest(
            partner_tag=partner_tag,
            partner_type=PartnerType.ASSOCIATES,
            marketplace="www.amazon.com",
            condition=Condition.NEW,
            item_ids=set_of_ten_asins,
            resources=get_items_resource,
        )
    except ValueError as exception:
        print("Error in forming GetItemsRequest: ", exception)
        return

    try:
        """ Sending request """
        response = default_api.get_items(get_items_request)

        print("API called Successfully")
#        print("Complete Response:", response)

        """ Parse response """
        if response.items_result is not None:
            #            print("Printing all item information in ItemsResult:")
            response_list = parse_response(response.items_result.items)
            for item_id in item_ids:
                #                print("Printing information about the item_id: ", item_id)
                if item_id in response_list:
                    item = response_list[item_id]
                    current = [None] * 9
                    if item is not None:
                        if item.asin is not None:
                            # print("ASIN: ", item.asin)
                            current[0] = item.asin
                        if item.detail_page_url is not None:
                            # print("DetailPageURL: ", item.detail_page_url)
                            current[1] = item.detail_page_url
                        if (
                                item.item_info is not None
                                and item.item_info.title is not None
                                and item.item_info.title.display_value is not None
                        ):
                            # print("Title: ", item.item_info.title.display_value)
                            current[2] = item.item_info.title.display_value
                        if (
                                item.offers is not None
                                and item.offers.listings is not None
                                and item.offers.listings[0].price is not None
                                and item.offers.listings[0].price.display_amount is not None
                        ):
                            try:
                                # print("Buying Price: ", item.offers.listings[0].price.amount)
                                current[3] = item.offers.listings[0].price.amount
                            except Exception as E:
                                print("Exception at buying price is: ", E)
                                print("No Buying Price Available")

                            try:
                                # print("Saving Amount: ",
                                #       item.offers.listings[0].price.savings.amount)
                                current[4] = item.offers.listings[0].price.savings.amount
                            except Exception as E:
                                print("Exception at Saving Amount is: ", E)
                                print("No saving amount available")

                            try:
                                # print("Saving Percentage: ",
                                #       item.offers.listings[0].price.savings.percentage)
                                current[5] = item.offers.listings[0].price.savings.percentage
                            except Exception as E:
                                print("Exception at Saving Percentage is: ", E)
                                print("No saving percentage available")

                            try:
                                orig_amt = item.offers.listings[0].price.savings.amount + item.offers.listings[
                                    0].price.amount
                                # print("Saving Original Amount: ", orig_amt)
                                current[6] = orig_amt
                                # print("current is: ", current)
                            except Exception as E:
                                print("Exception at Original Amount is: ", E)
                                print("Unable to calculate original amount")

                            try:
                                # print("Image link: ", item.images.primary.large.url)
                                current[7]=item.images.primary.large.url
                                # print("current at image is: ", current)

                            except Exception as E:
                                print("Exception at Image is: ", E)
                                print(current)
                            try:
                                # print("Including Ingestion date")
                                current[8] = datetime.datetime.now().strftime("%d/%m/%Y %H:%M")
                                # print("current at date is:", current)
                            except Exception as E:
                                print("Exception at Ingestion Date", E)

                    try:
                        my_sql_insert_query = '''
                                        INSERT INTO dbo.Amazon_Structure (ASIN, Affiliate_Link, Title,
                                        Discounted_Price, Saving_Amount, Saving_Percentage, Original_Price, Image_Link,
                                        Data_Ingested_On)
                                        VALUES (?,?,?,?,?,?,?,?,?)
                                        '''
                        cursor.execute(my_sql_insert_query, current)
                        conn.commit()
                        print("Record inserted")
                    except Exception as E:
                        # print("Error occurred: ", E)
                        if '23000' in str(E):
                            print("Duplicate ASIN found, updating fields")
                            my_sql_insert_query = '''
                                            UPDATE dbo.Amazon_Structure 
                                            SET Affiliate_Link = ? ,
                                                 Title = ? ,
                                                Discounted_Price = ?,
                                                Saving_Amount = ?,
                                                Saving_Percentage = ?,
                                                Original_Price = ?,
                                                Image_Link = ?,
                                                Data_Ingested_On = ? 
                                            WHERE ASIN = ?
                                            '''
                            cursor.execute(my_sql_insert_query, current[1], current[2], current[3],
                                           current[4], current[5], current[6], current[7], current[8], current[0])
                            # cursor.execute(my_sql_insert_query, current[1], current[0])
                            conn.commit()
                        elif 'The SQL Contains' in str(E):
                            print("Missing Parameters")
                        else:
                            print("Unknown Error, error is: ", E)
                # else:
                #     print("Item_id not found, item_id is", item_id)

        if response.errors is not None:
            print("\nPrinting Errors:\nPrinting First Error Object from list of Errors")
            print("Error code", response.errors[0].code)
            print("Error message", response.errors[0].message)

    except ApiException as exception:
        print("Error calling PA-API 5.0!")
        print("Status code:", exception.status)
        print("Errors :", exception.body)
        print("Request ID:", exception.headers["x-amzn-RequestId"])

    except TypeError as exception:
        print("TypeError :", exception)

    except ValueError as exception:
        print("ValueError :", exception)

    except Exception as exception:
        print("Exception here :", exception)
        pass



if __name__ == "__main__":
    conn = pyodbc.connect('Driver={SQL Server};'
                          'Server='
                          'Database='
                          'Trusted_Connection=yes;')
    cursor = conn.cursor()
    print(conn)
    # facebook access
    access_token = ""
    fb_client = fb.GraphAPI(access_token)
    options = ['Add New ASINs', 'Update Database', 'Tweet', 'Quit']
    user_input = ''
    input_message = "Pick an option:\n"
    for index, item in enumerate(options):
        input_message += f'{index + 1}) {item}\n'

    input_message += 'Your choice: '
    while user_input not in map(str, range(1, len(options) + 1)):
        user_input = input(input_message)
    if user_input == '1':
        update_asin_list()

    elif user_input == '2':
        query = """SELECT ASIN FROM dbo.Amazon_Structure """
        cursor.execute(query)
        database_asin_list = cursor.fetchall()
        item_ids = [str(asin[0]) for asin in database_asin_list]
        x = list(divide_chunks(item_ids, 10))
        for i in range(len(x)):
            get_items(x[i])
            time.sleep(1)

    elif user_input == '3':
        while True:
            tweet()
            sleep(randint(60, 90))
        pass
    elif user_input == '4':
        quit()

