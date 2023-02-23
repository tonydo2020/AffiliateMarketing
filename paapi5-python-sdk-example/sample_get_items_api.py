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
from paapi5_python_sdk.models.get_items_request import GetItemsRequest
from paapi5_python_sdk.models.get_items_resource import GetItemsResource
from paapi5_python_sdk.models.partner_type import PartnerType
from paapi5_python_sdk.rest import ApiException
import pyodbc
import pandas as pd
from pandas import DataFrame
from datetime import datetime


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


def get_items ():
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
    item_ids = ["B09JZJRCHL"]
    """ Choose resources you want from GetItemsResource enum """
    """ For more details, refer: https://webservices.amazon.com/paapi5/documentation/get-items.html#resources-parameter """
    get_items_resource = [
        # GetItemsResource.ITEMINFO_TITLE,
        GetItemsResource.OFFERS_LISTINGS_PRICE,
        # GetItemsResource.IMAGES_PRIMARY_MEDIUM
        GetItemsResource.OFFERS_LISTINGS_PROMOTIONS
        # GetItemsResource.BROWSENODEINFO_BROWSENODES_ANCESTOR,
    ]

    """ Forming request """

    try:
        get_items_request = GetItemsRequest(
            partner_tag=partner_tag,
            partner_type=PartnerType.ASSOCIATES,
            marketplace="www.amazon.com",
            condition=Condition.NEW,
            item_ids=item_ids,
            resources=get_items_resource,
        )
    except ValueError as exception:
        print("Error in forming GetItemsRequest: ", exception)
        return

    try:
        """ Sending request """
        response = default_api.get_items(get_items_request)

        print("API called Successfully")
        print("Complete Response:", response)

        """ Parse response """
        if response.items_result is not None:
            #            print("Printing all item information in ItemsResult:")
            response_list = parse_response(response.items_result.items)
            for item_id in item_ids:
                #                print("Printing information about the item_id: ", item_id)
                if item_id in response_list:
                    item = response_list[item_id]
                    current = [None] * 10
                    if item is not None:
                        if item.asin is not None:
                            print("ASIN: ", item.asin)
                            current[0] = item.asin
                        if item.detail_page_url is not None:
                            print("DetailPageURL: ", item.detail_page_url)
                            current[1] = item.detail_page_url
                        if (
                                item.item_info is not None
                                and item.item_info.title is not None
                                and item.item_info.title.display_value is not None
                        ):
                            print("Title: ", item.item_info.title.display_value)
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
                                # print("Exception at buying price is: ", E)
                                print("No Buying Price Available")

                            try:
                                # print("Saving Amount: ",
                                #       item.offers.listings[0].price.savings.amount)
                                current[4] = item.offers.listings[0].price.savings.amount
                            except Exception as E:
                                # print("Exception at Saving Amount is: ", E)
                                print("No saving amount available")

                            try:
                                # print("Saving Percentage: ",
                                #       item.offers.listings[0].price.savings.percentage)
                                current[5] = item.offers.listings[0].price.savings.percentage
                            except Exception as E:
                                # print("Exception at Saving Percentage is: ", E)
                                print("No saving percentage available")

                            try:
                                orig_amt = item.offers.listings[0].price.savings.amount + item.offers.listings[
                                    0].price.amount
                                # print("Saving Original Amount: ", orig_amt)
                                current[6] = orig_amt
                                # print("current is: ", current)
                            except Exception as E:
                                # print("Exception at Original Amount is: ", E)
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
                                current[8] = datetime.now().strftime("%d/%m/%Y %H:%M")
                                # print("current at date is:", current)
                            except Exception as E:
                                print("Exception at Ingestion Date", E)

                            # try:
                            #     print("current list finished successfully, adding current date")
                            #     current.append(date.today())
                            #     print("current is: ", current)
                            # except Exception as e:
                            #     print("unable to add date")
                    try:
                        # my_sql_insert_query = '''
                        #                 INSERT INTO dbo.Amazon_Structure (ASIN, Affiliate_Link, Title,
                        #                 Discounted_Price, Saving_Amount, Saving_Percentage, Original_Price, Image_Link,
                        #                 Data_Ingested_On)
                        #                 VALUES (?,?,?,?,?,?,?,?,?)
                        #                 '''
                        my_sql_insert_query = '''
                                        INSERT INTO dbo.Amazon_Structure (ASIN, Affiliate_Link, Title,
                                        Discounted_Price, Saving_Amount, Saving_Percentage, Original_Price, Image_Link,
                                        Data_Ingested_On)
                                        VALUES (?,?,?,?,?,?,?,?,?)
                                        '''
                        # cursor.execute(my_sql_insert_query, current)
                        # conn.commit()
                        # print("Record inserted")
                    except Exception as E:
                        # print("Error occurred: ", E)
                        if '23000' in str(E):
                            print("Duplicate ASIN found, updating fields")
                            # my_sql_insert_query = '''
                            #                 UPDATE dbo.Amazon_Structure
                            #                 SET Affiliate_Link = ?
                            #                 WHERE ASIN = ?
                            #                 '''
                            # cursor.execute(my_sql_insert_query, current[1], current[0])
                        elif 'The SQL Contains' in str(E):
                            print("Missing Parameters")
                        else:
                            print("Unknown Error, error is: ", E)


                else:
                    print("Item not found, check errors")

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


def get_items_with_http_info ():
    """ Following are your credentials """
    """ Please add your access key here """
    access_key = "<YOUR ACCESS KEY>"

    """ Please add your secret key here """
    secret_key = "<YOUR SECRET KEY>"

    """ Please add your partner tag (store/tracking id) here """
    partner_tag = "<YOUR PARTNER TAG>"

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
    item_ids = ["059035342X", "B00X4WHP5E", "B00ZV9RDKK"]

    """ Choose resources you want from GetItemsResource enum """
    """ For more details, refer: https://webservices.amazon.com/paapi5/documentation/get-items.html#resources-parameter """
    get_items_resource = [
        GetItemsResource.ITEMINFO_TITLE,
        GetItemsResource.OFFERS_LISTINGS_PRICE,
    ]

    """ Forming request """
    try:
        get_items_request = GetItemsRequest(
            partner_tag=partner_tag,
            partner_type=PartnerType.ASSOCIATES,
            marketplace="www.amazon.com",
            condition=Condition.NEW,
            item_ids=item_ids,
            resources=get_items_resource,
        )
    except ValueError as exception:
        print("Error in forming GetItemsRequest: ", exception)
        return

    try:
        """ Sending request """
        response_with_http_info = default_api.get_items_with_http_info(
            get_items_request
        )

        """ Parse response """
        if response_with_http_info is not None:
            print("API called Successfully")
            print("Complete Response Dump:", response_with_http_info)
            print("HTTP Info:", response_with_http_info[2])

            response = response_with_http_info[0]
            if response.items_result is not None:
                print("Printing all item information in ItemsResult:")
                response_list = parse_response(response.items_result.items)
                for item_id in item_ids:
                    print("Printing information about the item_id: ", item_id)
                    if item_id in response_list:
                        item = response_list[item_id]
                        if item is not None:
                            if item.asin is not None:
                                print("ASIN: ", item.asin)
                            if item.detail_page_url is not None:
                                print("DetailPageURL: ", item.detail_page_url)
                            if (
                                    item.item_info is not None
                                    and item.item_info.title is not None
                                    and item.item_info.title.display_value is not None
                            ):
                                print("Title: ", item.item_info.title.display_value)
                            if (
                                    item.offers is not None
                                    and item.offers.listings is not None
                                    and item.offers.listings[0].price is not None
                                    and item.offers.listings[0].price.display_amount
                                    is not None
                            ):
                                print(
                                    "Buying Price: ",
                                    item.offers.listings[0].price.display_amount,
                                )
                    else:
                        print("Item not found, check errors")

            if response.errors is not None:
                print(
                    "\nPrinting Errors:\nPrinting First Error Object from list of Errors"
                )
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


def get_items_async ():
    """ Following are your credentials """
    """ Please add your access key here """
    access_key = "<YOUR ACCESS KEY>"

    """ Please add your secret key here """
    secret_key = "<YOUR SECRET KEY>"

    """ Please add your partner tag (store/tracking id) here """
    partner_tag = "<YOUR PARTNER TAG>"

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
    item_ids = ["059035342X", "B00X4WHP5E", "B00ZV9RDKK"]

    """ Choose resources you want from GetItemsResource enum """
    """ For more details, refer: https://webservices.amazon.com/paapi5/documentation/get-items.html#resources-parameter """
    get_items_resource = [
        GetItemsResource.ITEMINFO_TITLE,
        GetItemsResource.OFFERS_LISTINGS_PRICE,
    ]

    """ Forming request """
    try:
        get_items_request = GetItemsRequest(
            partner_tag=partner_tag,
            partner_type=PartnerType.ASSOCIATES,
            marketplace="www.amazon.com",
            condition=Condition.NEW,
            item_ids=item_ids,
            resources=get_items_resource,
        )
    except ValueError as exception:
        print("Error in forming GetItemsRequest: ", exception)
        return

    try:
        """ Sending request """
        thread = default_api.get_items(get_items_request, async_req=True)
        response = thread.get()

        print("API called Successfully")
        print("Complete Response:", response)

        """ Parse response """
        if response.items_result is not None:
            print("Printing all item information in ItemsResult:")
            response_list = parse_response(response.items_result.items)
            for item_id in item_ids:
                print("Printing information about the item_id: ", item_id)
                if item_id in response_list:
                    item = response_list[item_id]
                    if item is not None:
                        if item.asin is not None:
                            print("ASIN: ", item.asin)
                        if item.detail_page_url is not None:
                            print("DetailPageURL: ", item.detail_page_url)
                        if (
                                item.item_info is not None
                                and item.item_info.title is not None
                                and item.item_info.title.display_value is not None
                        ):
                            print("Title: ", item.item_info.title.display_value)
                        if (
                                item.offers is not None
                                and item.offers.listings is not None
                                and item.offers.listings[0].price is not None
                                and item.offers.listings[0].price.display_amount is not None
                        ):
                            print(
                                "Buying Price: ",
                                item.offers.listings[0].price.display_amount,
                            )
                else:
                    print("Item not found, check errors")

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


if __name__ == "__main__":
    conn = pyodbc.connect('Driver={SQL Server};'
                          'Server=TONY-PC\MSSQLSERVER01;'
                          'Database=BrokeBalances;'
                          'Trusted_Connection=yes;')
    cursor = conn.cursor()
    # broke_balance_df = pd.read_sql_query('SELECT * FROM dbo.Amazon_Structure', conn)
    # print(broke_balance_df)
    get_items()

    # query = "SELECT {} FROM dbo.Amazon_Structure".format("ASIN")
    # cursor.execute(query)
    # DF = DataFrame(cursor.fetchall())
    # print(DF.iloc[0])
    # print(DF)
# get_items_with_http_info()
# get_items_async()
