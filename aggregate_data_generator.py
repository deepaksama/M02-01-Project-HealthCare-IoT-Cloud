from datetime import datetime, timedelta
from decimal import Decimal

from dynamodb_util import DynamoDBUtil
from config_reader import ConfigReader
import app_constants


class AggregateDataGenerator:
    def __init__(self):
        self._db_util = DynamoDBUtil()
        self._config_reader = ConfigReader()

    # Fetches sensor data for a given time range
    def get_data_between_time_range(self, table_name, start_time, end_time):
        print("Getting data for given time range .....")
        data = self._db_util.get_data_between_time_range(
            table_name, start_time, end_time)

        return data

    # Arranges data into sorted order for aggregate data generation
    def _arrange_data_for_aggregation(self, aggregate_data):
        print("Arranging data for aggregation .....")
        values = {}

        # Sample of aggregated values
        # ===============================
        # {
        #    "BSM_G101":{
        #       "HeartRate":{
        #          "2021-09-26T01:37:00Z":{
        #             "values":[
        #                82.0,
        #                82.0,
        #                75.0,
        #                79.0,
        #                95.0
        #             ]
        #          },
        #          "2021-09-26T01:38:00Z":{
        #             "values":[]
        #          },
        #          "2021-09-26T01:39:00Z":{
        #             "values":[]
        #          },
        #          "2021-09-26T01:40:00Z":{
        #             "values":[]
        #          },
        #          "2021-09-26T01:41:00Z":{
        #             "values":[]
        #          }
        #       },
        #       "SPO2":{
        #          "2021-09-26T01:40:00Z":{
        #             "values":[
        #                88.0
        #             ]
        #          }
        #       }
        #    }
        # }
        for entry in aggregate_data:
            time_truncated_to_minutes = datetime.strptime(
                entry['timestamp'], '%Y-%m-%dT%H:%M:%SZ').strftime('%Y-%m-%dT%H:%M:00Z')
            # if the value list does not exists for device id create empty list
            if entry['deviceid'] not in values.keys():
                values[entry["deviceid"]] = {}

            # if dictionary for the datatype does not exists then create an empty dictionary
            if entry['datatype'] not in values[entry["deviceid"]].keys():
                values[entry["deviceid"]][entry['datatype']] = {}

            # if dictionary for the minute does not exists then create an empty dictionary and empty value list for the timestamp
            if time_truncated_to_minutes not in values[entry["deviceid"]][entry['datatype']].keys():
                values[entry["deviceid"]][entry['datatype']
                                          ][time_truncated_to_minutes] = {}
                values[entry["deviceid"]][entry['datatype']
                                          ][time_truncated_to_minutes]["values"] = []

            values[entry["deviceid"]][entry['datatype']
                                      ][time_truncated_to_minutes]["values"].append(float(entry["value"]))

        return values

    # Generates aggregate data by minute from arranged sensor data values
    def aggregate_data(self, values):
        print("Aggregating data..")
        aggregate_data = []
        # Calculate aggregate data
        # For each device
        for device in values.keys():
            # For each datatype
            for datatype in values[device].keys():
                # For each minute
                for timestamp in values[device][datatype].keys():
                    values_list = values[device][datatype][timestamp]["values"]
                    aggregate_data_entry = {}
                    aggregate_data_entry['deviceid'] = device
                    aggregate_data_entry['datatype'] = datatype
                    aggregate_data_entry['timestamp'] = timestamp
                    aggregate_data_entry['minimum'] = Decimal(
                        str(min(values_list)))
                    aggregate_data_entry['maximum'] = Decimal(
                        str(max(values_list)))
                    aggregate_data_entry['average'] = Decimal(
                        str(round(sum(values_list)/len(values_list), 2)))

                    aggregate_data.append(aggregate_data_entry)

        return aggregate_data

    # Arranges data and generates aggregate data
    def get_aggeregate_data(self, start_time, end_time):
        aggregate_data = self.get_data_between_time_range(
            app_constants.TABLE_BSM_DATA, start_time, end_time)

        values = self._arrange_data_for_aggregation(aggregate_data)

        return self.aggregate_data(values)

    # Returns aggregate data for specific sensor datatype from given data
    def _get_aggreate_data_for_datatype(self, aggregate_data, datatype):
        return_data = []
        for data in aggregate_data:
            if data['datatype'] == datatype:
                return_data.append(data)
        return return_data

    # Bulk inserts aggregate data to Dynamodb
    def persist_aggregate_data(self, aggregate_data):
        db_util = DynamoDBUtil()

        # Create aggregate data tables if does not exist
        db_util.create_table_if_doesnot_exists(
            app_constants.TABLE_TEMPERATURE_AGGREGATE_DATA, "deviceid", 'S', "timestamp", 'S')
        db_util.create_table_if_doesnot_exists(
            app_constants.TABLE_SPO2_AGGREGATE_DATA, "deviceid", 'S', "timestamp", 'S')
        db_util.create_table_if_doesnot_exists(
            app_constants.TABLE_HEARTRATE_AGGREGATE_DATA, "deviceid", 'S', "timestamp", 'S')

        # Bulk insert aggregate data into tables
        db_util.bulk_insert(app_constants.TABLE_TEMPERATURE_AGGREGATE_DATA,
                            self._get_aggreate_data_for_datatype(aggregate_data, app_constants.DATATYPE_TEMPERATURE))
        db_util.bulk_insert(app_constants.TABLE_SPO2_AGGREGATE_DATA,
                            self._get_aggreate_data_for_datatype(aggregate_data,  app_constants.DATATYPE_SPO2))
        db_util.bulk_insert(app_constants.TABLE_HEARTRATE_AGGREGATE_DATA,
                            self._get_aggreate_data_for_datatype(aggregate_data, app_constants.DATATYPE_HEARTRATE))

    # Generate and Persist aggregate data
    def generate_aggregate_data(self, start_time, end_time):
        # Generating aggregate data
        aggregate_data = self.get_aggeregate_data(start_time, end_time)

        # Persist aggregate data
        self.persist_aggregate_data(aggregate_data)
