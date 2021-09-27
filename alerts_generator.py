from dynamodb_util import DynamoDBUtil
from config_reader import ConfigReader
import app_constants


class AlertsGenerator:
    def __init__(self):
        self._db_util = DynamoDBUtil()
        self._config_reader = ConfigReader()

    def get_data_between_time_range(self, table_name, start_time, end_time):
        data = self._db_util.get_data_between_time_range(
            table_name, start_time, end_time)

        return data

    def _parse_dynamodb_object(self, dict_list):
        return_list = []
        for entry in dict_list:
            temp = {}
            temp['deviceid'] = entry['deviceid']
            temp['datatype'] = entry['datatype']
            temp['timestamp'] = entry['timestamp']
            temp['minimum'] = float(entry['minimum'])
            temp['maximum'] = float(entry['maximum'])
            temp['average'] = float(entry['average'])
            return_list.append(temp)
        return return_list

    def generate_anomaly_data_for_datatype(self, data, datatype):
        config_data = self._config_reader.get_config_for_datatype(datatype)
        deviation_counter = 0
        first_anomaly_entry = None
        breach_conditions = []
        alerts = []
        for entry in data:
            if entry['average'] < config_data['min'] or entry['average'] > config_data['max']:
                # Capture first anomaly entry
                if not first_anomaly_entry:
                    first_anomaly_entry = entry
                    first_anomaly_entry['breach_condition'] = ''
                    if entry['average'] < config_data['min']:
                        breach_conditions.append('Min')
                    if entry['average'] > config_data['max']:
                        breach_conditions.append('Max')
                deviation_counter = deviation_counter + 1
            else:
                # Reset trigger counter and first anomaly entry
                first_anomaly_entry = None
                deviation_counter = 0
                breach_conditions = []

            if deviation_counter >= config_data['trigger_count']:
                first_anomaly_entry['breach_condition'] = ' And '.join(str(condition) for condition in set(breach_conditions))
                # Add first anomaly entry captured to alerts
                alerts.append(first_anomaly_entry)
                # Reset trigger counter and first anomaly entry
                first_anomaly_entry = None
                deviation_counter = 0
                breach_conditions = []
        return alerts

    def generate_alerts_for_datatype(self, start_time, end_time, datatype):
        # Determine the aggregate and alerts tables for given datatype
        if datatype == app_constants.DATATYPE_TEMPERATURE:
            aggregate_data_table_name = app_constants.TABLE_TEMPERATURE_AGGREGATE_DATA
            alerts_table_name = app_constants.TABLE_TEMPERATURE_ALERTS
        elif datatype == app_constants.DATATYPE_SPO2:
            aggregate_data_table_name = app_constants.TABLE_SPO2_AGGREGATE_DATA
            alerts_table_name = app_constants.TABLE_SPO2_ALERTS
        elif datatype == app_constants.DATATYPE_HEARTRATE:
            aggregate_data_table_name = app_constants.TABLE_HEARTRATE_AGGREGATE_DATA
            alerts_table_name = app_constants.TABLE_HEARTRATE_ALERTS

        # Get aggregate data of the datatype
        aggregate_data = self.get_data_between_time_range(
            aggregate_data_table_name, start_time, end_time)

        # Scan aggregate data for anomalies
        alerts = self.generate_anomaly_data_for_datatype(
            aggregate_data, datatype)

        # Create alerts table for respective datatype
        self._db_util.create_table_if_doesnot_exists(
            alerts_table_name, 'deviceid', 'S', 'timestamp', 'S')

        # Push alerts into alerts table
        self._db_util.bulk_insert(alerts_table_name, alerts)

    def generate_alerts_for_time_range(self, start_time, end_time):
        self.generate_alerts_for_datatype(
            start_time, end_time, app_constants.DATATYPE_TEMPERATURE)
        self.generate_alerts_for_datatype(
            start_time, end_time, app_constants.DATATYPE_SPO2)
        self.generate_alerts_for_datatype(
            start_time, end_time, app_constants.DATATYPE_HEARTRATE)
