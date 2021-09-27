import json
from decimal import Decimal
from datetime import datetime, timedelta
from re import template

from config_reader import ConfigReader
from dynamodb_util import DynamoDBUtil
import app_constants
from alerts_generator import AlertsGenerator

def main():
    start_time = (datetime.now() - timedelta(hours=8)
                     ).strftime('%Y-%m-%dT%H:%M:%SZ')
    end_time = datetime.now().strftime('%Y-%m-%dT%H:%M:%SZ')
    
    alerts_generator = AlertsGenerator()
    alerts_generator.generate_alerts_for_time_range(start_time, end_time)

if __name__ == '__main__':
    main()