from datetime import datetime, timedelta
from aggregate_data_generator import AggregateDataGenerator

def main():
    start_time = (datetime.now() - timedelta(hours=10)
                  ).strftime('%Y-%m-%dT%H:%M:%SZ')
    end_time = datetime.now().strftime('%Y-%m-%dT%H:%M:%SZ')

    aggregate_data_generator = AggregateDataGenerator()
    aggregate_data_generator.generate_aggregate_data(start_time, end_time)


if __name__ == '__main__':
    main()
