from kafka import KafkaProducer
import csv

# Configure Kafka producer
producer = KafkaProducer(bootstrap_servers='localhost:9092')

# Define the topic to which the messages will be sent
topic = 'test'

# Path to the CSV file
csv_file_path = 'D:\GAIL.csv'

# Read the CSV file and send each row as a message to Kafka
with open(csv_file_path, 'r') as file:
    reader = csv.reader(file)
    header = next(reader)  # Assuming the first row is the header row
    for row in reader:
        # Convert the CSV row to a string
        csv_row_string = ','.join(row)

        # Produce the CSV row as a message to Kafka
        producer.send(topic, value=csv_row_string.encode('utf-8'))
        print("Message Sent")

# Flush and close the Kafka producer
producer.flush()
producer.close()



    

