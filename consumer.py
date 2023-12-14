import matplotlib.pyplot as plt
from kafka import KafkaConsumer

# Configure Kafka consumer
consumer = KafkaConsumer('test', bootstrap_servers='localhost:9092')

# Initialize lists to store closing prices and moving averages
closing_prices = []
moving_averages_7days = []
moving_averages_30days = []

# Create a figure and axes for the plot
fig, ax = plt.subplots()

# Continuously consume messages from Kafka
for message in consumer:
    # Decode the message value from bytes to string
    csv_row_string = message.value.decode('utf-8')
    
    # Print the CSV row
    print(csv_row_string)

    # Extract the closing price from the CSV row
    closing_price = float(csv_row_string.split(',')[7])  

    # Append the closing price to the list
    closing_prices.append(closing_price)

    # Calculate the moving averages
    if len(closing_prices) >= 7:
        # Shifting 7 days moving average
        moving_average_7days = sum(closing_prices[-7:]) / 7
        moving_averages_7days.append(moving_average_7days)

    if len(closing_prices) >= 30:
        # Shifting 30 days moving average
        moving_average_30days = sum(closing_prices[-30:]) / 30
        moving_averages_30days.append(moving_average_30days)

    # Clear the current plot
    ax.clear()

    # Plot the closing prices, 7 days moving average, and 30 days moving average
    ax.plot(closing_prices, label='Closing Price')
    ax.plot(moving_averages_7days, label='7 Days Moving Average')
    ax.plot(moving_averages_30days, label='30 Days Moving Average')

    # Set labels and title
    ax.set_xlabel('Time (in Days)')
    ax.set_ylabel('Price (in Rupees)')
    ax.set_title('Moving Averages')

    # Add legend
    ax.legend()

    # Draw the updated plot
    plt.draw()
    plt.pause(0.01)

# Close the Kafka consumer
consumer.close()
