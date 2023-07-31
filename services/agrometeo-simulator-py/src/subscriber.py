import paho.mqtt.client as mqtt
import os

broker_host = os.getenv('BROKER_HOST', 'localhost')
broker_port = int(os.getenv('BROKER_PORT', 1883))

def on_message(client, userdata, message):
    print(f"Received message: {message.payload.decode()}")

# Create a MQTT client
client = mqtt.Client()

# Set up the callback function for when a message is received
client.on_message = on_message

# Connect to the MQTT broker (RabbitMQ)
client.connect(broker_host, broker_port, 60)

# Subscribe to the topic of interest
topic = "agrometeo/stations/+/+/+"
client.subscribe(topic)

# Start the MQTT loop to receive messages
client.loop_forever()