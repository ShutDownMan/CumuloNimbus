"""
    This is the main file of the Agrometeo Simulator.
    It is responsible for generating random data and sending it to a broker.
"""
from perlin_noise import PerlinNoise
import paho.mqtt.client as mqtt
import os
import threading

broker_host = os.getenv('BROKER_HOST', 'localhost')
broker_port = int(os.getenv('BROKER_PORT', 1883))

interval_seconds = 60
station_ids = range(1, 6)
sensor_ids = range(1, 6)
magnitude_ids = range(1, 3)

def generate_measurements(noises):
    """
        Generates random data and sends it to a broker.

        Args:
            noises (dict): A dictionary containing noise for each station/sensor/magnitude combination.

        Returns:
            data (dict): A dictionary containing generated data.
    """
    data = {}
    for station_id in station_ids:
        for sensor_id in sensor_ids:
            for magnitude_id in magnitude_ids:
                noise = noises[(station_id, sensor_id, magnitude_id)]
                data[(station_id, sensor_id, magnitude_id)] = noise([station_id, sensor_id, magnitude_id]) * 100

    print(data)
    return data


def main():
    """
    Generate noise for each station/sensor/magnitude combination and generate random data.

    Returns:
        None
    """
    # connect to a broker
    client = mqtt.Client(client_id="agrometeo-simulator-py", clean_session=True, userdata=None, protocol=mqtt.MQTTv311, transport="tcp")
    client.connect(broker_host, broker_port, 60)
    client.loop_start()

    # generate noise for each station/sensor/magnitude combination
    noises = {}
    for station_id in station_ids:
        for sensor_id in sensor_ids:
            for magnitude_id in magnitude_ids:
                noises[(station_id, sensor_id, magnitude_id)] = PerlinNoise(octaves=2.7, seed=station_id+sensor_id*10+magnitude_id*100)

    # generate random data
    def generate_data():
        print("Generating data...")
        generated_measurements = generate_measurements(noises)

        # send data to a broker
        print("Sending data to a broker...")
        for key, value in generated_measurements.items():
            # print(f"agrometeo/stations/{key[0]}/{key[1]}/{key[2]}: {value}")
            client.publish(f"agrometeo/stations/{key[0]}/{key[1]}/{key[2]}", value)

        print("Data sent to broker.")

        threading.Timer(interval_seconds, generate_data).start()

    generate_data()

if __name__ == '__main__':
    main()
