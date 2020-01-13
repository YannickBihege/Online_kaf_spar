import findspark
findspark.init('/home/yannick/dev/spark-2.4.4-bin-hadoop2.7')

import producer_server


def run_kafka_server():
     
	# TODO get the json file path
    #input_file = ""
    
    # read the data file
    input_file = './police-department-calls-for-service.json'
    df = spark.read.json(input_file)
    
    df.show(10, False)

    # TODO fill in blanks
    producer = producer_server.ProducerServer(
        input_file=input_file,
        topic="police.calls",
        bootstrap_servers="localhost:9092",
        client_id="1"
    )

    return producer


def feed():
    producer = run_kafka_server()
    producer.generate_data()


if __name__ == "__main__":
    feed()

