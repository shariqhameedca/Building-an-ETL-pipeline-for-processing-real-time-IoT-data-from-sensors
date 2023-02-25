# Building-an-ETL-pipeline-for-processing-real-time-IoT-data-from-sensors
Built an IoT system using <b>DHT22</b> sensor, <b>ESP32, MicroPython, MySQL,</b> and <b>AWS SQS</b>. The system collected real-time temperature and humidity data, transformed it, and loaded it into a database. Used AWS SQS for messaging and MicroPython for programming the ESP32

# How the project works
First, a DHT22 sensor interfaced with esp32 mcu reads temperature and humidity data from the sensor. From esp32 each record read is sent to amazon sqs queue.
From the queue, it is read, aggregated over a minute interval and saved to a MySQL database every minute. 

## Schematics

![schematics](https://user-images.githubusercontent.com/57900267/221359725-ac442567-c5a0-4cd3-9743-e2aa00ac0426.JPG)
