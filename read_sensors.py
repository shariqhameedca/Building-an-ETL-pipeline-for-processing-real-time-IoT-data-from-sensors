import dht
from machine import Pin

def read_dht(pin_num):
    d = dht.DHT11(Pin(pin_num))
    d.measure()
    return d.temperature(), d.humidity()