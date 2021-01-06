#!/usr/bin/env python3
import paho.mqtt.client as mqtt
from smbus2 import SMBus, i2c_msg
from time import sleep
import json
import time
import logging
import struct
import requests

clientName="Poolpi"
username=""
password=""
broker="127.0.0.1"
port=1883
baseTopic = "pool"
address = 0x04
config_in = {
    "heater": {
        "type": "digitalOut",
        "gpio": 41,
        "qos": 0
    },
    "relay7": {
        "type": "digitalOut",
        "gpio": 40,
        "qos": 0
    },
    "relay6": {
        "type": "digitalOut",
        "gpio": 39,
        "qos": 0
    },
    "valveSolar" : {
        "type": "digitalOut",
        "gpio": 38,
        "qos": 0
    },
    "valveCleaner" : {
        "type": "digitalOut",
        "gpio": 37,
        "qos": 0
    },
    "valveFountain" : {
        "type": "digitalOut",
        "gpio": 36,
        "qos": 0
    },
    "valveSpaJets" : {
        "type": "digitalOut",
        "gpio": 35,
        "qos": 0
    },
    "valveSpaDrain" : {
        "type": "digitalOut",
        "gpio": 34,
        "qos": 0
    },
    "waterfall" : {
        "type": "digitalOut",
        "gpio": 42,
        "qos": 0
    },
    "blower" : {
        "type": "digitalOut",
        "gpio": 43,
        "qos": 0
    },
    "ozone" : {
        "type": "digitalOut",
        "gpio": 44,
        "qos": 0
    },
    "lightTransformer" : {
        "type": "digitalOut",
        "gpio": 50,
        "qos": 0
    },
    "lightEast" : {
        "type": "intellibrite",
        "gpio": 53,
        "qos": 0
    },
    "lightWest" : {
        "type": "intellibrite",
        "gpio": 52,
        "qos": 0
    },
    "lightSpa" : {
        "type": "intellibrite",
        "gpio": 51,
        "qos": 0
    },
    "inTemp_F" : {
        "type": "thermistor",
        "gpio": 100,
        "qos": 0
    },
    "outTemp_F" : {
        "type": "thermistor",
        "gpio": 101,
        "qos": 0
    }
}


def on_log(client, userdata, level, buf):
        print("log: "+buf)

def on_connect(client, userdata, flags, rc):
    m="Connected flags"+str(flags)+"result code "\
    +str(rc)+"client_id  "+str(client)
    logging.info(m)

def on_disconnect(client, userdata, flags, rc=0):
    logging.info("disconnecting reason  "+str(rc))
    client.loop_stop()

def on_subscribe(client,userdata,mid,granted_qos):
    logging.info("in on subscribe callback result "+str(mid))

def on_publish(client, userdata, mid):
    logging.info("pub ack "+ str(mid))

def on_message(client, userdata, msg):
    topic=msg.topic
    msg_decode=str(msg.payload.decode("utf-8","ignore"))
    print("message received: ",topic," ",msg_decode)

def on_message_digitalOut(client, userdata, msg):
    topic=msg.topic
    gpio=-1
    print("on_message_digitalOut topic:",topic)
    for t in config_in:
        if config_in[t]["subTopic"] == topic:
#            print("match",t[0])
            gpio=config_in[t]["gpio"]
            pubTopic=config_in[t]["pubTopic"]
#            print("Found GPIO:",gpio)
    msg_decode=str(msg.payload.decode("utf-8","ignore")).lower()
    try:
        m_in=json.loads(msg_decode)
        print("message received for digitalOut: ",topic," gpio: ",gpio," data: ",m_in["state"])

        with SMBus(1) as bus:
            if m_in["state"] == "on":
                print("State = ON")
                bus.write_byte_data(address, gpio, 1)
            elif m_in["state"] == "off":
                print("State = OFF")
                bus.write_byte_data(address, gpio, 0)
            sleep (.2)
            bus_read = bus.read_byte_data(address, gpio)
            print(bus_read)
        publish_i2c(pubTopic, bus_read)

    except ValueError:
        print("Decoding JSON has failed: Error:",ValueError)


def on_message_intellibrite(client, userdata, msg):
    topic=msg.topic
    gpio=-1
    print("on_message_digitalOut topic:",topic)
    for t in config_in:
        if config_in[t]["subTopic"] == topic:
#            print("match",t[0])
            gpio=config_in[t]["gpio"]
            pubTopic=config_in[t]["pubTopic"]
#            print("Found GPIO:",gpio)
    msg_decode=str(msg.payload.decode("utf-8","ignore")).lower()

    try:
        m_in=json.loads(msg_decode)

 #   print("message received for digitalOut: ",topic," gpio: ",gpio," data: ",m_in["state"])
        with SMBus(1) as bus:
            if "state" in m_in.keys():
                if m_in["state"] == "off":
                    print("State = OFF")
                    bus.write_byte_data(address, gpio, 0)
                    sleep (.2)
                    bus_read = bus.read_byte_data(address, gpio)
                    print(bus_read)
                    publish_i2c(pubTopic, bus_read)
                elif m_in["state"] == "on":
                    print("State = ON")
                    bus.write_byte_data(address, gpio, 1)
                    sleep (.2)
                    bus_read = bus.read_byte_data(address, gpio)
                    print(bus_read)
                    publish_i2c(pubTopic, bus_read)
                    if "mode" in m_in.keys():
                        intellibrite_mode(m_in, gpio, pubTopic)
                else:
                    print("State set but neither on nor off")
            elif "mode" in m_in.keys():
                intellibrite_mode(m_in, gpio, pubTopic)
            else:
                print ("Neither state nor mode set!")
    except ValueError:
        print("Decoding JSON has failed: Error:",ValueError)

def intellibrite_mode(m_in, gpio, pubTopic):
    with SMBus(1) as bus:
        if m_in["mode"] == "sam":
            print("Mode = SAm")
            bus.write_byte_data(address, gpio, 2)
        elif m_in["mode"] == "party":
            print("Mode = Party")
            bus.write_byte_data(address, gpio, 3)
        elif m_in["mode"] == "romance":
            print("Mode = Romance")
            bus.write_byte_data(address, gpio, 4)
        elif m_in["mode"] == "caribbean":
            print("Mode = Caribbean")
            bus.write_byte_data(address, gpio, 5)
        elif m_in["mode"] == "american":
            print("Mode = American")
            bus.write_byte_data(address, gpio, 6)
        elif m_in["mode"] == "sunset":
            print("Mode = Sunset")
            bus.write_byte_data(address, gpio, 7)
        elif m_in["mode"] == "royal":
            print("Mode = Royal")
            bus.write_byte_data(address, gpio, 8)
        elif m_in["mode"] == "blue":
            print("Mode = Blue")
            bus.write_byte_data(address, gpio, 9)
        elif m_in["mode"] == "green":
            print("Mode = Green")
            bus.write_byte_data(address, gpio, 10)
        elif m_in["mode"] == "red":
            print("Mode = Red")
            bus.write_byte_data(address, gpio, 11)
        elif m_in["mode"] == "white":
            print("Mode = White")
            bus.write_byte_data(address, gpio, 12)
        elif m_in["mode"] == "magenta":
            print("Mode = Magenta")
            bus.write_byte_data(address, gpio, 13)

        sleep (.2)
        bus_read = bus.read_byte_data(address, gpio)
        publish_i2c(pubTopic, bus_read)


def publish_i2c(pubTopic, bus_read):
    if bus_read == 0:
        client.publish(pubTopic,'{"state": "off"}',retain=True)
    elif bus_read == 1:
        client.publish(pubTopic,'{"state": "on"}',retain=True) 
    elif bus_read == 2:
        client.publish(pubTopic,'{"state": "on", "mode": "sam"}',retain=True) 
    elif bus_read == 3:
        client.publish(pubTopic,'{"state": "on", "mode": "party"}',retain=True) 
    elif bus_read == 4:
        client.publish(pubTopic,'{"state": "on", "mode": "romance"}',retain=True) 
    elif bus_read == 5:
        client.publish(pubTopic,'{"state": "on", "mode": "caribbean"}',retain=True) 
    elif bus_read == 6:
        client.publish(pubTopic,'{"state": "on", "mode": "american"}',retain=True) 
    elif bus_read == 7:
        client.publish(pubTopic,'{"state": "on", "mode": "sunset"}',retain=True) 
    elif bus_read == 8:
        client.publish(pubTopic,'{"state": "on", "mode": "royal"}',retain=True) 
    elif bus_read == 9:
        client.publish(pubTopic,'{"state": "on", "mode": "blue"}',retain=True) 
    elif bus_read == 10:
        client.publish(pubTopic,'{"state": "on", "mode": "green"}',retain=True) 
    elif bus_read == 11:
        client.publish(pubTopic,'{"state": "on", "mode": "red"}',retain=True) 
    elif bus_read == 12:
        client.publish(pubTopic,'{"state": "on", "mode": "white"}',retain=True) 
    elif bus_read == 13:
        client.publish(pubTopic,'{"state": "on", "mode": "magenta"}',retain=True) 

#create the subscription topics
for x in config_in:
    config_in[x]["subTopic"]=baseTopic+"/"+x+"/set"
    config_in[x]["pubTopic"]=baseTopic+"/"+x
#    print(config_in[x]["subTopic"])


client= mqtt.Client(clientName)
client.on_connect= on_connect        #attach function to callback
client.on_message=on_message        #attach function to callback
client.on_disconnect=on_disconnect
client.on_subscribe=on_subscribe
client.on_publish=on_publish
client.on_log=on_log
if username !="":
    client.username_pw_set(username, password)




def main():

    print("Connecting to broker ",broker)
    client.connect(broker,port)
    
    for i in config_in:
        logging.info("subscribing  " + str(config_in[i]["subTopic"]))
        client.subscribe(config_in[i]["subTopic"],config_in[i]["qos"])
        if config_in[i]["type"]=="digitalOut":
            logging.info("callback_add digitalOut" + str(config_in[i]["gpio"]))
            client.message_callback_add(config_in[i]["subTopic"], on_message_digitalOut)
        elif config_in[i]["type"]=="intellibrite":
            logging.info("callback_add intellibrite" + str(config_in[i]["gpio"]))
            client.message_callback_add(config_in[i]["subTopic"], on_message_intellibrite)

    try:
        client.loop_start()

        while True:
            with SMBus(1) as bus:

                for i in config_in:
                    if config_in[i]["type"]=="thermistor":

                        # I2C can only send bytes.  Arduino stores floats as 4 byte IEEE-754 Floating Point so lets read that.  
                        # When we read request the thermistor from the Arduino, it sends the 4 byte pointer, which we read into a list. 
                        block = bus.read_i2c_block_data(address, config_in[i]["gpio"], 4)
                        # bytearray(block) converts the list "block" into a 4 byte bytearray.
                        # struct.unpack('f', ) converts the IEEE 754 bytearray into a tupple.  The first value is the float in decimal.
                        # str(round( )) rounds the tupple and converts to a string to send.
                        temp_f = str(round(struct.unpack('f',bytearray(block))[0], 2))
                        client.publish(config_in[i]["pubTopic"],temp_f,retain=True)
                        if config_in[i]["gpio"]==100:
                            url = 'http://127.0.0.1:4200/state/temps/'
                            round_temp_f = int(float(temp_f))
                            print(round_temp_f)
                            data = {"waterSensor1": round_temp_f}
                            print(json.dumps(data))
                            headers = {"Content-Type": "application/json"}
                            response = requests.put(url, data=json.dumps(data), headers=headers)
                            try:
                                res = response.json()
                            except:
                                print("http put exception, sleeping 60 and trying again")
                                sleep(60)
                                continue
                        sleep (.2)
                sleep(15)
    except: 
        print("Exception")
        client.loop_stop()
        client.disconnect()
        main()




if __name__ == '__main__':
    try:
        main()
    except KeyboardInterrupt:
        print ('Interrupted')
#        gpio.cleanup()
        sys.exit(0)
