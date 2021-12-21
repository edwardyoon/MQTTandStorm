# MQTTandStorm
This shows how to handles Streaming IoT Data with MQTT and Apache Storm.

# Getting started

To build the project, you can run following command:

```
$ mvn clean install package dependency:copy-dependencies
```

.

Once project is compiled, copy org.eclipse.paho.client.mqttv3-1.2.0.jar to $STORM_HOME/extlib folder. And then, you can submit your own topology to the Apache Storm cluster with following command:

```
$ cp lib/org.eclipse.paho.client.mqttv3-1.2.0.jar ~/{$STORM_HOME}/extlib
$ storm jar target/stormapp-1.0-SNAPSHOT.jar com.dolbomdream.MyTopology
```

By passing message through MQTT, you will see the received message on MQTTSpout worker logs.

```
2021-12-21 15:50:46.682 c.d.MQTTSpout MQTT Call: paho146468579208986 [INFO] Logging tuple with logger: test, hello from mqtt
```
