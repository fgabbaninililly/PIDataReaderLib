using NLog;
using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Net;
using uPLibrary.Networking.M2Mqtt;
using uPLibrary.Networking.M2Mqtt.Messages;

namespace PIDataReaderLib {
	public class MQTTWriter {

		private const string MQTT_LASTWILL_TOPIC = "/pireaderlastwill";
		
		private string brokeraddress;
		private string brokerport;
		private string clientname;

		private string lastWillMessage;

		private MqttClient mqttClient;
		Stopwatch grandTotalSwatch;
		long publishedBytesInSchedule;
		
		string serializationType = "xml";
		
		private static Logger logger = LogManager.GetCurrentClassLogger();

		public delegate void MQTTPublishTerminated(MQTTPublishTerminatedEventArgs e);
		public event MQTTPublishTerminated MQTTWriter_PublishCompleted;

		public delegate void MQTTClientClosed(MQTTClientClosedEventArgs e);
		public event MQTTClientClosed MQTTWriter_ClientClosed;

		public MQTTWriter(string brokeraddress, string brokerport, string clientname) {
			this.brokeraddress = brokeraddress;
			this.brokerport = brokerport;
			this.clientname = clientname;

			this.lastWillMessage = String.Format("Client {0} failed", this.clientname);
		}

		public void initAndConnect() {
			mqttClient = new MqttClient(brokeraddress, int.Parse(brokerport), false, null, null, MqttSslProtocols.None);
			logger.Info("Created new MQTT client connecting to broker {0}", brokeraddress);
			mqttClient.MqttMsgPublished += MqttClient_MqttMsgPublished;
			mqttClient.ConnectionClosed += MqttClient_ConnectionClosed;
			connect();
		}

		public void close() {
			mqttClient.ConnectionClosed -= MqttClient_ConnectionClosed;
			mqttClient.Disconnect();
			logger.Info("Client was disconnected from broker {0}", brokeraddress);

			if (null != MQTTWriter_ClientClosed) {
				MQTTClientClosedEventArgs ea = new MQTTClientClosedEventArgs();
				MQTTWriter_ClientClosed(ea);
			}
		}

		private void connect() {
			string txt;
			try {
				logger.Info("Attempting connection to broker. Client name is '{0}'", clientname);
				byte b = mqttClient.Connect(clientname, "", "", false, MqttMsgBase.QOS_LEVEL_EXACTLY_ONCE, true, MQTT_LASTWILL_TOPIC, lastWillMessage, false, 1000); 
				if (0 == b) {
					txt = "Connected to broker";
				} else {
					txt = "Connection to broker failed: please check IP address and username/password";
				}
				logger.Info(txt);
			} catch(Exception ex) {
				txt = "Unable to connect to broker. Please check that a broker is up and running";
				logger.Error(txt);
				logger.Error("Details: {0}", ex.ToString());
				throw ex;
			}
		}

		public void write(Dictionary<string, PIData> piDataMap, Dictionary<string, string> topicsMap) {
			grandTotalSwatch = Stopwatch.StartNew();
			foreach (string equipmentName in piDataMap.Keys) {
				try {
					PIData piData = piDataMap[equipmentName];
					string topic = topicsMap[equipmentName];
					write(piData, topic);
				} catch (Exception e) {
					logger.Error(e.ToString());
				}
			}
		}

		private void write(PIData piData, string topic) {
			Stopwatch swatch = Stopwatch.StartNew();
			string mqttPayloadString = piData.writeToString(serializationType);
			swatch.Stop();
			logger.Debug("Time required for serializing data ({0}): {1}s", serializationType, swatch.Elapsed.TotalSeconds.ToString());

			if (!mqttClient.IsConnected) {
				logger.Error("Cannot publish data: client is not connected to MQTT broker.");
				logger.Error("Trying to reconnect...");
				connect();
			}
			if (mqttClient.IsConnected) {
				publish(mqttPayloadString, topic);
			}
		}
		
		private void publish(string mqttMsg, string topic) {
			byte[] payload;
			try {
				payload = System.Text.Encoding.UTF8.GetBytes(mqttMsg);
				ulong msgId = mqttClient.Publish(topic, payload, MqttMsgBase.QOS_LEVEL_EXACTLY_ONCE, false);
				publishedBytesInSchedule = payload.Length;
				logger.Info("Message [{0}] - Published {1} bytes of data to MQTT broker ({2})", msgId, publishedBytesInSchedule.ToString(), topic);
			} catch (Exception ex) {
				logger.Error("Error publishing MQTT message: {0}", ex.Message);
			}
		}

		private void MqttClient_ConnectionClosed(object sender, System.EventArgs e) {
			logger.Info("Connection to broker was closed");
		}

		private void MqttClient_MqttMsgPublished(object sender, MqttMsgPublishedEventArgs e) {
			grandTotalSwatch.Stop();
			double grandTotalTimeSec = grandTotalSwatch.Elapsed.TotalSeconds;
			logger.Info("Message [{0}] - Publish complete", e.MessageId.ToString());
			
			double thrput = 0;
			if (0 != grandTotalTimeSec) {
				thrput = publishedBytesInSchedule / grandTotalTimeSec;
			}
			
			if (null != MQTTWriter_PublishCompleted) {
				MQTTPublishTerminatedEventArgs ea = new MQTTPublishTerminatedEventArgs(grandTotalTimeSec, thrput, e.MessageId);
				MQTTWriter_PublishCompleted(ea);
			}
		}
	}

	public class MQTTPublishTerminatedEventArgs : EventArgs {
		public MQTTPublishTerminatedEventArgs(double elapsedTimeSec, double throughput, ushort messageId) {
			this.elapsedTimeSec = elapsedTimeSec;
			this.throughput = throughput;
			this.messageId = messageId;
		}
		public double elapsedTimeSec;
		public double throughput;
		public ushort messageId;
	}

	public class MQTTClientClosedEventArgs : EventArgs {
		
	}
	
}
