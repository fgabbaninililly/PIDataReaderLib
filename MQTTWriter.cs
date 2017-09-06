using NLog;
using System;
using System.Collections.Concurrent;
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

		private ConcurrentDictionary<string, ConcurrentQueue<string>> messageQueuesByTopic = new ConcurrentDictionary<string, ConcurrentQueue<string>>();

		private ulong publishedMessageCount = 0;
		private ulong publishedConfirmedMessageCount = 0;

		public MQTTWriter(string brokeraddress, string brokerport, string clientname) {
			this.brokeraddress = brokeraddress;
			this.brokerport = brokerport;
			this.clientname = clientname;

			this.lastWillMessage = String.Format("Client {0} failed", this.clientname);
		}

		public string getClientName() {
			return clientname;
		}

		public void initAndConnect() {
			create();
			connect();
		}

		public bool isConnected() {
			if (null == mqttClient) {
				return false;
			}
			return mqttClient.IsConnected;
		}

		public void close() {
			if (null == mqttClient) {
				logger.Warn("Nothing to disconnect: null client!!!");
				return;
			}

			try { 
				mqttClient.ConnectionClosed -= MqttClient_ConnectionClosed;
				mqttClient.Disconnect();
				logger.Info("Client was disconnected from broker {0}", brokeraddress);
				if (null != MQTTWriter_ClientClosed) {
					MQTTClientClosedEventArgs ea = new MQTTClientClosedEventArgs();
					MQTTWriter_ClientClosed(ea);
				}
			} catch(Exception e) {
				logger.Error("Error disconnecting client");
				logger.Error("Details: {0}", e.Message);
			}
			mqttClient = null;
		}

		private void create() {
			mqttClient = new MqttClient(brokeraddress, int.Parse(brokerport), false, null, null, MqttSslProtocols.None);
			logger.Info("Created new MQTT client connecting to broker {0}", brokeraddress);

			mqttClient.MqttMsgPublished -= MqttClient_MqttMsgPublished;
			mqttClient.ConnectionClosed -= MqttClient_ConnectionClosed;

			mqttClient.MqttMsgPublished += MqttClient_MqttMsgPublished;
			mqttClient.ConnectionClosed += MqttClient_ConnectionClosed;
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
				txt = String.Format("Unable to connect to broker {0}:{1}. Please check that a broker is up and running.", brokeraddress, brokerport);
				logger.Error(txt);
				logger.Error("Details: {0}", ex.ToString());
			}
		}

		public void write(Dictionary<string, PIData> piDataMap, Dictionary<string, string> topicsMap) {
			grandTotalSwatch = Stopwatch.StartNew();

			if (!mqttClient.IsConnected) {
				logger.Info("No connection to broker detected. Attempting to reconnect.");
				close();
				initAndConnect();
			}

			foreach (string equipmentName in piDataMap.Keys) {
				try {
					PIData piData = piDataMap[equipmentName];
					string topic = topicsMap[equipmentName];
					if (!messageQueuesByTopic.ContainsKey(topic)) {
						messageQueuesByTopic[topic] = new ConcurrentQueue<string>();
					}
					write(piData, topic);
				} catch (Exception e) {
					logger.Error("Error writing data");
					logger.Error("Details: {0}", e.ToString());
				}
			}
		}

		private void write(PIData piData, string topic) {
			string mqttPayloadString = piData.writeToString(serializationType);

			if (!messageQueuesByTopic.ContainsKey(topic)) {
				logger.Error("Unable to publish: message queue for topic {0} not found!!");
				return;
			}

			ConcurrentQueue<string> messageQueue = messageQueuesByTopic[topic];
			messageQueue.Enqueue(mqttPayloadString);

			try {
				if (mqttClient.IsConnected) {
					logger.Info("Publishing {0} message(s) queued for topic {1}", messageQueue.Count, topic);
					writeQueue(messageQueue, topic);
				} else {
					logger.Info("Still no connection to broker...message was queued to be sent in the next round. Topic {0}, queue size {1}", topic, messageQueue.Count);
				}
			} catch(Exception ex) {
				Console.WriteLine("Unexpected error while publishing. Will attempt to publish later. {0} messages in queue.", messageQueue.Count);
				Console.WriteLine("Details: {0}", ex.Message);
			}
		}

		private void writeQueue(ConcurrentQueue<string> messageQueue, string topic) {
			while (messageQueue.Count > 0) {
				string msg = "";
				bool res = messageQueue.TryDequeue(out msg);
				if (!res) {
					logger.Error("Could not dequeue message!");
				} else {
					publish(msg, topic);
					logger.Info("{0} message(s) left in queue.", messageQueue.Count);
				}
			}
		}
		
		private void publish(string mqttMsg, string topic) {
			byte[] payload;
			try {
				payload = System.Text.Encoding.UTF8.GetBytes(mqttMsg);
				ulong msgId = mqttClient.Publish(topic, payload, MqttMsgBase.QOS_LEVEL_EXACTLY_ONCE, false);
				publishedBytesInSchedule = payload.Length;
				publishedMessageCount++;
				logger.Info("Message [{0}] - Published {1} bytes of data to MQTT broker ({2}). ", msgId, publishedBytesInSchedule.ToString(), topic);
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

			if (e.IsPublished) {
				publishedConfirmedMessageCount++;
			} else {
				logger.Error("Message having id {0} was not confirmed to be published", e.MessageId);
			}
			
			logger.Info("Message [{0}] - Publish complete. Metrics since startup: published = {1}, publish confirmed = {2}.", e.MessageId.ToString(), publishedMessageCount, publishedConfirmedMessageCount);
			
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
