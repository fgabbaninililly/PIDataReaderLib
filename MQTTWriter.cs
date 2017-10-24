using NLog;
using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Diagnostics;
using System.Net;
using uPLibrary.Networking.M2Mqtt;
using uPLibrary.Networking.M2Mqtt.Messages;

namespace PIDataReaderLib {
	public class MQTTWriter : AbstractMQTTWriter {

		private static Logger logger = LogManager.GetCurrentClassLogger();
		Stopwatch grandTotalSwatch;
		private ConcurrentDictionary<string, ConcurrentQueue<string>> messageQueuesByTopic = new ConcurrentDictionary<string, ConcurrentQueue<string>>();

		private MqttClient mqttClient;

		internal MQTTWriter(string brokeraddress, int brokerport, string clientname) {
			this.brokeraddress = brokeraddress;
			this.brokerport = brokerport;
			this.clientname = clientname;

			this.lastWillMessage = String.Format("Client {0} failed", this.clientname);

			publishedBytesPerWrite = 0;
			publishedMessagesPerWrite = 0;
			publishedMessageCount = 0;
			publishedConfirmedMessageCount = 0;
		}

		public override string getClientName() {
			return clientname;
		}

		public override void initAndConnect() {
			create();
			connect();
		}

		public override bool isConnected() {
			if (null == mqttClient) {
				return false;
			}
			return mqttClient.IsConnected;
		}

		public override void close() {
			if (null == mqttClient) {
				logger.Warn("Nothing to disconnect: null client!!!");
				return;
			}

			try { 
				mqttClient.ConnectionClosed -= MqttClient_ConnectionClosed;
				mqttClient.Disconnect();
				logger.Info("Client was disconnected from broker {0}", brokeraddress);
				base.raiseWriterClosed();
			} catch(Exception e) {
				logger.Error("Error disconnecting client");
				logger.Error("Details: {0}", e.Message);
			}
			mqttClient = null;
		}

		private void create() {
			try { 
				mqttClient = new MqttClient(brokeraddress, brokerport, false, null, null, MqttSslProtocols.None);
				logger.Info("Created new M2MQTT client connecting to broker {0}", brokeraddress);

				mqttClient.MqttMsgPublished -= MqttClient_MqttMsgPublished;
				mqttClient.ConnectionClosed -= MqttClient_ConnectionClosed;

				mqttClient.MqttMsgPublished += MqttClient_MqttMsgPublished;
				mqttClient.ConnectionClosed += MqttClient_ConnectionClosed;
			} catch (Exception e) {
				logger.Fatal("Cannot connect to broker: {0}:{1}. Please check that connection parameters are correct.", brokeraddress, brokerport);
			}
		}

		private void connect() {
			string txt;
			try {
				logger.Info("Attempting connection to broker. Client name is '{0}'", clientname);
				byte b = mqttClient.Connect(clientname, "", "", false, MqttMsgBase.QOS_LEVEL_EXACTLY_ONCE, true, MQTTWriterParams.MQTT_LASTWILL_TOPIC, lastWillMessage, false, 1000); 
				if (0 == b) {
					txt = "Connected to broker";
				} else {
					txt = "Connection to broker failed: please check IP address and username/password";
				}
				logger.Info(txt);
			} catch(Exception ex) {
				logger.Fatal("Unable to connect to broker {0}:{1}. Please check that a broker is up and running.", brokeraddress, brokerport);
				logger.Fatal("Details: {0}", ex.ToString());
			}
		}

		public override void write(PIData piData, string equipmentName, Dictionary<string, string> topicsMap) {
			grandTotalSwatch = Stopwatch.StartNew();
			publishedBytesPerWrite = 0;
			publishedMessagesPerWrite = 0;

			if (!mqttClient.IsConnected) {
				logger.Error("No connection to broker detected. Attempting to reconnect.");
				close();
				initAndConnect();
			}
						
			try {
				string topic = topicsMap[equipmentName];
				if (!messageQueuesByTopic.ContainsKey(topic)) {
					messageQueuesByTopic[topic] = new ConcurrentQueue<string>();
				}
				write(piData, topic);
			} catch (Exception e) {
				logger.Error("Error writing data");
				logger.Error("Details: {0}", e.ToString());
			}
			
			grandTotalSwatch.Stop();
			double grandTotalTimeSec = grandTotalSwatch.Elapsed.TotalSeconds;
			base.raisePublishCompleted(publishedMessagesPerWrite, publishedBytesPerWrite, grandTotalTimeSec);
		}

		private void write(PIData piData, string topic) {
			string mqttPayloadString = piData.writeToString(MQTTWriterParams.SERIALIZATION_TYPE);

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
				logger.Info("Unexpected error while publishing. Will attempt to publish later. {0} messages in queue.", messageQueue.Count);
				logger.Info("Details: {0}", ex.Message);
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
				publishedBytesPerWrite += (ulong)payload.Length;
				publishedMessagesPerWrite++;
				logger.Info("Message [{0}] - Published {1} bytes of data to MQTT broker. Topic: {2}.", msgId, payload.Length, topic);
			} catch (Exception ex) {
				logger.Error("Error publishing MQTT message: {0}", ex.Message);
			}
		}

		private void MqttClient_ConnectionClosed(object sender, System.EventArgs e) {
			logger.Warn("Connection to broker was closed");
		}

		private void MqttClient_MqttMsgPublished(object sender, MqttMsgPublishedEventArgs e) {
			if (e.IsPublished) {
				publishedConfirmedMessageCount++;
			} else {
				logger.Error("Message having id {0} was not confirmed to be published", e.MessageId);
			}
			logger.Info("Message [{0}] - Publish complete. Metrics since startup: published = {1}, publish confirmed = {2}.", e.MessageId.ToString(), publishedMessageCount, publishedConfirmedMessageCount);
		}

	}

}
