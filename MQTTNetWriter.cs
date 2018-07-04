using MQTTnet;
using MQTTnet.Core;
using MQTTnet.Core.Client;
using MQTTnet.Core.Protocol;
using NLog;
using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Runtime.CompilerServices;
using System.Text;
using System.Threading.Tasks;

namespace PIDataReaderLib {
	class MQTTNetWriter : AbstractMQTTWriter {

		private static Logger logger = LogManager.GetCurrentClassLogger();
		Stopwatch grandTotalSwatch;
		private ConcurrentDictionary<string, ConcurrentQueue<string>> messageQueuesByTopic = new ConcurrentDictionary<string, ConcurrentQueue<string>>();

		private MqttClientTcpOptions options;
		private IMqttClient mqttClient;
		
		private bool isClientConnectedVar;
		public bool isClientConnected {
			[MethodImpl(MethodImplOptions.Synchronized)]
			get { return isClientConnectedVar; }
			[MethodImpl(MethodImplOptions.Synchronized)]
			set { isClientConnectedVar = value; }
		}

		private bool attemptReconnectVar;
		public bool attemptReconnect {
			[MethodImpl(MethodImplOptions.Synchronized)]
			get { return attemptReconnectVar; }
			[MethodImpl(MethodImplOptions.Synchronized)]
			set { attemptReconnectVar = value; }
		}

		internal MQTTNetWriter(string brokeraddress, int brokerport, string clientname, ushort keepAliveSec) {
			this.brokeraddress = brokeraddress;
			this.brokerport = brokerport;
			this.clientname = clientname;
			this.keepAliveSec = keepAliveSec;

			this.lastWillMessage = String.Format("Client {0} failed", this.clientname);
			
			options = new MqttClientTcpOptions();
			options.CleanSession = false;
			options.ClientId = clientname;
			options.KeepAlivePeriod = TimeSpan.FromSeconds(keepAliveSec);
			options.Server = brokeraddress;
			options.Port = brokerport;

			MqttApplicationMessage lastWillMessage = new MqttApplicationMessage(MQTTWriterParams.MQTT_LASTWILL_TOPIC, Encoding.UTF8.GetBytes(this.lastWillMessage), MqttQualityOfServiceLevel.ExactlyOnce, false);
			options.WillMessage = lastWillMessage;
			
			attemptReconnect = true;

			publishedBytesPerWrite = 0;
			publishedConfirmedMessageCount = 0;
			publishedMessageCount = 0;
		}

		public override void initAndConnect() {
			create();
			try {
				Task tsk = connectAsync();
				tsk.Wait();
			} catch (Exception ex) {
				attemptReconnect = false;
				logger.Fatal("Error connecting to broker. Details: {0}", ex.Message);
				return;
			}
		}

		public override void close() {
			disconnect(false);
		}

		public override string getClientName() {
			return clientname;
		}
				
		public override bool isConnected() {
			return isClientConnected;
		}

		public override void write(PIData piData, string equipmentName, Dictionary<string, string> topicsMap) {
			grandTotalSwatch = Stopwatch.StartNew();
			publishedBytesPerWrite = 0;
			publishedMessagesPerWrite = 0;
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
				logger.Info("Publishing {0} message(s) queued for topic {1}", messageQueue.Count, topic);
				writeQueue(messageQueue, topic);
			} catch (Exception ex) {
				logger.Info("Unexpected error while publishing. Will attempt to publish later. {0} messages in queue.", messageQueue.Count);
				logger.Info("Details: {0}", ex.Message);
			}
		}

		private void writeQueue(ConcurrentQueue<string> messageQueue, string topic) {
			string msg = null;
			while (!messageQueue.IsEmpty) {
				if (!isClientConnected) {
					break;
				}
				if (messageQueue.TryPeek(out msg)) {
					Task t = publishSingleMessageAsync(messageQueue, msg, topic);
					t.Wait();
				} else {
					logger.Error("Could not peek message from queue. Topic: {0}.", topic);
				}
			}
		}

		private async Task publishSingleMessageAsync(ConcurrentQueue<string> messageQueue, string message, string topic) {
			byte[] payload = Encoding.UTF8.GetBytes(message);
			MqttApplicationMessage applicationMessage = new MqttApplicationMessage(topic, payload, MqttQualityOfServiceLevel.ExactlyOnce, false);
			try {
				await mqttClient.PublishAsync(applicationMessage);
				
				logger.Info("Published {0} bytes of data to MQTT broker. Topic: {1}.", payload.Length, topic);
				publishedMessagesPerWrite++;
				publishedConfirmedMessageCount++;
				publishedMessageCount++;
				publishedBytesPerWrite += (ulong)payload.Length;
				if (!messageQueue.TryDequeue(out message)) {
					logger.Error("Could not dequeue message from queue. Topic: {0}.", topic);
				} else {
					logger.Info("{0} message(s) left in queue. Topic: {1}", messageQueue.Count, topic);
				}
			} catch (Exception e) {
				logger.Error(String.Format("Failed publishing message '{0}'. Message queued to be sent later.", message.Substring(0, 150) + "[...]"));
				logger.Error("Details: {0}", e.Message);
			}
		}

		private void create() {
			mqttClient = new MqttClientFactory().CreateMqttClient();
			mqttClient.Connected += MqttClient_Connected;
			
			mqttClient.Disconnected += async (s, e) => {
				isClientConnected = false;
				logger.Warn("Connection to broker was closed");
				if (!attemptReconnect) {
					return;
				}

				await Task.Delay(TimeSpan.FromSeconds(5));

				try {
					await connectAsync();
				} catch {
					logger.Warn("Reconnection to broker failed");
				}
			};
		}

		private async Task connectAsync() {
			if (isClientConnected) {
				return;
			}

			logger.Info("Attempting connection to broker. Client name is '{0}'", clientname);
			await mqttClient.ConnectAsync(options);
			logger.Info("Connected to broker");
			
			/*
			try {
				await mqttClient.ConnectAsync(options);
				logger.Info("Connected to broker");
			} catch (Exception ex) {
				logger.Fatal("Unable to connect to broker {0}:{1}. Please check that a broker is up and running.", brokeraddress, brokerport);
				logger.Fatal("Details: {0}", ex.Message);
			}
			*/
		}

		public void disconnect(bool attemptRec) {
			attemptReconnect = attemptRec;
			try {
				Task t = mqttClient.DisconnectAsync();
				t.Wait();
			} catch (Exception e) {
				logger.Error("Error disconnecting from broker: " + e.Message);
			}
		}

		private void MqttClient_Connected(object sender, EventArgs e) {
			isClientConnected = true;
			logger.Info("Client connected to broker");
		}

	}
}
