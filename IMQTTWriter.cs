using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace PIDataReaderLib {
	internal class MQTTWriterParams {
		internal static readonly string MQTT_LASTWILL_TOPIC = "/pireaderlastwill";
		internal static readonly string SERIALIZATION_TYPE = "xml";
	}

	public class MQTTWriterFactory {
		public static AbstractMQTTWriter createM2MQTT(string brokeraddress, string brokerport, string clientname) {
			int brokerportInt = Int32.Parse(brokerport);
			return new MQTTWriter(brokeraddress, brokerportInt, clientname);
		}

		public static AbstractMQTTWriter createMQTTNet(string brokeraddress, string brokerport, string clientname) {
			int brokerportInt = Int32.Parse(brokerport);
			return new MQTTNetWriter(brokeraddress, brokerportInt, clientname);
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

	public interface IMQTTWriter {
		bool isConnected();
		string getClientName();
		void initAndConnect();
		void close();
		void write(Dictionary<string, PIData> piDataMap, Dictionary<string, string> topicsMap);
	}

	public abstract class AbstractMQTTWriter : IMQTTWriter {
		public delegate void MQTTPublishTerminated(MQTTPublishTerminatedEventArgs e);
		public event MQTTPublishTerminated MQTTWriter_PublishCompleted;

		public delegate void MQTTClientClosed(MQTTClientClosedEventArgs e);
		public event MQTTClientClosed MQTTWriter_ClientClosed;

		public abstract bool isConnected();
		public abstract string getClientName();
		public abstract void initAndConnect();
		public abstract void close();
		public abstract void write(Dictionary<string, PIData> piDataMap, Dictionary<string, string> topicsMap);

		protected string brokeraddress;
		protected int brokerport;
		protected string clientname;
		protected string lastWillMessage;

		protected ulong publishedBytesInSchedule;
		protected ulong publishedConfirmedMessageCount;

		internal void raiseWriterClosed() {
			if (null != MQTTWriter_ClientClosed) {
				MQTTClientClosedEventArgs ea = new MQTTClientClosedEventArgs();
				MQTTWriter_ClientClosed(ea);
			}
		}

		internal void raisePublishCompleted(double grandTotalTimeSec, double thrput, int v) {
			if (null != MQTTWriter_PublishCompleted) {
				MQTTPublishTerminatedEventArgs ea = new MQTTPublishTerminatedEventArgs(grandTotalTimeSec, thrput, 0);
				MQTTWriter_PublishCompleted(ea);
			}
		}
	}
}
