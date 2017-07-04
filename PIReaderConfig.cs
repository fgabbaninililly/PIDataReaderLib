using System.Collections.Generic;
using System.Xml;
using System.Xml.Serialization;
using System.IO;
using System;

namespace PIDataReaderLib {
	
	public class TimeUnit {
		public const string UNIT_SECONDS = "s";
		public const string UNIT_HOURS = "h";
		public const string UNIT_DAYS = "d";
	}

	public class TimeMultipliers {
		public const int HOURS_IN_DAY = 24;
		public const int SECONDS_IN_HOUR = 3600;
		public const int SECONDS_IN_DAY = SECONDS_IN_HOUR * HOURS_IN_DAY;
	}

	public class ConfigurationErrors : Dictionary<int, string> {
		public static readonly int CFGERR_NONE = 0;

		public static readonly int CFGERR_NULL_CONNECTION_OBJECT = 10;
		public static readonly int CFGERR_NULL_PICONNECTION_OBJECT = 20;
		public static readonly int CFGERR_NULL_MQTTCONNECTION_OBJECT = 30;
		public static readonly int CFGERR_NULL_AFCONNECTION_OBJECT = 31;
		public static readonly int CFGERR_NULL_READ_OBJECT = 40;
		public static readonly int CFGERR_NULL_READEXTENT_OBJECT = 50;
		public static readonly int CFGERR_NULL_READEXTENTTYPE_OBJECT = 51;
		public static readonly int CFGERR_NULL_READEXTENTFREQ_OBJECT = 60;
		public static readonly int CFGERR_NULL_READEXTENTFIXED_OBJECT = 70;
		public static readonly int CFGERR_NULL_READEXTENTRELATIVE_OBJECT = 80;
		public static readonly int CFGERR_NULL_PISERVER_NAME = 90;
		public static readonly int CFGERR_NULL_PISDK_NAME = 100;
		public static readonly int CFGERR_NULL_AFSERVER_NAME = 110;
		public static readonly int CFGERR_INVALID_READMODE = 120;
		public static readonly int CFGERR_NULL_AFELEMENTS_OBJECT = 130;
		public static readonly int CFGERR_NULLORINVALID_MAIL_DATA = 140;

		public ConfigurationErrors() {
			this.Add(CFGERR_NONE, "No error detected in configuration.");
			this.Add(CFGERR_NULL_CONNECTION_OBJECT, "Null or invalid reference to Connection object. Please check your xml configuration file.");
			this.Add(CFGERR_NULL_PICONNECTION_OBJECT, "Null or invalid reference to PI connection object. Please check your xml configuration file.");
			this.Add(CFGERR_NULL_AFCONNECTION_OBJECT, "Null or invalid reference to AF connection object. Please check your xml configuration file.");
			this.Add(CFGERR_NULL_MQTTCONNECTION_OBJECT, "Null or invalid reference to MQTT connection object. Please check your xml configuration file.");
			this.Add(CFGERR_NULL_READ_OBJECT, "Null or invalid reference to Read section. Please check your xml configuration file.");
			this.Add(CFGERR_NULL_READEXTENT_OBJECT, "Null or invalid reference to ReadExtent section. Please check your xml configuration file.");
			this.Add(CFGERR_NULL_READEXTENTTYPE_OBJECT, "Null or invalid read extent type. Please check your xml configuration file.");
			this.Add(CFGERR_NULL_READEXTENTFREQ_OBJECT, "Null or invalid reference to read extent frequency section. Please check your xml configuration file.");
			this.Add(CFGERR_NULL_READEXTENTFIXED_OBJECT, "Null or invalid reference to read extent fixed section. Please check your xml configuration file.");
			this.Add(CFGERR_NULL_READEXTENTRELATIVE_OBJECT, "Null or invalid reference to read extent relative section. Please check your xml configuration file.");
			this.Add(CFGERR_NULL_PISERVER_NAME, "Null or invalid reference to PI server name. Please check your xml configuration file.");
			this.Add(CFGERR_NULL_PISDK_NAME, "Null or invalid reference to PI sdk type. Please check your xml configuration file.");
			this.Add(CFGERR_NULL_AFSERVER_NAME, "Null or invalid reference to AF server or database. Please check your xml configuration file.");
			this.Add(CFGERR_INVALID_READMODE, "Null or invalid read mode. Please check your xml configuration file.");
			this.Add(CFGERR_NULL_AFELEMENTS_OBJECT, "Null reference to AF element list. Please check your xml configuration file.");
		}
	}

	[XmlRootAttribute("config", Namespace = "", IsNullable = false)]
	public class PIReaderConfig {
		[XmlAttribute("test")]
		public string test;

		[XmlAttribute("version")]
		public string version;

		[XmlArrayItem("connection")]
		public List<Connection> connections { get; set; }

		[XmlElement("read")]
		public Read read { get; set; }

		[XmlElement("dateformats")]
		public DateFormats dateFormats { get; set; }

		[XmlElement("maildata")]
		public MailData mailData { get; set; }

		public bool isTest() {
			if (test.ToLower().Equals("true")) {
				return true;
			}
			return false;
		}

		public Connection getConnectionByName(string name) {
			foreach (Connection connection in connections) {
				if (connection.name.ToLower().Equals(name)) {
					return connection;
				}
			}
			return null;
		}

		public static PIReaderConfig parseFromFile(string configFile) {
			XmlSerializer serializer = new XmlSerializer(typeof(PIReaderConfig));

			FileStream fs = new FileStream(configFile, FileMode.Open);
			// Declare an object variable of the type to be deserialized.
			PIReaderConfig configuration = null;
			// Use the Deserialize method to restore the object's state with
			// data from the XML document. 
			configuration = (PIReaderConfig)serializer.Deserialize(fs);
			fs.Close();
			return configuration;
		}

		private static int checkCommonValid(PIReaderConfig config) {
			if (null == config.connections) {
				return ConfigurationErrors.CFGERR_NULL_CONNECTION_OBJECT;
			}
			return ConfigurationErrors.CFGERR_NONE;
		}

		public static int checkAFValid(PIReaderConfig config) {
			int err = checkCommonValid(config);
			if (ConfigurationErrors.CFGERR_NONE != err) {
				return err;
			}

			Connection connection = config.getConnectionByName("af");
			if (null == connection) {
				return ConfigurationErrors.CFGERR_NULL_AFCONNECTION_OBJECT;
			}

			//CONNECTION SECTION
			if (null == connection.getParameterValueByName(Parameter.PARAMNAME_AFSERVERNAME) || 0 == connection.getParameterValueByName(Parameter.PARAMNAME_AFDATABASE).Length) {
				return ConfigurationErrors.CFGERR_NULL_AFSERVER_NAME;
			}
			if (null == connection.getParameterValueByName(Parameter.PARAMNAME_AFDATABASE) || 0 == connection.getParameterValueByName(Parameter.PARAMNAME_AFDATABASE).Length) {
				return ConfigurationErrors.CFGERR_NULL_AFSERVER_NAME;
			}

			//READ SECTION
			if (null == config.read) {
				return ConfigurationErrors.CFGERR_NULL_READ_OBJECT;
			}

			if (null == config.read.readMode || !Read.READMODE_AFELEMENT.Equals(config.read.readMode.ToLower())) {
				return ConfigurationErrors.CFGERR_INVALID_READMODE;
			}

			if (null == config.read.afelements) {
				return ConfigurationErrors.CFGERR_NULL_AFELEMENTS_OBJECT;
			}
			
			return ConfigurationErrors.CFGERR_NONE;
		}

		public static int checkValid(PIReaderConfig config) {
			int err = checkCommonValid(config);
			if (ConfigurationErrors.CFGERR_NONE != err) {
				return err;
			}
						
			Connection connection = config.getConnectionByName("pi");
			if (null == connection) {
				return ConfigurationErrors.CFGERR_NULL_PICONNECTION_OBJECT;
			}

			Connection mqttConnection = config.getConnectionByName("mqtt");
			if (null == mqttConnection) {
				return ConfigurationErrors.CFGERR_NULL_MQTTCONNECTION_OBJECT;
			}

			if (null == config.read) {
				return ConfigurationErrors.CFGERR_NULL_READ_OBJECT;
			}

			//MAIL SECTION
			if (null == config.mailData) {
				return ConfigurationErrors.CFGERR_NULLORINVALID_MAIL_DATA;
			}
			if (null == config.mailData.smtphost || null == config.mailData.from || null == config.mailData.to || null == config.mailData.subject || null == config.mailData.body) {
				return ConfigurationErrors.CFGERR_NULLORINVALID_MAIL_DATA;
			}

			//READ EXTENT SECTION
			if (null == config.read.readExtent) {
				return ConfigurationErrors.CFGERR_NULL_READEXTENT_OBJECT;
			}

			if (null == config.read.readExtent.type || 0 == config.read.readExtent.type.Length) {
				return ConfigurationErrors.CFGERR_NULL_READEXTENTTYPE_OBJECT;
			}

			if (config.read.readExtent.type.Equals(ReadExtent.READ_EXTENT_FREQUENCY)) {
				if (null == config.read.readExtent.readExtentFrequency) {
					return ConfigurationErrors.CFGERR_NULL_READEXTENTFREQ_OBJECT;
				}
				
			} else if (config.read.readExtent.type.Equals(ReadExtent.READ_EXTENT_FIXED)) {
				if (null == config.read.readExtent.readExtentFixed) {
					return ConfigurationErrors.CFGERR_NULL_READEXTENTFIXED_OBJECT;
				}
			} else if (config.read.readExtent.type.Equals(ReadExtent.READ_EXTENT_RELATIVE)) {
				if (null == config.read.readExtent.readExtentRelative) {
					return ConfigurationErrors.CFGERR_NULL_READEXTENTRELATIVE_OBJECT;
				}
			} else {
				return ConfigurationErrors.CFGERR_NULL_READEXTENTTYPE_OBJECT;
			}

			//CONNECTION SECTION
			if (null == connection.getParameterValueByName(Parameter.PARAMNAME_PISERVERNAME) || 0 == connection.getParameterValueByName(Parameter.PARAMNAME_PISERVERNAME).Length) {
				return ConfigurationErrors.CFGERR_NULL_PISERVER_NAME;
			}

			if (null == connection.getParameterValueByName(Parameter.PARAMNAME_PISDKTYPE) || 0 == connection.getParameterValueByName(Parameter.PARAMNAME_PISDKTYPE).Length) {
				return ConfigurationErrors.CFGERR_NULL_PISDK_NAME;
			}
			return ConfigurationErrors.CFGERR_NONE;
		}
	}

	public class DateFormats {
		[XmlAttribute("pi")]
		public string pi;

		[XmlAttribute("reference")]
		public string reference;

		[XmlAttribute("hadoop")]
		public string hadoop;
	}

	public class MailData {
		[XmlAttribute("enabled")]
		public bool enabled;

		[XmlElement("smtphost")]
		public string smtphost;

		[XmlElement("from")]
		public string from;

		[XmlElement("to")]
		public string to;

		[XmlElement("subject")]
		public string subject;

		[XmlElement("body")]
		public string body;
	}

	public class Connection {
		[XmlAttribute("name")]
		public string name;

		[XmlArrayItem("parameter")]
		public List<Parameter> parameters { get; set; }

		public string getParameterValueByName(string parameterName) {
			foreach(Parameter parameter in parameters) {
				if (parameter.name.ToLower().Equals(parameterName)) {
					return parameter.value;
				}
			}
			return null;
		}
	}

	public class Parameter {
		public const string PARAMNAME_PISERVERNAME = "piservername";
		public const string PARAMNAME_PISDKTYPE = "pisdktype";
		public const string PARAMNAME_PIBOUNDARY = "piboundarytype";

		public const string PARAMNAME_MQTTBROKERADDRESS = "mqttbrokeraddress";
		public const string PARAMNAME_MQTTBROKERPORT = "mqttbrokerport";
		public const string PARAMNAME_MQTTCLIENTNAME = "mqttclientname";

		public const string PARAMNAME_AFSERVERNAME = "afservername";
		public const string PARAMNAME_AFDATABASE = "afdatabase";

		public const string PARAMNAME_MQTTOUT_TOPIC = "topic";
		public const string PARAMNAME_MQTTOUT_CLIENTID = "clientid";

		public const string PARAM_VALUE_SDK_AF = "afsdk";
		public const string PARAM_VALUE_SDK_PI = "pisdk";
		public const string PARAM_VALUE_BOUNDARY_INSIDE = "inside";
		public const string PARAM_VALUE_BOUNDARY_OUTSIDE = "outside";
		public const string PARAM_VALUE_BOUNDARY_INTERPOLATED = "interpolated";

		[XmlAttribute("name")]
		public string name;

		[XmlText()]
		public string value;
	}
	
	public class Read {
		public const string READMODE_TAG = "tag";
		public const string READMODE_BATCH = "batch";
		public const string READMODE_AFELEMENT = "afelement";

		[XmlAttribute("mode")]
		public string readMode;

		[XmlElement("readextent")]
		public ReadExtent readExtent;

		[XmlArrayItem("equipment")]
		public List<EquipmentCfg> equipments { get; set; }
		
		[XmlArrayItem("batch")]
		public List<BatchCfg> batches { get; set; }

		[XmlArrayItem("afelement")]
		/*
		 * DIAAFElement class implemented in AFData.cs. Same classe used for in/out.
		 * */
		public List<DIAAFElement> afelements { get; set; }

		public bool readBatches() {
			if (READMODE_BATCH.Equals(readMode.ToLower())) {
				return true;
			}
			return false;
		}
	}

	public class BatchCfg {
		[XmlAttribute("usebatch")]
		public string useBatch;

		[XmlAttribute("useunitbatch")]
		public string useUnitBatch;

		[XmlAttribute("usesubbatch")]
		public string useSubBatch;

		[XmlAttribute("usephase")]
		public string usePhase;

		[XmlAttribute("modulepath")]
		public string modulePath;

		[XmlAttribute("modulename")]
		public string moduleName;

		[XmlAttribute("mqtttopic")]
		public string mqttTopic;

		public bool useUnitBatchBool() {
			if (useUnitBatch.ToLower().Equals("false") || useUnitBatch.ToLower().Equals("no")) {
				return false;
			}
			return true;
		}
		public bool useBatchBool() {
			if (useBatch.ToLower().Equals("false") || useBatch.ToLower().Equals("no")) {
				return false;
			}
			return true;
		}
		public bool useSubBatchBool() {
			if (useSubBatch.ToLower().Equals("false") || useSubBatch.ToLower().Equals("no")) {
				return false;
			}
			return true;
		}
		public bool usePhaseBool() {
			if (usePhase.ToLower().Equals("false") || usePhase.ToLower().Equals("no")) {
				return false;
			}
			return true;
		}
	}

	public class EquipmentCfg {
		[XmlAttribute("name")]
		public string name;
		
		[XmlAttribute("mqtttopic")]
		public string mqttTopic;
		
		[XmlElement("taglist")]
		public TagList tagList;

		[XmlElement("phaselist")]
		public PhaseList phaseList;

		public EquipmentCfg() {
			phaseList = new PhaseList();
			tagList = new TagList();
		}
	}

	public class TagList {
		[XmlText()]
		public string tags;
	}

	public class PhaseList {
		[XmlText()]
		public string phases;
	}

	public class PISDKData {
		[XmlAttribute("type")]
		public string type;
		
		[XmlAttribute("boundary")]
		public string boundary;
	}

	public class ReadExtent {
		public static readonly string READ_EXTENT_FREQUENCY = "frequency";
		public static readonly string READ_EXTENT_FIXED = "fixed";
		public static readonly string READ_EXTENT_RELATIVE = "relative";

		[XmlElement("frequency")]
		public ReadExtentFrequency readExtentFrequency;

		[XmlElement("fixed")]
		public ReadExtentFixed readExtentFixed;

		[XmlElement("relative")]
		public ReadExtentRelative readExtentRelative;

		[XmlAttribute("type")]
		public string type;

		[XmlAttribute("slice")]
		public string slice;

		[XmlAttribute("sliceunit")]
		public string sliceunit;

		public long getSliceDuration() {
			long sd = 0;
			try { 
				sd = Int64.Parse(slice);
			} catch(Exception) {}
			return sd;
		}

		public bool isSliced() {
			return getSliceDuration() != 0;
		}

		public long getSliceDurationSec() {
			double multiplier = 1.0;
			if ("m".Equals(sliceunit.ToLower())) {
				multiplier = 60.0;
			}
			if ("h".Equals(sliceunit.ToLower())) {
				multiplier = 3600.0;
			}
			if ("d".Equals(sliceunit.ToLower())) {
				multiplier = 86400.0;
			}
						
			return (long)(multiplier * getSliceDuration());
		}
	}

	public class ReadExtentFrequency {
		[XmlAttribute("value")]
		public string value;

		[XmlAttribute("unit")]
		public string unit;

		[XmlAttribute("buffer")]
		public string buffer;

		[XmlAttribute("limit")]
		public string limit;

		public double getReadBackSecondsAsDouble() {
			double buf = double.Parse(buffer);
			return getFrequencySecondsAsDouble() * (1 + buf);
		}

		public double getFrequencySecondsAsDouble() {
			double multiplier = 1.0;
			if (unit.ToLower().Equals(TimeUnit.UNIT_DAYS)) {
				multiplier = TimeMultipliers.SECONDS_IN_DAY;
			}
			if (unit.ToLower().Equals(TimeUnit.UNIT_HOURS)) {
				multiplier = TimeMultipliers.SECONDS_IN_HOUR;
			}
			return double.Parse(value) * multiplier;
		}

		public long getReadbackLimitSeconds() {
			long multiplier = 1;
			if (unit.ToLower().Equals(TimeUnit.UNIT_DAYS)) {
				multiplier = TimeMultipliers.SECONDS_IN_DAY;
			}
			if (unit.ToLower().Equals(TimeUnit.UNIT_HOURS)) {
				multiplier = TimeMultipliers.SECONDS_IN_HOUR;
			}
			long readBackLimit = 0;
			if (null != limit && limit.Length > 0) {
				readBackLimit = long.Parse(limit) * multiplier;
			}
			return readBackLimit;
		}
	}

	public class ReadExtentFixed {
		[XmlAttribute("startdate")]
		public string startdate;

		[XmlAttribute("enddate")]
		public string enddate;
	}

	public class ReadExtentRelative {
		[XmlAttribute("readpast")]
		public string readpast;

		[XmlAttribute("unit")]
		public string unit;

		public double getReadBackSecondsAsDouble() {
			double multiplier = 1.0;
			if (unit.ToLower().Equals(TimeUnit.UNIT_DAYS)) {
				multiplier = TimeMultipliers.SECONDS_IN_DAY;
			}
			if (unit.ToLower().Equals(TimeUnit.UNIT_HOURS)) {
				multiplier = TimeMultipliers.SECONDS_IN_HOUR;
			}
			return double.Parse(readpast) * multiplier;
		}
	}
}
