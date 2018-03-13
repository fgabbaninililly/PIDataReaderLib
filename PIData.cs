using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Xml;
using System.Xml.Serialization;

namespace PIDataReaderLib {
	[XmlRootAttribute("pidata", Namespace = "", IsNullable = false)]
	public class PIData {

		public enum SerializationType {
			xml, text
		};

		[XmlAttribute("type")]
		public string type;

		[XmlAttribute("intervalstart")]
		public string readIntervalStart;

		[XmlAttribute("intervalend")]
		public string readIntervalEnd;

		[XmlAttribute("readfinished")]
		public string readFinished;

		[XmlAttribute("schedule_frequency_sec")]
		public string scheduleFrequencySec;

		[XmlAttribute("timeSeparator")]
		public string timeSeparator;

		[XmlAttribute("fieldSeparator")]
		public string fieldSeparator;

		[XmlAttribute("valueSeparator")]
		public string valueSeparator;

		[XmlElement("batch")]
		public List<Batch> batches { get; set; }

		[XmlArrayItem("tag")]
		public List<Tag> tags { get; set; }

		[XmlIgnore]
		public bool tagsSpecified {
			get {
				return (tags.Count > 0);
			}
		}

		public PIData() {
			batches = new List<Batch>();
			tags = new List<Tag>();
		}

		public string writeToString(string serializationType) {
			if (serializationType.ToLower().Equals("xml")) {
				return writeToString(SerializationType.xml);
			} else {
				return writeToString(SerializationType.text);
			}
		}

		public string writeToString(SerializationType serializationType) {
			if ((serializationType == SerializationType.xml)) {
				return writeToXMLString();
			} else {
				return writeToTextString();
			}

		}

		private string writeToXMLString() {
			XmlWriterSettings settings = new XmlWriterSettings();
			settings.Indent = false;
			settings.NewLineHandling = NewLineHandling.None;

			StringWriterWithEncoding sbuilder = new StringWriterWithEncoding(Encoding.UTF8);
			XmlWriter writer = XmlWriter.Create(sbuilder, settings);
			XmlSerializer serializer = new XmlSerializer(typeof(PIData));

			XmlSerializerNamespaces ns = new XmlSerializerNamespaces();
			ns.Add("", "");

			serializer.Serialize(writer, this, ns);
			return sbuilder.ToString();
		}

		private string writeToTextString() {
			StringBuilder sbuilder = new StringBuilder();
			foreach (Tag tag in tags) {
				sbuilder.Append(tag.name + ": {");
				sbuilder.Append(tag.tagvalues);
				sbuilder.Append("},");
			}
			sbuilder.Remove(sbuilder.Length - 1, 1);
			return sbuilder.ToString();
		}

		public static PIData parseFromFile(string piDataFile) {
			XmlSerializer serializer = new XmlSerializer(typeof(PIData));

			FileStream fs = new FileStream(piDataFile, FileMode.Open);
			// Declare an object variable of the type to be deserialized.
			PIData piData = null;
			// Use the Deserialize method to restore the object's state with
			// data from the XML document. 
			piData = (PIData)serializer.Deserialize(fs);
			fs.Close();
			return piData;
		}
	}

	public class Batch {
		[XmlAttribute("uid")]
		public string uid;
		
		[XmlAttribute("batchid")]
		public string batchid;
		
		[XmlAttribute("starttime")]
		public string starttime;
		
		[XmlAttribute("endtime")]
		public string endtime;

		[XmlAttribute("product")]
		public string product;

		[XmlAttribute("recipe")]
		public string recipe;

		[XmlAttribute("campaignuid")]
		public string campaignuid;

		[XmlElement("unitbatch")]
		public List<UnitBatch> unitBatches { get; set; }

		public Batch() {
			unitBatches = new List<UnitBatch>();
		}
	}

	public class UnitBatch {
		[XmlAttribute("uid")]
		public string uid;

		[XmlAttribute("batchid")]
		public string batchid;

		[XmlAttribute("starttime")]
		public string starttime;

		[XmlAttribute("endtime")]
		public string endtime;

		[XmlAttribute("product")]
		public string product;

		[XmlAttribute("procedure")]
		public string procedure;

		[XmlAttribute("moduleuid")]
		public string moduleuid;

		[XmlAttribute("batchuid")]
		public string batchuid;

		[XmlElement("subbatch")]
		public List<SubBatch> subBatches { get; set; }

		public UnitBatch() {
			subBatches = new List<SubBatch>();
		}
	}

	public class SubBatch {
		[XmlAttribute("uid")]
		public string uid;

		[XmlAttribute("unitbatchuid")]
		public string unitbatchuid;

		[XmlAttribute("starttime")]
		public string starttime;

		[XmlAttribute("endtime")]
		public string endtime;

		[XmlAttribute("name")]
		public string name;

		[XmlAttribute("headinguid")]
		public string headinguid;

		[XmlAttribute("level")]
		public int level;

		[XmlAttribute("childcount")]
		public int childcount;

		[XmlAttribute("parentuid")]
		public string parentuid;

		[XmlAttribute("path")]
		public string path;

		[XmlElement("subbatch")]
		public List<SubBatch> subBatches { get; set; }

		public SubBatch() {
			subBatches = new List<SubBatch>();
		}

		public override string ToString() {
			return uid;
		}

	}

	public class Tag {
		[XmlAttribute("name")]
		public string name;
		
		[XmlText()]
		public string tagvalues;

		[XmlAttribute("isphase")]
		public string isPhase;

		[XmlIgnoreAttribute]
		public Type valueType;
		
		public Tag() {
			isPhase = "false";
		}

		public void setIsPhaseTag(bool isPhaseTag) {
			isPhase = "false";
			if (isPhaseTag) {
				isPhase = "true";
			}
		}

		public bool getIsPhaseTag() {
			if ("false".Equals(isPhase.ToLower())) {
				return false;
			}
			return true;
		}

		public void addTimedValue(string dateTimeStr, string valueStr, string timeSeparator, string fieldSeparator, string valueSeparator) {
			if (null == tagvalues || 0 == tagvalues.Length) {
				tagvalues += String.Format("{0}{1}{2}", 
					dateTimeStr,							//{0}
					timeSeparator,                          //{1}
					valueStr                                //{2}
					);	
			} else {
				tagvalues += String.Format("{0}{1}{2}{3}", 
					valueSeparator,                         //{0}
					dateTimeStr,                            //{1}
					timeSeparator,                          //{2}
					valueStr                                //{3}
					);
			}
		}

		public void addTimedTriple(string dateTimeStr, string valueStr, string svalueStr, string statusStr, string timeSeparator, string fieldSeparator, string valueSeparator) {
			if (null == tagvalues || 0 == tagvalues.Length) {
				tagvalues += String.Format("{0}{1}{2}{3}{4}{5}{6}", 
					dateTimeStr,							//{0}
					timeSeparator,                          //{1}
					valueStr,                               //{2}
					fieldSeparator,                         //{3}
					svalueStr,                              //{4}
					fieldSeparator,                         //{5}
					statusStr                               //{6}
					);			
			} else {
				tagvalues += String.Format("{0}{1}{2}{3}{4}{5}{6}{7}", 
					valueSeparator,							//{0}
					dateTimeStr,                            //{1}
					timeSeparator,                          //{2}
					valueStr,                               //{3}
					fieldSeparator,                         //{4}
					svalueStr,                              //{5}
					fieldSeparator,                         //{6}
					statusStr                               //{7}
					);
			}
		}
	}
}
