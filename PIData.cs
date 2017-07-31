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

		[XmlElement("batch")]
		public List<Batch> batches { get; set; }

		[XmlArrayItem("tag")]
		public List<Tag> tags { get; set; }

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

		[XmlElement("subbatch")]
		public List<SubBatch> subBatches { get; set; }

		public UnitBatch() {
			subBatches = new List<SubBatch>();
		}
	}

	public class SubBatch {
		[XmlAttribute("uid")]
		public string uid;

		[XmlAttribute("starttime")]
		public string starttime;

		[XmlAttribute("endtime")]
		public string endtime;

		[XmlAttribute("name")]
		public string name;

		[XmlAttribute("headinguid")]
		public string headinguid;

		//[XmlAttribute("path")]
		//public string path;

		[XmlElement("subbatch")]
		public List<SubBatch> subBatches { get; set; }

		public SubBatch() {
			subBatches = new List<SubBatch>();
		}
	}

	/*
	public class SubBatchItem {
		[XmlAttribute("uid")]
		public string uid;

		[XmlAttribute("starttime")]
		public string starttime;

		[XmlAttribute("endtime")]
		public string endtime;

		[XmlAttribute("name")]
		public string name;

		[XmlAttribute("headinguid")]
		public string headinguid;

		[XmlAttribute("path")]
		public string path;
	}
	*/

	public class Tag {
		[XmlAttribute("name")]
		public string name;
		
		[XmlText()]
		public string tagvalues;

		[XmlAttribute("isphase")]
		public string isPhase;

		public bool hasStringValues;

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
	}
}
