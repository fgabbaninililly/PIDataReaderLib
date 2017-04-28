using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Xml;
using System.Xml.Serialization;

namespace PIDataReaderLib {

	[XmlRootAttribute("afdata", Namespace = "", IsNullable = false)]
	public class AFData {
		[XmlArrayItem("afelement")]
		public List<DIAAFElement> afelements { get; set; }

		public AFData() {
			afelements = new List<DIAAFElement>();
		}

		public string writeToXMLString() {
			XmlWriterSettings settings = new XmlWriterSettings();
			settings.Indent = false;
			settings.NewLineHandling = NewLineHandling.None;

			StringWriterWithEncoding sbuilder = new StringWriterWithEncoding(Encoding.UTF8);
			XmlWriter writer = XmlWriter.Create(sbuilder, settings);
			XmlSerializer serializer = new XmlSerializer(typeof(AFData));

			XmlSerializerNamespaces ns = new XmlSerializerNamespaces();
			ns.Add("", "");

			serializer.Serialize(writer, this, ns);
			return sbuilder.ToString();
		}

		public void writeToFile(string filePath) {
			XmlSerializer ser = new XmlSerializer(typeof(AFData));
			TextWriter writer = new StreamWriter(filePath);

			XmlSerializerNamespaces ns = new XmlSerializerNamespaces();
			ns.Add("", "");

			ser.Serialize(writer, this, ns);
			writer.Close();
		}

		public static AFData parseFromFile(string piDataFile) {
			XmlSerializer serializer = new XmlSerializer(typeof(AFData));

			FileStream fs = new FileStream(piDataFile, FileMode.Open);
			// Declare an object variable of the type to be deserialized.
			AFData afOut = null;
			// Use the Deserialize method to restore the object's state with
			// data from the XML document. 
			afOut = (AFData)serializer.Deserialize(fs);
			fs.Close();
			return afOut;
		}
	}

	public class DIAAFElement {
		[XmlAttribute("name")]
		public string name;

		[XmlAttribute("path")]
		public string path;

		[XmlArrayItem("afattribute")]
		public List<DIAAFAttribute> afattributes { get; set; }

		public DIAAFElement() {
			afattributes = new List<DIAAFAttribute>();
		}
	}

	public class DIAAFAttribute {
		[XmlAttribute("name")]
		public string name;

		[XmlAttribute("value")]
		public string value;

		[XmlAttribute("description")]
		public string description;

		[XmlAttribute("type")]
		public string type;

		[XmlAttribute("uom")]
		public string uom;

		/*
		[XmlArrayItem("parameter")]
		public List<Parameter> parameters { get; set; }

		public DIAAFAttribute() {
			parameters = new List<Parameter>();
		}

		public string getParameterValueByName(string parameterName) {
			foreach (Parameter parameter in parameters) {
				if (parameter.name.ToLower().Equals(parameterName)) {
					return parameter.value;
				}
			}
			return null;
		}
		*/
	}

}
