using NLog;
using NLog.Targets;
using NLog.Targets.Wrappers;
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Security.Cryptography;
using System.Text;
using System.Threading.Tasks;

namespace PIDataReaderLib {
	public class ReadInterval {
		public DateTime start { get; set; }
		public DateTime end { get; set; }

		private const string format = "yyyy-MM-ddTHH-mm-ss.fff";

		public ReadInterval(DateTime start, DateTime end) {
			this.start = start;
			this.end = end;
		}

		public override string ToString() {
			return String.Format("[{0}, {1}]", start.ToString(format), end.ToString(format));
		}
	}
	public class Utils {
		public static readonly string READEND_MARKER = "!#Readend";
		public static readonly string READEND_SEPARATOR = "=>";

		private static string getLogFileFullPath() {
			FileTarget fileTarget = null;
			try {
				AsyncTargetWrapper awTarget = (AsyncTargetWrapper)LogManager.Configuration.FindTargetByName("f2");
				fileTarget = (FileTarget)awTarget.WrappedTarget;
			} catch (InvalidCastException) {
				fileTarget = (FileTarget)LogManager.Configuration.FindTargetByName("f2");
			}
			LogEventInfo logEventInfo = new LogEventInfo { TimeStamp = DateTime.Now };
			string fileFullPath = fileTarget.FileName.Render(logEventInfo);
			return fileFullPath;
		}

		public static Dictionary<string, string> getLastReadTimesByEquipmentFromLog() {
			//search for latest read times both in the most recent log file AND in the most recent archive file
			string logFileName = getLogFileFullPath();

			Dictionary<string, string> lastReadTimesFromLog = getLastReadTimesByEquipment(logFileName);
			
			string folderName = Path.GetDirectoryName(logFileName);
			DirectoryInfo info = new DirectoryInfo(folderName);
			FileInfo[] files = info.GetFiles("*.log");

			if (files.Length == 0) {
				//no archive files...
				return lastReadTimesFromLog;
			}
			
			// Sort by creation-time descending 
			Array.Sort(files, delegate (FileInfo f1, FileInfo f2)
			{
				return f1.CreationTime.CompareTo(f2.CreationTime);
			});
			

			Dictionary<string, string> lastReadTimesFromArchive = getLastReadTimesByEquipment(files[0].FullName);
			foreach (string equipment in lastReadTimesFromArchive.Keys) {
				if (!lastReadTimesFromLog.ContainsKey(equipment)) {
					lastReadTimesFromLog.Add(equipment, lastReadTimesFromArchive[equipment]);
				}
			}

			return lastReadTimesFromLog;
		}

		private static Dictionary<string, string> getLastReadTimesByEquipment(string fileName) {
			Dictionary<string, string> lastReadTimesByEquipment = new Dictionary<string, string>();
			if (!File.Exists(fileName)) {
				return lastReadTimesByEquipment;
			}
			StreamReader file = new StreamReader(fileName);
			try {
				string line;
				while ((line = file.ReadLine()) != null) {
					if (line.Contains(Utils.READEND_MARKER)) {
						addReadEnd(line, lastReadTimesByEquipment);
					}
				}
			} finally {
				file.Close();
			}
			return lastReadTimesByEquipment;
		}

		/*
		public static Dictionary<string, string> getLastReadTimesByEquipmentFromLogOld() {
			string fileName = getLogFileFullPath();
			Dictionary<string, string> lastReadTimesByEquipment = new Dictionary<string, string>();
			if (!File.Exists(fileName)) {
				return lastReadTimesByEquipment;
			}

			StreamReader file = new StreamReader(fileName);
			try {
				string line;
				while ((line = file.ReadLine()) != null) {
					if (line.Contains(Utils.READEND_MARKER)) {
						addReadEnd(line, lastReadTimesByEquipment);
					}
				}
			} finally {
				file.Close();
			}
			return lastReadTimesByEquipment;
		}
		*/

		public static void redirectLogFile(string newPath) {

			FileTarget fileTarget = null;
			try {
				AsyncTargetWrapper awTarget = (AsyncTargetWrapper)LogManager.Configuration.FindTargetByName("f2");
				fileTarget = (FileTarget)awTarget.WrappedTarget;
			} catch (InvalidCastException) {
				fileTarget = (FileTarget)LogManager.Configuration.FindTargetByName("f2");
			}
			// Need to set timestamp here if filename uses date. 
			// For example - filename="${basedir}/logs/${shortdate}/trace.log"
			LogEventInfo logEventInfo = new LogEventInfo { TimeStamp = DateTime.Now };
			string fileFullPath = fileTarget.FileName.Render(logEventInfo);
			string archiveFileFullPath = fileTarget.ArchiveFileName.Render(logEventInfo);

			string filePath = Path.GetDirectoryName(fileFullPath);
			string archiveFilePath = Path.GetDirectoryName(archiveFileFullPath);
			string fileName = Path.GetFileName(fileFullPath);
			string archiveFileName = Path.GetFileName(archiveFileFullPath);

			filePath = newPath + "\\" + fileName;
			archiveFilePath = newPath + "\\" + archiveFileName;

			fileTarget.FileName = filePath;
			fileTarget.ArchiveFileName = archiveFilePath;
		}

		private static void addReadEnd(string line, Dictionary<string, string> lastReadTimesByEquipment) {
			string[] tmp = line.Split(new string[] { Utils.READEND_SEPARATOR }, StringSplitOptions.None);
			string[] nameValuePair = tmp[1].Split(',');
			if (lastReadTimesByEquipment.ContainsKey(nameValuePair[0])) {
				lastReadTimesByEquipment.Remove(nameValuePair[0]);
			}
			lastReadTimesByEquipment.Add(nameValuePair[0], nameValuePair[1]);
		}

		public static string md5Calc(string inString) {
			MD5 md5Hash = MD5.Create();
			// Convert the input string to a byte array and compute the hash.
			byte[] data = md5Hash.ComputeHash(Encoding.UTF8.GetBytes(inString));

			// Create a new Stringbuilder to collect the bytes
			// and create a string.
			StringBuilder sBuilder = new StringBuilder();

			// Loop through each byte of the hashed data 
			// and format each one as a hexadecimal string.
			for (int i = 0; i < data.Length; i++) {
				sBuilder.Append(data[i].ToString("x2"));
			}

			// Return the hexadecimal string.
			return sBuilder.ToString();
		}

		public static string stringList2QuotedCsv(SortedSet<string> strList) {
			if (0 == strList.Count) {
				return "";
			}

			StringBuilder sb = new StringBuilder();
			foreach(string s in strList) {
				sb.Append("'" + s + "',");
			}
			sb.Remove(sb.Length - 1, 1);

			return sb.ToString();
		}
	}
	public sealed class TypeUtil {
		private static volatile TypeUtil instance;
		private static object syncRoot = new Object();
		private static HashSet<TypeCode> integerCodes;
		private static HashSet<TypeCode> decimalCodes;
		private static HashSet<TypeCode> stringCodes;

		private TypeUtil() {
			TypeCode[] num = {
				TypeCode.Byte,
				TypeCode.Int16,
				TypeCode.Int32,
				TypeCode.Int64,
				TypeCode.SByte,
				TypeCode.UInt16,
				TypeCode.UInt32,
				TypeCode.UInt64 };

			TypeCode[] flt = {
				TypeCode.Decimal,
				TypeCode.Double,
				TypeCode.Single
			};

			TypeCode[] str = {
				TypeCode.String
			};

			integerCodes = new HashSet<TypeCode>(num);
			decimalCodes = new HashSet<TypeCode>(flt);
			stringCodes = new HashSet<TypeCode>(str);
		}

		public static TypeUtil Instance {
			get {
				if (instance == null) {
					lock (syncRoot) {
						if (instance == null)
							instance = new TypeUtil();
					}
				}
				return instance;
			}
		}

		public bool isInteger(Type t) {
			return integerCodes.Contains(Type.GetTypeCode(t));
		}

		public bool isDecimal(Type t) {
			return decimalCodes.Contains(Type.GetTypeCode(t));
		}

		public bool isString(Type t) {
			return stringCodes.Contains(Type.GetTypeCode(t));
		}
	}
}
