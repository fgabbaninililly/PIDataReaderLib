﻿using NLog;
using NLog.Targets;
using NLog.Targets.Wrappers;
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace PIDataReaderLib {
	public class ReadInterval {
		public DateTime start { get; set; }
		public DateTime end { get; set; }

		public ReadInterval(DateTime start, DateTime end) {
			this.start = start;
			this.end = end;
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

	}
}
