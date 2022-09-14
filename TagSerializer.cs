using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

using System.IO;
using System.Globalization;
using NLog;

namespace PIDataReaderLib {
	public class TagSerializer {

		public string inDateFormat;
		public string outDateFormat;
		private string valueSeparator = ",";
		private string timeValueSeparator = ":";
		private string fieldSeparator = "|";

		public char csvSeparator = ',';

		private static Logger logger = LogManager.GetCurrentClassLogger();

		public TagSerializer(string inDateFormat, string outDateFormat, string timeValueSeparator, string valueSeparator, string fieldSeparator) {
			this.inDateFormat = inDateFormat;
			this.outDateFormat = outDateFormat;
			this.timeValueSeparator = timeValueSeparator;
			this.valueSeparator = valueSeparator;
			this.fieldSeparator = fieldSeparator;
		}

		public void createFileWithHeader(string outFileFullPath, bool append) {
			if (!append) {
				System.IO.File.Delete(outFileFullPath);
			}

			if (!System.IO.File.Exists(outFileFullPath)) { 
				StreamWriter outFile = null;
				try {
					outFile = File.CreateText(outFileFullPath);
					writeTagTblHeader(outFile);
				} finally {
					if (null != outFile) {
						outFile.Close();
					}
				}
			}
		}

		/*
		 * format of tag files is: tag; time (YYYY-MM-DD HH:mm:ss); value; svalue; status; flags
		 * Example: ITP_MTC_AE-066-491-01.P10.PV; 2016-08-22 11:49:03; -9.2926378; null; 0
		 */
		/*
		* format of phase tag files is: tag, time (YYYY-MM-DD HH:mm:ss), value, svalue, status, flags
		* Example: Phase_T_ITP_PhaseName.P10.BAT, 2016-08-22 11:49:03, leaktest, null, 0
		*/
		public void serializeTagsToLocalFile(List<Tag> tags, string outFileFullPath) {
			StreamWriter outFile = null;
			try {
				outFile = File.AppendText(outFileFullPath);
				
				foreach (Tag tag in tags) {
					try {
						//serializeTagToLocalFileBuffered(tag, outFile);
						serializeTagToLocalFile(tag, outFile);
					} catch (Exception e) {
						logger.Error("Error serializing tag to file. Details: {0}", e.ToString());
					}
				}
			} finally {
				if (null != outFile) {
					outFile.Close();
				}
			}
		}
		
		private void writeTagTblHeader(StreamWriter sw) {
			StringBuilder sb = new StringBuilder();
			sb.AppendFormat("tag,time,value,svalue,status,flags");
			sw.WriteLine(sb.ToString());
			sw.Flush();
		}

		private void serializeTagToLocalFileBuffered(Tag tag, StreamWriter outFile) {
			//sample tag values: 2017-02-13T11-47-16:236.375,2017-02-13T11-48-12:No Data
			//sample csv line should be: ITP_MTC_AE-066-491-01.P10.PV, 2016-08-22 11:49:03, -9.2926378, null, 0

			if (null == tag.tagvalues || 0 == tag.tagvalues.Length) {
				return;
			}
			List<string> tagListItems = new List<string>(tag.tagvalues.Split(new string[] { valueSeparator }, StringSplitOptions.None));
			StringBuilder lineBuilder = new StringBuilder();
			try {
				ulong lineCnt = 0;
				foreach (string tagInfo in tagListItems) {
					string[] tagInfoArray = tagInfo.Split(new string[] { timeValueSeparator }, StringSplitOptions.None);

					if (tagInfoArray.Length < 2) {
						logger.Error("Invalid measurement format detected for Tag {0}. Measurement string is {1}.", tag.name, tagInfo);
						continue;
					}

					string dateStr = tagInfoArray[0];
					string valueStr = null;
					string svalueStr = null;
					string statusString = null;

					//if (tagInfoArray[1].Contains('|')) {
					if (tagInfoArray[1].Contains(fieldSeparator)) {
						string[] valueStrArray = tagInfoArray[1].Split(new string[] { fieldSeparator }, StringSplitOptions.None);
						valueStr = valueStrArray[0];
						svalueStr = valueStrArray[1];
						statusString = valueStrArray[2];
					} else {
						valueStr = tagInfoArray[1];
						statusString = "0";
						if (tag.getIsPhaseTag()) {
							statusString = valueStr;
							valueStr = null;
						}

						if (TypeUtil.Instance.isString(tag.valueType)) {
							svalueStr = valueStr;
							valueStr = null;
						}
					}

					try {

						DateTime dt = DateTime.ParseExact(dateStr, inDateFormat, CultureInfo.InvariantCulture);
						string hadoopDate = dt.ToString(outDateFormat);

						lineBuilder.AppendFormat("{0}{1}{2}{1}{3}{1}{4}{1}{5}{1}{6}\r\n",
							 tag.name,              //0 tag
							 csvSeparator,          //1
							 hadoopDate,            //2 time
							 valueStr,              //3 value
							 svalueStr,             //4 svalue
							 statusString,          //5 status
							 null                   //6 flag
							 );
						lineCnt++;
						if (lineCnt > 1e05) {
							lineBuilder.Remove(lineBuilder.Length - 2, 2);
							outFile.WriteLine(lineBuilder.ToString());
							outFile.Flush();
							lineBuilder = new StringBuilder();
							lineCnt = 0;
						}
					} catch (Exception ex) {
						logger.Error("Unable to serialize tag {0} to file. Value: {1}, date: {2}.", tag.name, valueStr, dateStr);
						logger.Error("Details: {0}", ex.Message);
						logger.Info("Exception: {0}", ex.ToString());
					}
				}
				lineBuilder.Remove(lineBuilder.Length - 2, 2);
				outFile.WriteLine(lineBuilder.ToString());
				outFile.Flush();
			} catch (Exception e) {
				throw new Exception("Unable to serialize tag " + tag.name + " on CSV file. Details: " + e.Message);
			}
		}

		[System.Obsolete]
		private void serializeTagToLocalFile(Tag tag, StreamWriter outFile) {
			//sample tag values: 2017-02-13T11-47-16:236.375,2017-02-13T11-48-12:No Data
			//sample csv line should be: ITP_MTC_AE-066-491-01.P10.PV, 2016-08-22 11:49:03, -9.2926378, null, 0

			if (null == tag.tagvalues || 0 == tag.tagvalues.Length) {
				return;
			}
			List<string> tagListItems = new List<string>(tag.tagvalues.Split(new string[] { valueSeparator }, StringSplitOptions.None));
			StringBuilder lineBuilder = null;
			try { 
				foreach (string tagInfo in tagListItems) {
					string[] tagInfoArray = tagInfo.Split(new string[] { timeValueSeparator }, StringSplitOptions.None);

					if (tagInfoArray.Length < 2) {
						logger.Error("Invalid measurement format detected for Tag {0}. Measurement string is {1}.", tag.name, tagInfo);
						continue;
					}

					string dateStr = tagInfoArray[0];
					string valueStr = null;
					string svalueStr = null;
					string statusString = null;

					//if (tagInfoArray[1].Contains('|')) {
					if (tagInfoArray[1].Contains(fieldSeparator)) {
						string[] valueStrArray = tagInfoArray[1].Split(new string[] { fieldSeparator }, StringSplitOptions.None);
						valueStr = valueStrArray[0];
						svalueStr = valueStrArray[1];
						statusString = valueStrArray[2];
					} else {
						valueStr = tagInfoArray[1];
						statusString = "0";
						if (tag.getIsPhaseTag()) {
							statusString = valueStr;
							valueStr = null;
						}

						if (TypeUtil.Instance.isString(tag.valueType)) {
							svalueStr = valueStr;
							valueStr = null;
						}
					}

					try {
						
						DateTime dt = DateTime.ParseExact(dateStr, inDateFormat, CultureInfo.InvariantCulture);
						string hadoopDate = dt.ToString(outDateFormat);

						lineBuilder = new StringBuilder();

						lineBuilder.AppendFormat("{0}{1}{2}{1}{3}{1}{4}{1}{5}{1}{6}",
							 tag.name,              //0 tag
							 csvSeparator,          //1
							 hadoopDate,            //2 time
							 valueStr,              //3 value
							 svalueStr,             //4 svalue
							 statusString,          //5 status
							 null                   //6 flag
							 );

						outFile.WriteLine(lineBuilder.ToString());
						outFile.Flush();
					} catch(Exception ex) {
						logger.Error("Unable to serialize tag {0} to file. Value: {1}, date: {2}.", tag.name, valueStr, dateStr);
						logger.Error("Details: {0}", ex.Message);
						logger.Info("Exception: {0}", ex.ToString());
					}
				}
			} catch(Exception e) {
				throw new Exception("Unable to serialize tag " + tag.name + " on CSV file. Details: " + e.Message);
			}
		}
	}
}
