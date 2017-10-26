using NLog;
using System;
using System.Collections.Generic;
using System.Globalization;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace PIDataReaderLib {
	public class BatchSerializer {

		public string inDateFormat;
		public string outDateFormat;
		public char recordSeparator = ',';
		public char timeValueSeparator = ':';
		public char csvSeparator = ',';

		private static Logger logger = LogManager.GetCurrentClassLogger();

		private string batchOutFileFullPath;
		private string unitBatchOutFilePath;
		private string subBatchOutFilePath;

		private StreamWriter swBatch;
		private StreamWriter swUnitBatch;
		private StreamWriter swSubBatch;

		public BatchSerializer(string batchOutFileFullPath, string unitBatchOutFilePath, string subBatchOutFilePath, string inDateFormat, string outDateFormat) {
			this.batchOutFileFullPath = batchOutFileFullPath;
			this.unitBatchOutFilePath = unitBatchOutFilePath;
			this.subBatchOutFilePath = subBatchOutFilePath;
			this.inDateFormat = inDateFormat;
			this.outDateFormat = outDateFormat;
		}

		public bool createFilesWithHeader(bool append) {
			if (!append) {
				System.IO.File.Delete(batchOutFileFullPath);
				System.IO.File.Delete(unitBatchOutFilePath);
				System.IO.File.Delete(subBatchOutFilePath);
			}

			try {
				if (!System.IO.File.Exists(batchOutFileFullPath)) {
					swBatch = File.AppendText(batchOutFileFullPath);
					writeBatchTblHeader();
				} else {
					swBatch = File.AppendText(batchOutFileFullPath);
				}
			} catch (Exception) {
				logger.Error("Error creating file or writing header to file: {0}", batchOutFileFullPath);
				return false;
			}

			try {
				if (!System.IO.File.Exists(unitBatchOutFilePath)) {
					swUnitBatch = File.AppendText(unitBatchOutFilePath);
					writeUnitBatchTblHeader();
				} else {
					swUnitBatch = File.AppendText(unitBatchOutFilePath);
				}
			} catch (Exception) {
				logger.Error("Error creating file or writing header to file: {0}", batchOutFileFullPath);
				return false;
			}

			try {
				if (!System.IO.File.Exists(subBatchOutFilePath)) {
					swSubBatch = File.AppendText(subBatchOutFilePath);
					writeSubBatchTblHeader();
				} else {
					swSubBatch = File.AppendText(subBatchOutFilePath);
				}
			} catch (Exception) {
				logger.Error("Error creating file or writing header to file: {0}", batchOutFileFullPath);
				return false;
			}

			return true;
		}

		[System.Obsolete]
		public bool createFiles(bool append) {
			swBatch = getWriter(batchOutFileFullPath, append);
			if (null == swBatch) {
				return false;
			}
			
			swUnitBatch = getWriter(unitBatchOutFilePath, append);
			if (null == swUnitBatch) {
				return false;
			}

			swSubBatch = getWriter(subBatchOutFilePath, append);
			if (null == swSubBatch) {
				return false;
			}

			if (!append) {
				writeBatchTblHeader();
				writeUnitBatchTblHeader();
				writeSubBatchTblHeader();
			}

			return true;
		}

		public void closeFiles() {
			try { 
				if (null != swBatch) {
					swBatch.Flush();
					swBatch.Close();
				}

				if (null != swUnitBatch) {
					swUnitBatch.Flush();
					swUnitBatch.Close();
				}

				if (null != swSubBatch) {
					swSubBatch.Flush();
					swSubBatch.Close();
				}
			} catch(Exception e) {
				logger.Error("Error closing file. Details: {0}", e.Message);
			}
		}

		/*
		 * Format of batch files is: uid; batchid; starttime (YYYY-MM-DD HH:mm:ss); endtime (YYYY-MM-DD HH:mm:ss); product; recipe; campaignuid
		 * Example: 
		 */
		public void serializeBatchesToLocalFile(List<Batch> batches) {
			foreach (Batch batch in batches) {
				StringBuilder sb = new StringBuilder();
				
				//batch uid may not exist...
				if (null != batch.uid && batch.uid.Length > 0) {
					logger.Info(">Serializing batch {0} ({1})", batch.uid, batch.batchid);
					string hadoopStartDate = getHadoopDate(batch.starttime);
					string hadoopEndDate = "";
					if (null == batch.endtime || batch.endtime.Length == 0) {
						logger.Info("Batch has an empty end date"); 
					} else {
						hadoopEndDate = getHadoopDate(batch.endtime);
					}
					
					try { 
						sb.AppendFormat("'{0}'{1}'{2}'{1}{3}{1}{4}{1}'{5}'{1}'{6}'{1}'{7}'", batch.uid, csvSeparator, batch.batchid, hadoopStartDate, hadoopEndDate, batch.product, batch.recipe, "");
						swBatch.WriteLine(sb.ToString());
						swBatch.Flush();
					} catch (Exception e) {
						logger.Error(">Error while serializing batch {0}", batch.uid);
						logger.Error(">Details: {0}", e.ToString());
					}
				} else {
					logger.Info(">Serializing child unit batches having no parent batch");
				}

				if (null != batch.unitBatches && batch.unitBatches.Count > 0) { 
					serializeUnitBatchesToLocalFile(batch, batch.unitBatches, unitBatchOutFilePath, subBatchOutFilePath);
				}

				if (null != batch.uid && batch.uid.Length > 0) {
					logger.Info(">Completed serializing batch {0} ({1})", batch.uid, batch.batchid);
				} else {
					logger.Info(">Completed serializing child unit batches having no parent batch");
				}
			}
		}

		/*
		 * Format of unit batch files is: uid; batchid; starttime (YYYY-MM-DD HH:mm:ss); endtime (YYYY-MM-DD HH:mm:ss); product; procedure, moduleuid; batchuid
		 * Example: e9153e3b-d7f6-4530-adb7-eabca433a32d; EQO-085599; 2016-12-04 21:50:35; 2016-12-05 03:14:29; TITP_Decontamination; 335082; 04d8902b-e6b5-4bbc-b405-94bae0f1eee7; [batchuid or null]
		 */
		private void serializeUnitBatchesToLocalFile(Batch refBatch, List<UnitBatch> unitBatches, string unitBatchOutFilePath, string subBatchOutFilePath) {
			
			foreach (UnitBatch unitBatch in unitBatches) {
				StringBuilder sb = new StringBuilder();
				logger.Info(">>Serializing unit batch {0} ({1})", unitBatch.uid, unitBatch.batchid);

				string hadoopStartDate = getHadoopDate(unitBatch.starttime);
				string hadoopEndDate = "";
				if (null == unitBatch.endtime || unitBatch.endtime.Length == 0) {
					logger.Info("Unit batch has an empty end date"); 
				} else {
					hadoopEndDate = getHadoopDate(unitBatch.endtime);
				}

				try { 
					sb.AppendFormat(@"'{0}'{1}'{2}'{1}{3}{1}{4}{1}'{5}'{1}'{6}'{1}'{7}'{1}'{8}'", unitBatch.uid, csvSeparator, unitBatch.batchid, hadoopStartDate, hadoopEndDate, unitBatch.product, unitBatch.procedure, unitBatch.moduleuid, refBatch.uid);
					swUnitBatch.WriteLine(sb.ToString());
					swUnitBatch.Flush();
				} catch (Exception e) {
					logger.Error(">>Error while serializing unit batch {0}", unitBatch.uid);
					logger.Error(">>Details: {0}", e.ToString());
				}
				
				if (null != unitBatch.subBatches && unitBatch.subBatches.Count > 0) {
					serializeSubBatchesToLocalFile(unitBatch, subBatchOutFilePath);
				}
				logger.Info(">>Completed serializing unit batch {0} ({1})", unitBatch.uid, unitBatch.batchid);
			}
		}

		/*
		 * Format of sub batch files is: uid; unitbatchuid; path; name; level; childcount; starttime; endtime; headinguid; parentuid
		 * Example: 58ebfcf9-f0b8-45f6-8ddd-1b5661a6946e; e9153e3b-d7f6-4530-adb7-eabca433a32d; [path]; Safe-Vap; 0; 5; 2016-12-04 22:40:58; 2016-12-05 00:50:29; [heading uid]; [parentuid, only for subsubbatches]
		 */
		private void serializeSubBatchesToLocalFile(UnitBatch refUnitBatch, string subBatchOutFilePath) {
			int level = 0;
			string path = @"\";

			foreach (SubBatch subBatch in refUnitBatch.subBatches) {
				writeSubBatchToLocalFile(subBatch, refUnitBatch.uid, null, path, level);
				logger.Info(">>>Completed serializing sub batch {0}", subBatch.uid);

				recurseSerializeSubBatchesToLocalFile(subBatch, refUnitBatch.uid, path, level);
			}
		}

		private void recurseSerializeSubBatchesToLocalFile(SubBatch subBatch, string refUnitBatchUid, string path, int level) {
			if (null == subBatch || 0 == subBatch.subBatches.Count) {
				return;
			}
			level++;

			path += subBatch.name + @"\";
			foreach (SubBatch subBatchChild in subBatch.subBatches) {
				writeSubBatchToLocalFile(subBatchChild, refUnitBatchUid, subBatch.uid, path, level);
				logger.Info(">>>Completed serializing sub batch {0}, level = {1}", subBatchChild.uid, level);

				recurseSerializeSubBatchesToLocalFile(subBatchChild, refUnitBatchUid, path, level);
			}
		}

		private void writeSubBatchToLocalFile(SubBatch subBatch, string refUnitBatchId, string parentUid, string path, int level) {
			StringBuilder sb = new StringBuilder();
			logger.Info(">>>Serializing sub batch {0}", subBatch.uid);

			string hadoopStartDate = getHadoopDate(subBatch.starttime);
			string hadoopEndDate = "";
			if (null == subBatch.endtime || subBatch.endtime.Length == 0) {
				logger.Info("Sub batch has an empty end date");
			} else {
				hadoopEndDate = getHadoopDate(subBatch.endtime);
			}

			try {
				sb.AppendFormat(@"'{0}'{1}'{2}'{1}'{3}'{1}'{4}'{1}{5}{1}{6}{1}{7}{1}{8}{1}'{9}'{1}'{10}'", subBatch.uid, csvSeparator, refUnitBatchId, path, subBatch.name, level, subBatch.subBatches.Count, hadoopStartDate, hadoopEndDate, subBatch.headinguid, parentUid);
				swSubBatch.WriteLine(sb.ToString());
				swSubBatch.Flush();
			} catch (Exception e) {
				logger.Error(">>>Error while serializing sub batch {0}", subBatch.uid);
				logger.Error(">>>Details: {0}", e.ToString());
			}
		}

		/*
		 * If the file specified by path does not exist, it is created. 
		 * If the file does exist, write operations to the StreamWriter append text to the file. 
		 * Additional threads are permitted to read the file while it is open.
		 */
		 [System.Obsolete]
		private StreamWriter getWriter(string outFilePath, bool append) {
			StreamWriter sw = null;
			try {
				if (append) {
					sw = File.AppendText(outFilePath);
				} else {
					sw = File.CreateText(outFilePath);
				}
				
			} catch(Exception ex) {
				logger.Error("Failed to create writer for file: {0}", outFilePath);
				logger.Error("Details: {0}", ex.ToString());
			}
			return sw;
		}

		private string getHadoopDate(string piDate) {
			string hDate = "";
			try { 
				DateTime dtS = DateTime.ParseExact(piDate, inDateFormat, CultureInfo.InvariantCulture);
				hDate = dtS.ToString(outDateFormat);
			} catch(Exception ex) {
				logger.Error("Failed to parse date: {0}", piDate);
				logger.Error("Details: {0}", ex.ToString());
			}

			return hDate;
		}

		private void writeBatchTblHeader() {
			StringBuilder sb = new StringBuilder();
			sb.AppendFormat("uid,batchid,starttime,endtime,product,recipe,campaignuid");
			swBatch.WriteLine(sb.ToString());
			swBatch.Flush();
		}

		private void writeUnitBatchTblHeader() {
			StringBuilder sb = new StringBuilder();
			sb.AppendFormat("uid,batchid,starttime,endtime,product,procedure,moduleuid,batchuid");
			swUnitBatch.WriteLine(sb.ToString());
			swUnitBatch.Flush();
		}

		private void writeSubBatchTblHeader() {
			StringBuilder sb = new StringBuilder();
			sb.AppendFormat("uid,unitbatchuid,path,name,level,childcount,starttime,endtime,headinguid,parentuid");
			swSubBatch.WriteLine(sb.ToString());
			swSubBatch.Flush();
		}

	}
}
