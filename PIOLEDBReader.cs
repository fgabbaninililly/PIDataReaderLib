using System;
using System.Collections.Generic;
using System.Data;
using System.Data.OleDb;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace PIDataReaderLib {
	
	class PIOLEDBSQL {
		//UNIT BATCHES
		internal static string QRY_MODULEUID_TEMPLATE = @"SELECT uid FROM [pimodule].[pimoduleh] WHERE path = '{0}' AND name = '{1}'";
		internal static string QRY_UNITBATCHES_TEMPLATE = @"SELECT * FROM [pibatch].[piunitbatch] WHERE (endtime is null OR endtime BETWEEN '{0}' AND '*') AND (starttime BETWEEN '01/01/1970 00:00:00' AND '{1}') AND (moduleuid='{2}')";
		internal static string PIUNITBATCH_FLD_UID = @"uid";
		internal static string PIUNITBATCH_FLD_BATCHID = @"batchid";
		internal static string PIUNITBATCH_FLD_STARTTIME = @"starttime";
		internal static string PIUNITBATCH_FLD_ENDTIME = @"endtime";
		internal static string PIUNITBATCH_FLD_PRODUCT = @"product";
		internal static string PIUNITBATCH_FLD_PROCEDURE = @"procedure";
		internal static string PIUNITBATCH_FLD_MODULEUID = @"moduleuid";
		internal static string PIUNITBATCH_FLD_BATCHUID = @"batchuid";

		//BATCHES
		internal static string QRY_BATCHES_TEMPLATE = @"SELECT * FROM [pibatch].[pibatch] WHERE uid IN ({0})";
		internal static string PIBATCH_FLD_UID = @"uid";
		internal static string PIBATCH_FLD_BATCHID = @"batchid";
		internal static string PIBATCH_FLD_STARTTIME = @"starttime";
		internal static string PIBATCH_FLD_ENDTIME = @"endtime";
		internal static string PIBATCH_FLD_PRODUCT = @"product";
		internal static string PIBATCH_FLD_RECIPE = @"recipe";
		internal static string PIBATCH_FLD_CAMPAIGNUID = @"campaignuid";

		//SUB BATCHES
		internal static string QRY_SUBBATCHES_TEMPLATE = @"SELECT * FROM [pibatch].[pisubbatch] WHERE (endtime is null OR endtime BETWEEN '{0}' AND '*') AND (starttime BETWEEN '01/01/1970 00:00:00' AND '{1}') AND unitbatchuid IN({2})";
		internal static string PISUBBATCH_FLD_UID = @"uid";
		internal static string PISUBBATCH_FLD_UNITBATCHUID = @"unitbatchuid";
		internal static string PISUBBATCH_FLD_PATH = @"path";
		internal static string PISUBBATCH_FLD_NAME = @"name";
		internal static string PISUBBATCH_FLD_LEVEL = @"level";
		internal static string PISUBBATCH_FLD_CHILDCOUNT = @"childcount";
		internal static string PISUBBATCH_FLD_STARTTIME = @"starttime";
		internal static string PISUBBATCH_FLD_ENDTIME = @"endtime";
		internal static string PISUBBATCH_FLD_HEADINGUID = @"headinguid";
		internal static string PISUBBATCH_FLD_PARENTUID = @"parentuid";
		
		//TAGS
		internal static string QRY_TAGS_TEMPLATE = @"SELECT * FROM [piarchive].[picomp] WHERE tag IN({0}) AND time BETWEEN '{1}' AND '{2}'";
		internal static string PICOMP_FLD_TAGNAME = @"tag";
		internal static string PICOMP_FLD_TIME = @"time";
		internal static string PICOMP_FLD_VALUE = @"value";
		internal static string PICOMP_FLD_SVALUE = @"svalue";
		internal static string PICOMP_FLD_STATUS = @"status";
		internal static string PICOMP_FLD_FLAGS = @"flags";
	}

	public class PIOLEDBReader : PIReaderInterface {

		private string dateFormat;
		private string dateFormatPI;
		private string piServerName;
		private string timeSeparator;
		private string fieldSeparator;
		private string valueSeparator;

		private uint lastReadRecordCount;

		private OleDbConnection cnn;
		
		public PIOLEDBReader(string piServerName, string outDateFormat, string dateFormatPI, string timeSeparator, string fieldSeparator, string valueSeparator) {
			this.dateFormat = outDateFormat;
			this.dateFormatPI = dateFormatPI;
			this.piServerName = piServerName;
			this.timeSeparator = timeSeparator;
			this.fieldSeparator = fieldSeparator;
			this.valueSeparator = valueSeparator;
		}

		public uint GetLastReadRecordCount() {
			return lastReadRecordCount;
		}

		public PIData Read(string tagListCsvString, string phaseTagListCsvString, DateTime startTime, DateTime endTime) {

			lastReadRecordCount = 0;

			SortedSet<string> tagNameList = new SortedSet<string>();
			if (null != tagListCsvString && tagListCsvString.Length > 0) {
				tagNameList = new SortedSet<string>(tagListCsvString.Split(','));
			}
			SortedSet<string> phaseTagNameList = new SortedSet<string>();
			if (null != phaseTagListCsvString && phaseTagListCsvString.Length > 0) {
				phaseTagNameList = new SortedSet<string>(phaseTagListCsvString.Split(','));
			}

			List<Tag> tagList = ReadTags(tagNameList, startTime, endTime, false);
			List<Tag> phaseTagList = ReadTags(phaseTagNameList, startTime, endTime, true);

			PIData piData = new PIData();
			piData.tags.AddRange(tagList);
			piData.tags.AddRange(phaseTagList);
			piData.readIntervalStart = startTime.ToString(dateFormat);
			piData.readIntervalEnd = endTime.ToString(dateFormat);
			piData.readFinished = endTime.ToString(dateFormat);
			piData.timeSeparator = timeSeparator;
			piData.fieldSeparator = fieldSeparator;
			piData.valueSeparator = valueSeparator;

			return piData;
		}

		/*
		 * Unit batches are used as entry points for this method.
		 * Given a module, the method first identifies unit batches matching the start and end dates. 
		 * Then it reconstructs the batch and sub batch hierarchy.
		 * Any unit batch which has an end time on or after the search start and a start time on or 
		 * before search end matches the search time criteria.
		 */
		public PIData ReadBatchTree(DateTime startTime, DateTime endTime, string modulePath) {
			string uid = getModuleUidFromPath(modulePath);
			if (null == uid) {
				throw new Exception(String.Format("Unable to find moduleuid from module path: {0}", modulePath));
			}

			Dictionary<string, UnitBatch> unitBatchDict = getUnitBatches(uid, startTime, endTime);
			Dictionary<string, Batch> batchDict = getBatches(unitBatchDict);
			Dictionary<string, SubBatch> subBatchDict = getSubBatches(unitBatchDict, startTime, endTime);

			PIData piData = makePIDataStructure(unitBatchDict, batchDict, subBatchDict);
			piData.readIntervalStart = startTime.ToString(dateFormat);
			piData.readIntervalEnd = endTime.ToString(dateFormat);
			piData.readFinished = endTime.ToString(dateFormat);

			return piData;
		}

		private PIData makePIDataStructure(Dictionary<string, UnitBatch> unitBatchDict, Dictionary<string, Batch> batchDict, Dictionary<string, SubBatch> subBatchDict) {
			PIData piData = new PIData();

			//1. build sub batch hierarchy and assign top level sub batches to unit bathches
			foreach (SubBatch sb in subBatchDict.Values) {
				if (null != sb.parentuid && 0 != sb.parentuid.Length) {
					SubBatch parentSubBatch = subBatchDict[sb.parentuid];
					parentSubBatch.subBatches.Add(sb);
				}
				if (0 == sb.level && null != sb.unitbatchuid) {
					UnitBatch parentUnitBatch = unitBatchDict[sb.unitbatchuid];
					parentUnitBatch.subBatches.Add(sb);
				}
			}

			//2. make dummy batch list for unit batches that do not have a parent batch
			List<Batch> dummyBatchList = new List<Batch>();

			//3. assign unit batches to batches
			foreach (UnitBatch uBatch in unitBatchDict.Values) {
				Batch batch = null;
				if (null == uBatch.batchuid || 0 == uBatch.batchuid.Length) {
					batch = new Batch();
					string str = "dummy for unit batch " + uBatch.uid;
					batch.uid = str;
					batch.batchid = str;
					batch.unitBatches.Add(uBatch);
					dummyBatchList.Add(batch);
				} else {
					batch = batchDict[uBatch.batchuid];
					batch.unitBatches.Add(uBatch);
				}
			}

			foreach(Batch b in dummyBatchList) {
				piData.batches.Add(b);
			}
			foreach(Batch b in batchDict.Values) {
				piData.batches.Add(b);
			}
			
			return piData;
		}

		#region Utility methods to read batch information from PI database

		private Dictionary<string, Batch> getBatches(Dictionary<string, UnitBatch> unitBatchDict) {
			Dictionary<string, Batch> batchDict = new Dictionary<string, Batch>();

			SortedSet<string> batchUids = new SortedSet<string>();
			foreach (UnitBatch uBatch in unitBatchDict.Values) {
				if (null != uBatch.batchuid && 0 != uBatch.batchuid.Length) { 
					batchUids.Add(uBatch.batchuid);
				}
			}
			StringBuilder batchUidQuotedCsv = new StringBuilder();
			foreach(string batchUid in batchUids) {
				batchUidQuotedCsv.AppendFormat("'{0}',", batchUid);
			}
			if (0 == batchUidQuotedCsv.Length) {
				return batchDict;
			}
			batchUidQuotedCsv.Remove(batchUidQuotedCsv.Length - 1, 1);
			
			string qry = string.Format(PIOLEDBSQL.QRY_BATCHES_TEMPLATE, batchUidQuotedCsv.ToString());
			connect();

			OleDbDataAdapter dataAdapter = new OleDbDataAdapter(qry, cnn);
			DataTable dataTable = new DataTable();
			dataAdapter.Fill(dataTable);
			foreach (DataRow row in dataTable.Rows) {
				Batch batch = buildBatch(row);
				batchDict.Add(batch.uid, batch);
			}

			return batchDict;
		}

		private Dictionary<string, UnitBatch> getUnitBatches(string moduleUid, DateTime startTime, DateTime endTime) {
			Dictionary<string, UnitBatch> unitBatchDict = new Dictionary<string, UnitBatch>();

			string startDateStr = startTime.ToString(dateFormatPI);
			string endDateStr = endTime.ToString(dateFormatPI);

			string qry = string.Format(PIOLEDBSQL.QRY_UNITBATCHES_TEMPLATE, startDateStr, endDateStr, moduleUid);
			connect();

			OleDbDataAdapter dataAdapter = new OleDbDataAdapter(qry, cnn);
			DataTable dataTable = new DataTable();
			dataAdapter.Fill(dataTable);
			foreach(DataRow row in dataTable.Rows) {
				UnitBatch uBatch = buildUnitBatch(row);
				unitBatchDict.Add(uBatch.uid, uBatch);
			}
			return unitBatchDict;
		}

		private Dictionary<string, SubBatch> getSubBatches(Dictionary<string, UnitBatch> unitBatchDict, DateTime startTime, DateTime endTime) {
			Dictionary<string, SubBatch> subBatchDict = new Dictionary<string, SubBatch>();

			SortedSet<string> uBatchUids = new SortedSet<string>();
			foreach(UnitBatch uBatch in unitBatchDict.Values) {
				if(null != uBatch.uid && 0 != uBatch.uid.Length) {
					uBatchUids.Add(uBatch.uid);
				}
			}
			StringBuilder uBatchUidQuotedCsv = new StringBuilder();
			foreach(string uBatchUid in uBatchUids) {
				uBatchUidQuotedCsv.AppendFormat("'{0}',", uBatchUid);
			}
			if (0 == uBatchUidQuotedCsv.Length) {
				return subBatchDict;
			}
			uBatchUidQuotedCsv.Remove(uBatchUidQuotedCsv.Length - 1, 1);

			string startDateStr = startTime.ToString(dateFormatPI);
			string endDateStr = endTime.ToString(dateFormatPI);

			string qry = string.Format(PIOLEDBSQL.QRY_SUBBATCHES_TEMPLATE, startDateStr, endDateStr, uBatchUidQuotedCsv);
			connect();

			OleDbDataAdapter dataAdapter = new OleDbDataAdapter(qry, cnn);
			DataTable dataTable = new DataTable();
			dataAdapter.Fill(dataTable);
			foreach (DataRow row in dataTable.Rows) {
				SubBatch subBatch = buildSubBatch(row);
				subBatchDict.Add(subBatch.uid, subBatch);
			}

			return subBatchDict;
		}

		private string getModuleUidFromPath(string moduleFullPath) {
			
			int lastBackslash = moduleFullPath.LastIndexOf('\\');
			string modulePath = moduleFullPath.Substring(0, lastBackslash+1);
			string moduleName = moduleFullPath.Substring(lastBackslash+1);

			string qry = string.Format(PIOLEDBSQL.QRY_MODULEUID_TEMPLATE, modulePath, moduleName);
			connect();

			OleDbDataAdapter dataAdapter = new OleDbDataAdapter(qry, cnn);
			DataTable dataTable = new DataTable();
			dataAdapter.Fill(dataTable);

			string moduleUid = null;
			if (dataTable.Rows.Count > 0) {
				DataRow row = dataTable.Rows[0];
				moduleUid = row[0].ToString();
			}
			
			return moduleUid;
		}

		#endregion

		#region Utility methods to build unit batches, batches and subbatches from database rows

		private Batch buildBatch(DataRow row) {
			Batch batch = new Batch();
			//uid, batchid, starttime, endtime, product, recipe, campaignuid
			batch.uid = row[PIOLEDBSQL.PIBATCH_FLD_UID].ToString();
			batch.batchid = row[PIOLEDBSQL.PIBATCH_FLD_BATCHID].ToString();
			DateTime dtStart = (DateTime)row[PIOLEDBSQL.PIBATCH_FLD_STARTTIME];
			batch.starttime = dtStart.ToString(dateFormat);

			try {
				//returned unit batches may have an empty end time (batch still ongoing): 
				//next statement will throw an InvalidCastException. 
				//We can ignore the exception.
				DateTime dtEnd = (DateTime)row[PIOLEDBSQL.PIBATCH_FLD_ENDTIME];
				batch.endtime = dtEnd.ToString(dateFormat);
			} catch (InvalidCastException) { }

			batch.product = row[PIOLEDBSQL.PIBATCH_FLD_PRODUCT].ToString();
			batch.recipe = row[PIOLEDBSQL.PIBATCH_FLD_RECIPE].ToString();
			batch.campaignuid = row[PIOLEDBSQL.PIBATCH_FLD_CAMPAIGNUID].ToString();
			
			return batch;
		}

		private UnitBatch buildUnitBatch(DataRow row) {
			UnitBatch uBatch = new UnitBatch();
			uBatch.uid = row[PIOLEDBSQL.PIUNITBATCH_FLD_UID].ToString();
			uBatch.batchid = row[PIOLEDBSQL.PIUNITBATCH_FLD_BATCHID].ToString();
			DateTime dtStart = (DateTime)row[PIOLEDBSQL.PIUNITBATCH_FLD_STARTTIME];
			uBatch.starttime = dtStart.ToString(dateFormat);

			try {
				//returned unit batches may have an empty end time (unit batch still ongoing): 
				//next statement will throw an InvalidCastException. 
				//We can ignore the exception.
				DateTime dtEnd = (DateTime)row[PIOLEDBSQL.PIUNITBATCH_FLD_ENDTIME];
				uBatch.endtime = dtEnd.ToString(dateFormat);
			} catch(InvalidCastException) {	}

			uBatch.product = row[PIOLEDBSQL.PIUNITBATCH_FLD_PRODUCT].ToString();
			uBatch.procedure = row[PIOLEDBSQL.PIUNITBATCH_FLD_PROCEDURE].ToString();
			uBatch.moduleuid = row[PIOLEDBSQL.PIUNITBATCH_FLD_MODULEUID].ToString();
			uBatch.batchuid = row[PIOLEDBSQL.PIUNITBATCH_FLD_BATCHUID].ToString();

			return uBatch;
		}

		private SubBatch buildSubBatch(DataRow row) {
			SubBatch subBatch = new SubBatch();

			subBatch.uid = row[PIOLEDBSQL.PISUBBATCH_FLD_UID].ToString();
			subBatch.unitbatchuid = row[PIOLEDBSQL.PISUBBATCH_FLD_UNITBATCHUID].ToString();
			subBatch.path = row[PIOLEDBSQL.PISUBBATCH_FLD_PATH].ToString();
			subBatch.name = row[PIOLEDBSQL.PISUBBATCH_FLD_NAME].ToString();
			subBatch.level = (int)row[PIOLEDBSQL.PISUBBATCH_FLD_LEVEL];
			subBatch.childcount = (int)row[PIOLEDBSQL.PISUBBATCH_FLD_CHILDCOUNT];

			DateTime dtStart = (DateTime)row[PIOLEDBSQL.PISUBBATCH_FLD_STARTTIME];
			subBatch.starttime = dtStart.ToString(dateFormat);

			try {
				//returned sub batches may have an empty end time (unit batch still ongoing): 
				//next statement will throw an InvalidCastException. 
				//We can ignore the exception.
				DateTime dtEnd = (DateTime)row[PIOLEDBSQL.PISUBBATCH_FLD_ENDTIME];
				subBatch.endtime = dtEnd.ToString(dateFormat);
			} catch (InvalidCastException) { }
			subBatch.headinguid = row[PIOLEDBSQL.PISUBBATCH_FLD_HEADINGUID].ToString();
			subBatch.parentuid = row[PIOLEDBSQL.PISUBBATCH_FLD_PARENTUID].ToString();

			return subBatch;
		}

		#endregion

		#region Utility methods to read tags

		private List<Tag> ReadTags(SortedSet<string> tagNameList, DateTime startTime, DateTime endTime, bool phaseTags) {
			List<Tag> tagList = new List<Tag>();

			if (0 == tagNameList.Count) {
				return tagList;
			}

			string startTimeStr = startTime.ToString(dateFormatPI);
			string endTimeStr = endTime.ToString(dateFormatPI);
			string qry = string.Format(PIOLEDBSQL.QRY_TAGS_TEMPLATE, Utils.stringList2QuotedCsv(tagNameList), startTimeStr, endTimeStr);
			
			connect();

			OleDbDataAdapter dataAdapter = new OleDbDataAdapter(qry, cnn);
			DataTable dataTable = new DataTable();
			dataAdapter.Fill(dataTable);

			lastReadRecordCount = lastReadRecordCount + (uint)(dataTable.Rows.Count);

			foreach (DataRow row in dataTable.Rows) {
				addTagRow(tagList, row, phaseTags);
			}

			return tagList;
		}
		
		private void addTagRow(List<Tag> tagList, DataRow row, bool isPhaseTag) {
			string tagName = row[PIOLEDBSQL.PICOMP_FLD_TAGNAME].ToString();

			Tag tag = findTagByName(tagList, tagName);
			if (null == tag) {
				tag = new Tag();
				tag.name = tagName;
				tag.setIsPhaseTag(isPhaseTag);
				tagList.Add(tag);
			}

			//send all values, separated by '|'
			string tagSvalue = row[PIOLEDBSQL.PICOMP_FLD_SVALUE].ToString();
			string tagValue = row[PIOLEDBSQL.PICOMP_FLD_VALUE].ToString();
			string tagStatus = row[PIOLEDBSQL.PICOMP_FLD_STATUS].ToString();

			DateTime dt = (DateTime)row[PIOLEDBSQL.PICOMP_FLD_TIME];
			if (tagSvalue.Length != 0) {
				tag.valueType = typeof(string);
			} else {
				if (isPhaseTag) {
					tag.valueType = row[PIOLEDBSQL.PICOMP_FLD_STATUS].GetType();
				} else { 
					tag.valueType = row[PIOLEDBSQL.PICOMP_FLD_VALUE].GetType();
				}
			}
			
			tag.addTimedTriple(dt.ToString(dateFormat), tagValue, tagSvalue, tagStatus, timeSeparator, fieldSeparator, valueSeparator);

		}

		private Tag findTagByName(List<Tag> tagList, string tagName) {
			foreach(Tag t in tagList) {
				if (t.name.Equals(tagName)) {
					return t;
				}
			}
			return null;
		}

		#endregion

		#region OleDbConnection
		private void connect() {
			if (null == cnn) {
				string connectionString = "Provider = PIOLEDB; " + "Data Source = " + piServerName;
				try {
					cnn = new OleDbConnection(connectionString);
				} catch (Exception) {
					cnn = null;
				}
			}

			if (cnn.State == ConnectionState.Broken) {
				cnn.Close();
			}
			if (cnn.State == ConnectionState.Closed) {
				cnn.Open();
			}
		}

		private void disconnect() {
			try {
				cnn.Close();
			} catch (Exception) { }
		}
		#endregion
	}
}
