using PISDK;
using PISDKCommon;
using PITimeServer;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace PIDataReaderLib {
	public class PISDKReader : PIReaderInterface {
		string dateFormat;

		string dateFormatPI;
		PISDK.PISDK piSDK;
		PISDK.Server piServer;
		long lastReadRecordCount;

		//DateTime readFinishedTimestamp;

		public PISDKReader(string piServerName, string outDateFormat, string dateFormatPI) {
			this.dateFormat = outDateFormat;
			this.dateFormatPI = dateFormatPI;
			this.piSDK = new PISDK.PISDK();
			this.piServer = this.piSDK.Servers[piServerName];
		}

		/*
		public DateTime getReadFinishedTime() {
			return readFinishedTimestamp;
		}
		*/

		public long GetLastReadRecordCount() {
			return lastReadRecordCount;
		}

		public PIData Read(string tagListCsvString, string phaseTagListCsvString, DateTime startTime, DateTime endTime) {
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
			return piData;
		}

		private List<Tag> ReadTags(SortedSet<string> tagNameList, DateTime startTime, DateTime endTime, bool phaseTags) {
			PISDK.PIPoint piPoint = default(PISDK.PIPoint);
			List<Tag> tagList = new List<Tag>();

			foreach (string tagName in tagNameList) {
				try {
					piPoint = piServer.PIPoints[tagName];
					PIValues lValues = piPoint.Data.RecordedValues(startTime, endTime);
					if (lValues.Count > 0) {
						Tag outTag = new Tag();
						outTag.name = piPoint.Name;
						outTag.setIsPhaseTag(phaseTags);
						outTag.hasStringValues = (lValues[1].Value.GetType() == typeof(string));
						StringBuilder tagValues = new StringBuilder();
						//PIValue collection index is 1-based!!!
						for (int i = 1; i <= lValues.Count; i++) {
							PIValue piValue = lValues[i];
							string tVal = piValue.Value.ToString();
							if (phaseTags) {
								tVal = piValue.Value.Name.ToString();
							}
							tagValues.AppendFormat("{0}:{1},", piValue.TimeStamp.LocalDate.ToString(dateFormat), tVal);
							lastReadRecordCount = lastReadRecordCount + 1;
						}
						tagValues.Remove(tagValues.Length - 1, 1);
						outTag.tagvalues = tagValues.ToString();
						tagList.Add(outTag);
					}
				} catch (Exception ex) {
					throw new Exception("Unable to read values for tag " + tagName + ". Details: " + ex.Message);
				}
			}
			return tagList;
		}
		
		public PIModule getModuleFromPath(string modulePath) {
			return PIModuleIdentifier.getModuleFromPath(modulePath, piServer);
		}

		/*
		 * Unit batches are used as entry points for this method.
		 * Given a module, the method first identifies unit batches matching the start and end dates. 
		 * Then it reconstructs the batch and sub batch hierarchy.
		 * Any unit batch which has an end time on or after the search start and a start time on or 
		 * before search end matches the search time criteria.
		 */
		public PIData ReadBatchTree(DateTime startTime, DateTime endTime, string modulePath, bool readBatches, bool readUnitBatches, bool readSubBatches, bool readPhases) {
			PIModule piModule = getModuleFromPath(modulePath);

			PITime piTimeStart = new PITime();
			piTimeStart.LocalDate = startTime;

			PITime piTimeEnd = new PITime();
			piTimeEnd.LocalDate = endTime;

			PIUnitBatchList piUnitBatchList = piModule.PIUnitBatchSearch(piTimeStart, piTimeEnd);
			List<UnitBatch> unitBatchList = new List<UnitBatch>();
			Dictionary<string, Batch> batchMap = new Dictionary<string, Batch>();
			PIData pidata = new PIData();
			pidata.readIntervalStart = startTime.ToString(dateFormat);
			pidata.readIntervalEnd = endTime.ToString(dateFormat);

			int c = piUnitBatchList.Count;
			EntityBuilder eb = new EntityBuilder(dateFormat);
			foreach (PIUnitBatch piUnitBatch in piUnitBatchList) {
				UnitBatch unitBatch = eb.buildUnitBatch(piUnitBatch, piModule.UniqueID);
				
				Batch batch = null;
				if (null == piUnitBatch.PIBatch) {
					//build a dummy batch
					batch = new Batch();
					pidata.batches.Add(batch);
				} else {
					string uid = piUnitBatch.PIBatch.UniqueID;
					if (!batchMap.ContainsKey(piUnitBatch.PIBatch.UniqueID)) {
						batch = eb.buildBatch(piUnitBatch.PIBatch);
						batchMap.Add(batch.uid, batch);
						pidata.batches.Add(batch);
					} else {
						batch = batchMap[piUnitBatch.PIBatch.UniqueID];
					}
				}
				batch.unitBatches.Add(unitBatch);
			}
			//readFinishedTimestamp = DateTime.Now;
			pidata.readIntervalStart = startTime.ToString(dateFormat);
			pidata.readIntervalEnd = endTime.ToString(dateFormat);
			pidata.readFinished = endTime.ToString(dateFormat);
			return pidata;
		}

	}
}
