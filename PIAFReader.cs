using OSIsoft.AF;
using OSIsoft.AF.Asset;
using OSIsoft.AF.Data;
using OSIsoft.AF.PI;
using OSIsoft.AF.Time;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace PIDataReaderLib {
	public class PIAFReader : PIReaderInterface {
		string dateFormat;
		string dateFormatPI;
		AFBoundaryType boundaryType;
		
		PIServer piServer;
		long lastReadRecordCount;
		
		DateTime firstValueTimestamp;
		DateTime lastValueTimestamp;
		//DateTime readFinishedTimestamp;

		public PIAFReader(string piServerName, string outDateFormat, string dateFormatPI) {
			this.dateFormat = outDateFormat;
			this.dateFormatPI = dateFormatPI;

			PIServers piServers = new PIServers();
			this.piServer = piServers[piServerName];
			boundaryType = AFBoundaryType.Interpolated;
		}

		/*
		public DateTime getReadFinishedTime() {
			return readFinishedTimestamp;
		}
		*/

		public string getSummary() {
			StringBuilder sb = new StringBuilder();
			sb.AppendFormat("Current user: {0}\n", piServer.CurrentUserName);
			AFIdentity identity = piServer.Identity;
			sb.AppendFormat("Current security identity: {0}\n", identity.ToString());
			return sb.ToString();
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
			List<Tag> tagList = ReadTags(tagNameList, startTime, endTime, boundaryType, false);
			List<Tag> phaseTagList = ReadTags(phaseTagNameList, startTime, endTime, boundaryType, true);
			
			PIData piData = new PIData();
			piData.tags.AddRange(tagList);
			piData.tags.AddRange(phaseTagList);
			piData.readIntervalStart = startTime.ToString(dateFormat);
			piData.readIntervalEnd = endTime.ToString(dateFormat);
			piData.readFinished = endTime.ToString(dateFormat);
			return piData;
		}

		public void setBoundaryType(string boundaryType) {
			if (Parameter.PARAM_VALUE_BOUNDARY_INSIDE.Equals(boundaryType.ToLower())) {
				setBoundaryType(OSIsoft.AF.Data.AFBoundaryType.Inside);
			} else if (Parameter.PARAM_VALUE_BOUNDARY_OUTSIDE.Equals(boundaryType.ToLower())) {
				setBoundaryType(OSIsoft.AF.Data.AFBoundaryType.Outside);
			} else if (Parameter.PARAM_VALUE_BOUNDARY_INTERPOLATED.Equals(boundaryType.ToLower())) { 
				setBoundaryType(OSIsoft.AF.Data.AFBoundaryType.Interpolated);
			} else {
				throw new Exception("Invalid boundary condition was specified. Valid alternatives are: inside, outside, interpolated. please check your configuration file.");
			}
		}

		public void setBoundaryType(AFBoundaryType bType) {
			boundaryType = bType;
		}

		private AFTimeRange SetupTimeRange(DateTime startTime, DateTime endTime) {
			AFTime afStartTime = new AFTime(startTime);
			AFTime afEndTime = new AFTime(endTime);
			AFTimeZoneFormatProvider prv = new AFTimeZoneFormatProvider(new AFTimeZone());
			TimeZone tz = TimeZone.CurrentTimeZone;

			string d1 = startTime.ToString(dateFormatPI);
			string d2 = endTime.ToString(dateFormatPI);

			return new AFTimeRange(d1, d2, prv);
		}

		public long GetLastReadRecordCount() {
			return lastReadRecordCount;
		}

		private List<Tag> ReadTags(SortedSet<string> tagNameList, DateTime startTime, DateTime endTime, AFBoundaryType boundaryType, bool phaseTags) {
			IList<PIPoint> points = OSIsoft.AF.PI.PIPoint.FindPIPoints(piServer, tagNameList.ToArray());
			PIPointList pointList = new PIPointList(points);
			AFTimeRange timeRange = SetupTimeRange(startTime, endTime);

			List<Tag> tagList = new List<Tag>();
			foreach (PIPoint pt in pointList) {
				try {
					StringBuilder sb = new StringBuilder();
					AFValues afVals = pt.RecordedValues(timeRange, boundaryType, "", false);
					foreach (AFValue afVal in afVals) {
						sb.AppendFormat("{0}:{1},", afVal.Timestamp.LocalTime.ToString(dateFormat), afVal.Value.ToString());
						lastReadRecordCount = lastReadRecordCount + 1;
					}
					if (sb.Length > 0) {
						sb.Remove(sb.Length - 1, 1);
					}
					if (afVals.Count > 0) {
						Tag tag = new Tag();
						tag.name = pt.Name;
						tag.setIsPhaseTag(phaseTags);
						tag.hasStringValues = (afVals[0].Value.GetType() == typeof(string));
						tag.tagvalues = sb.ToString();
						tagList.Add(tag);
					}
				} catch (Exception ex) {
					throw new Exception("Unable to read values for tag " + pt.Name + ". Details: " + ex.Message);
				}
			}
			return tagList;
		}

	}
}
