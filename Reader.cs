using NLog;
using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Globalization;

namespace PIDataReaderLib {
	public class Reader {
		private static Logger logger = LogManager.GetCurrentClassLogger();

		private PIReaderInterface piReader;
		private string outDateFormat;
		private string piDateFormat;
		//private Dictionary<string, DateTime> startTimesByEquipment;
		private string piServerName;
		private string piSdkType;
		private string boundary = Parameter.PARAM_VALUE_BOUNDARY_INSIDE;
		private bool readBatchMode;

		public delegate void PIReadTerminated(PIReadTerminatedEventArgs e);
		public event PIReadTerminated Reader_PIReadTerminated;

		public Reader(
			string piServerName,
			string piSdkType,
			bool readBatchMode, 
			string outDateFormat, 
			string piDateFormat
			) {

			this.piServerName = piServerName;
			this.piSdkType = piSdkType;
			this.readBatchMode = readBatchMode;
			this.outDateFormat = outDateFormat;
			this.piDateFormat = piDateFormat;

			this.boundary = Parameter.PARAM_VALUE_BOUNDARY_INSIDE;
		}
		
		public void setBoundaryCondition(string bCondition) {
			if (!Parameter.PARAM_VALUE_BOUNDARY_INSIDE.Equals(bCondition.ToLower()) ||
				!Parameter.PARAM_VALUE_BOUNDARY_OUTSIDE.Equals(bCondition.ToLower()) ||
				!Parameter.PARAM_VALUE_BOUNDARY_INTERPOLATED.Equals(bCondition.ToLower())
				) {
				throw new Exception("Invalid boundary condition was specified. Valid alternatives are: inside, outside, interpolated. please check your configuration file.");
			}
			this.boundary = bCondition;
		}

		public void init() {
			logger.Info("Initializing reader...");
			try {
				if (piSdkType.ToLower().Equals(Parameter.PARAM_VALUE_SDK_OLEDB)) {
					//use PI OLEDB
					logger.Info("Using OLEDB");
					PIOLEDBReader piOLEDBReader = new PIOLEDBReader(piServerName, outDateFormat, piDateFormat);
					piReader = piOLEDBReader;
				} else if (piSdkType.ToLower().Equals(Parameter.PARAM_VALUE_SDK_AF)) {
					//use PI AF SDK
					logger.Info("Using PI AF SDK. Boundary condition: {0}", boundary);
					//output error message in case we are asked to read batches using AF SDK
					if (readBatchMode) {
						string str = String.Format("Cannot read batch information using PI AF SDK. Please specify {0} or {1} to read batch information.", Parameter.PARAM_VALUE_SDK_OLEDB, Parameter.PARAM_VALUE_SDK_PI);
						logger.Fatal(str);
						piReader = null;
						throw new Exception(str);
					} else {
						PIAFReader piAFReader = new PIAFReader(piServerName, outDateFormat, piDateFormat);
						piAFReader.setBoundaryType(boundary);
						piReader = piAFReader;
					}
				} else {
					//use PI SDK
					logger.Info("Using PI SDK");
					PISDKReader piSDKReader = new PISDKReader(piServerName, outDateFormat, piDateFormat);
					piReader = piSDKReader;
				}
				logger.Info("Reader successfully built. Connecting to PI server named {0}", piServerName);
			} catch (Exception ex) {
				string msg = "Fatal error while building reader.";
				logger.Fatal(msg);
				piReader = null;
				throw ex;
			}
		}
		
		public void dummyRead() {
			DateTime s = DateTime.Now;
			piReader.Read("sinusoid", "", s, s.AddSeconds(-1));
			logger.Info("Executed dummy read to improve performance of next read");
		}

		public Dictionary<string, PIData> readTags(PIReaderConfig config, Dictionary<string, ReadInterval> readIntervalsByEquipment) {
			logger.Info(">>Start reading data");
			Dictionary<string, PIData> piDataMap = new Dictionary<string, PIData>();
			Dictionary<string, double> readTimesMap = new Dictionary<string, double>();
			List<EquipmentCfg> equipments = config.read.equipments;
			foreach (EquipmentCfg equipment in equipments) {
				try {
					ReadInterval rInterval = readIntervalsByEquipment[equipment.name];
					logger.Info("Reading equipment '{0}'. Interval [{1}, {2}]", equipment.name, rInterval.start.ToString(config.dateFormats.reference), rInterval.end.ToString(config.dateFormats.reference));
					Stopwatch swatch = Stopwatch.StartNew();
					PIData piData = piReader.Read(equipment.tagList.tags, equipment.phaseList.phases, rInterval.start, rInterval.end);
					swatch.Stop();
					readTimesMap.Add(equipment.name, swatch.Elapsed.TotalSeconds);
					logger.Info("Finished reading equipment. Total tags: {0}. Total records: {1}. Time required: {2}s", piData.tags.Count, piReader.GetLastReadRecordCount(), swatch.Elapsed.TotalSeconds);
					piDataMap.Add(equipment.name, piData);
				} catch (Exception e) {
					logger.Error("Error reading tags. Details: {0}", e.ToString());
				}
			}
			logger.Info(">>Finished reading data");
			if (null != Reader_PIReadTerminated) {
				PIReadTerminatedEventArgs ea = new PIReadTerminatedEventArgs(readTimesMap);
				Reader_PIReadTerminated(ea);
			}

			return piDataMap;
		}
		
		public Dictionary<string, PIData> readBatches( PIReaderConfig config, Dictionary<string, ReadInterval> readIntervalsByEquipment) {

			logger.Info(">>Start reading data");

			Dictionary<string, PIData> piDataMap = new Dictionary<string, PIData>();
			Dictionary<string, double> readTimesMap = new Dictionary<string, double>();
			foreach (BatchCfg batchCfg in config.read.batches) {
				try {
					ReadInterval rInterval = readIntervalsByEquipment[batchCfg.moduleName];
					logger.Info("Reading module {0}. Interval [{1}, {2}]", batchCfg.moduleName, rInterval.start.ToString(config.dateFormats.reference), rInterval.end.ToString(config.dateFormats.reference));
					Stopwatch swatch = Stopwatch.StartNew();
					PIData piData = piReader.ReadBatchTree(rInterval.start, rInterval.end, batchCfg.modulePath);
					swatch.Stop();
					readTimesMap.Add(batchCfg.moduleName, swatch.Elapsed.TotalSeconds);
					logger.Info("Finished reading module");
					piDataMap.Add(batchCfg.moduleName, piData);
				} catch (Exception e) {
					logger.Error("Error reading batch/unit batch/sub batch. Details: {0}", e.ToString());
				}
			}
			logger.Info(">>Finished reading data");
			if (null != Reader_PIReadTerminated) {
				PIReadTerminatedEventArgs ea = new PIReadTerminatedEventArgs(readTimesMap);
				Reader_PIReadTerminated(ea);
			}
			return piDataMap;
		}
		
		private DateTime getStartTime(string equipmentName, Dictionary<string, DateTime> startTimesByEquipment, DateTime startTime, string readExtentType) {
			if (!ReadExtent.READ_EXTENT_FREQUENCY.Equals(readExtentType.ToLower())) {
				return startTime;
			}
			if (null == startTimesByEquipment || 0 == startTimesByEquipment.Count) {
				return startTime;
			}

			if (startTime > startTimesByEquipment[equipmentName]) {
				logger.Info("Start time overridden to {0}", startTimesByEquipment[equipmentName].ToString(outDateFormat));
				return startTimesByEquipment[equipmentName];
			}
			return startTime;
		}
	}

	public class PIReadTerminatedEventArgs {
		public PIReadTerminatedEventArgs(Dictionary<string, double> readTimesMap) {
			this.readTimesByEquipment = readTimesMap;
		}
		public Dictionary<string, double> readTimesByEquipment;
	}
}
