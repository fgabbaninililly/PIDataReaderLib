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
		string timeSeparator;
		string fieldSeparator;
		string valueSeparator;

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
			string piDateFormat,
			string timeSeparator, 
			string fieldSeparator, 
			string valueSeparator
			) {

			this.piServerName = piServerName;
			this.piSdkType = piSdkType;
			this.readBatchMode = readBatchMode;
			this.outDateFormat = outDateFormat;
			this.piDateFormat = piDateFormat;

			this.timeSeparator = timeSeparator;
			this.fieldSeparator = fieldSeparator;
			this.valueSeparator = valueSeparator;

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
					PIOLEDBReader piOLEDBReader = new PIOLEDBReader(piServerName, outDateFormat, piDateFormat, timeSeparator, fieldSeparator, valueSeparator);
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
						PIAFReader piAFReader = new PIAFReader(piServerName, outDateFormat, piDateFormat, timeSeparator, fieldSeparator, valueSeparator);
						piAFReader.setBoundaryType(boundary);
						piReader = piAFReader;
					}
				} else {
					//use PI SDK
					logger.Info("Using PI SDK");
					PISDKReader piSDKReader = new PISDKReader(piServerName, outDateFormat, piDateFormat, timeSeparator, fieldSeparator, valueSeparator);
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
			try { 
				piReader.Read("sinusoid", "", s, s.AddSeconds(-1));
				logger.Info("Executed dummy read to improve performance of next read");
			} catch(Exception e) {
				logger.Error("Generic error in dummy read. Details: {0}", e.ToString());
			}
		}

		public PIData readTags(EquipmentCfg equipmentCfg, ReadInterval readInterval) {
			PIData piData = null;
			try {
				logger.Info("Reading '{0}'", equipmentCfg.name);
				Stopwatch swatch = Stopwatch.StartNew();
				piData = piReader.Read(equipmentCfg.tagList.tags, equipmentCfg.phaseList.phases, readInterval.start, readInterval.end);
				swatch.Stop();
				logger.Info("Finished reading {0} tags", piData.tags.Count);
				if (null != Reader_PIReadTerminated) {
					PIReadTerminatedEventArgs ea = new PIReadTerminatedEventArgs(piReader.GetLastReadRecordCount(), swatch.Elapsed.TotalSeconds);
					Reader_PIReadTerminated(ea);
				}
			} catch (System.Data.OleDb.OleDbException e) {
				logger.Error("Error connecting to PI. Details: {0}", e.ToString());
			} catch(System.TypeInitializationException e) {
				logger.Error("Error connecting to PI. Details: {0}", e.ToString());
			} catch (System.Runtime.InteropServices.COMException e) {
				logger.Error("Error connecting to PI. Details: {0}", e.ToString());
			} catch (Exception e) {
				logger.Error("Generic error reading tags. Details: {0}", e.ToString());
			}
			return piData;
		}

		public PIData readBatches(BatchCfg batchCfg, ReadInterval readInterval) {
			PIData piData = null;
			try {
				Stopwatch swatch = Stopwatch.StartNew();
				piData = piReader.ReadBatchTree(readInterval.start, readInterval.end, batchCfg.modulePath);
				swatch.Stop();
				if (null != Reader_PIReadTerminated) {
					PIReadTerminatedEventArgs ea = new PIReadTerminatedEventArgs(piReader.GetLastReadRecordCount(), swatch.Elapsed.TotalSeconds);
					Reader_PIReadTerminated(ea);
				}
			} catch (Exception e) {
				logger.Error("Error reading batch/unit batch/sub batch. Details: {0}", e.ToString());
			}
						
			return piData;
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
		public PIReadTerminatedEventArgs(uint recordCount, double elapsedTime) {
			this.recordCount = recordCount;
			this.elapsedTime = elapsedTime;
		}
		public ulong recordCount;
		public double elapsedTime;
	}
}
