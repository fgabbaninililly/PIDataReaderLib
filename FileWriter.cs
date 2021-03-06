﻿using NLog;
using System;
using System.Collections.Generic;

namespace PIDataReaderLib {
	public class FileWriter {
		private static Logger logger = LogManager.GetCurrentClassLogger();

		private string outFolder;
		private string inDateFormat;
		private string outDateFormat;
		private string timeValueSeparator;
		private string recordSeparator;
		private string fieldSeparator;

		public FileWriter(string outFolder, string inDateFormat, string outDateFormat, string timeValueSeparator, string recordSeparator, string fieldSeparator) {
			this.outFolder = outFolder;
			this.inDateFormat = inDateFormat;
			this.outDateFormat = outDateFormat;
			this.timeValueSeparator = timeValueSeparator;
			this.recordSeparator = recordSeparator;
			this.fieldSeparator = fieldSeparator;
		}
		
		private void createFolder(string pathToFolder) {
			bool exists = System.IO.Directory.Exists(pathToFolder);
			if (!exists)
				System.IO.Directory.CreateDirectory(pathToFolder);
		}

		public void writeTags(PIData piData, string equipmentName, bool append) {
			TagSerializer tagSerializer = new TagSerializer(inDateFormat, outDateFormat, timeValueSeparator, recordSeparator, fieldSeparator);
			
			try {
				string destFolder = String.Format(@"{0}\Out\Tags\", outFolder);
				createFolder(destFolder);
				string destFile = String.Format(@"{0}\{1}.tag.txt", destFolder, equipmentName);
				tagSerializer.createFileWithHeader(destFile, append);
				tagSerializer.serializeTagsToLocalFile(piData.tags, destFile);
			} catch (Exception e) {
				logger.Error(e.ToString());
			}
			
		}
		[System.Obsolete]
		public void writeTags(Dictionary<string, PIData> piDataMap, bool append) {
			TagSerializer tagSerializer = new TagSerializer(inDateFormat, outDateFormat, timeValueSeparator, recordSeparator, fieldSeparator);
			foreach (string equipmentName in piDataMap.Keys) {
				PIData piData = piDataMap[equipmentName];
				writeTags(piData, equipmentName, append);
			}
		}

		public void writeBatches(PIData piData, string moduleName, bool append) {
			try {
				string destFolder = String.Format(@"{0}\Out\Batches\", outFolder);
				createFolder(destFolder);
				string destFileBatch = String.Format(@"{0}\{1}.batch.txt", destFolder, moduleName);
				string destFileUBatch = String.Format(@"{0}\{1}.ubatch.txt", destFolder, moduleName);
				string destFileSBatch = String.Format(@"{0}\{1}.sbatch.txt", destFolder, moduleName);
				BatchSerializer batchSerializer = new BatchSerializer(destFileBatch, destFileUBatch, destFileSBatch, inDateFormat, outDateFormat);
				
				if (batchSerializer.createFilesWithHeader(append)) {
					batchSerializer.serializeBatchesToLocalFile(piData.batches);
				}
				batchSerializer.closeFiles();
			} catch (Exception e) {
				logger.Error(e.ToString());
			}
		}
		[System.Obsolete]
		public void writeBatches(Dictionary<string, PIData> piDataMap, bool append) {
			foreach (string moduleName in piDataMap.Keys) {
				PIData piData = piDataMap[moduleName];
				writeBatches(piData, moduleName, append);
			}
		}
	}
}
