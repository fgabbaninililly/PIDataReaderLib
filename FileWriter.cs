using NLog;
using System;
using System.Collections.Generic;

namespace PIDataReaderLib {
	public class FileWriter {
		private static Logger logger = LogManager.GetCurrentClassLogger();

		private string outFolder;
		private string inDateFormat;
		private string outDateFormat;

		public FileWriter(string outFolder, string inDateFormat, string outDateFormat) {
			this.outFolder = outFolder;
			this.inDateFormat = inDateFormat;
			this.outDateFormat = outDateFormat;
		}
		
		private void createFolder(string pathToFolder) {
			bool exists = System.IO.Directory.Exists(pathToFolder);
			if (!exists)
				System.IO.Directory.CreateDirectory(pathToFolder);
		}

		public void writeTags(Dictionary<string, PIData> piDataMap, bool append) {
			TagSerializer tagSerializer = new TagSerializer(inDateFormat, outDateFormat);
			foreach (string equipmentName in piDataMap.Keys) {
				try { 
					PIData piData = piDataMap[equipmentName];
					string destFolder = String.Format(@"{0}\Out\Tags\", outFolder);
					createFolder(destFolder);
					string destFile = String.Format(@"{0}\{1}.tag.txt", destFolder, equipmentName);
					tagSerializer.createFileWithHeader(destFile, append);
					tagSerializer.serializeTagsToLocalFile(piData.tags, destFile);
				} catch (Exception e) {
					logger.Error(e.ToString());
				}
			}
		}

		public void writeBatches(Dictionary<string, PIData> piDataMap, bool append) {
			foreach (string moduleName in piDataMap.Keys) {
				try { 
					PIData piData = piDataMap[moduleName];
					string destFolder = String.Format(@"{0}\Out\Batches\", outFolder);
					createFolder(destFolder);
					string destFileBatch = String.Format(@"{0}\{1}.batch.txt", destFolder, moduleName);
					string destFileUBatch = String.Format(@"{0}\{1}.ubatch.txt", destFolder, moduleName);
					string destFileSBatch = String.Format(@"{0}\{1}.sbatch.txt", destFolder, moduleName);
					BatchSerializer batchSerializer = new BatchSerializer(destFileBatch, destFileUBatch, destFileSBatch, inDateFormat, outDateFormat);
					bool b = batchSerializer.createFiles(append);
					batchSerializer.serializeBatchesToLocalFile(piData.batches);
					batchSerializer.closeFiles();
				} catch (Exception e) {
					logger.Error(e.ToString());
				}
			}
		}
	}
}
