using PISDK;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace PIDataReaderLib {
	class EntityBuilder {
		string dateFormat;
		uint recordCount;

		public EntityBuilder(string dateFormat) {
			this.dateFormat = dateFormat;
		}

		public UnitBatch buildUnitBatch(PIUnitBatch piUnitBatch, string moduleUid) {
			recordCount = 0;
			UnitBatch unitBatch = buildUnitBatch(piUnitBatch);
			unitBatch.moduleuid = moduleUid;
			unitBatch.subBatches = buildSubBatches(piUnitBatch);
			return unitBatch;
		}

		public uint getRecordCount() {
			return recordCount;
		}

		public Batch buildBatch(PIBatch piBatch) {
			
			Batch batch = new Batch();

			batch.batchid = piBatch.BatchID;
			batch.starttime = piBatch.StartTime.LocalDate.ToString(dateFormat);
			if (null != piBatch.EndTime) {
				//end time may be not available
				batch.endtime = piBatch.EndTime.LocalDate.ToString(dateFormat);
			} else {
				batch.endtime = "";
			}
			batch.product = piBatch.Product;
			batch.recipe = piBatch.Recipe;
			batch.uid = piBatch.UniqueID;

			recordCount = 1;

			return batch;
		}

		private UnitBatch buildUnitBatch(PIUnitBatch piUnitBatch) {
			UnitBatch unitBatch = new UnitBatch();
			
			unitBatch.batchid = piUnitBatch.BatchID;
			unitBatch.starttime = piUnitBatch.StartTime.LocalDate.ToString(dateFormat);
			if (null != piUnitBatch.EndTime) {
				//end time may be not available
				unitBatch.endtime = piUnitBatch.EndTime.LocalDate.ToString(dateFormat);
			} else {
				unitBatch.endtime = "";
			}
			unitBatch.procedure = piUnitBatch.ProcedureName;
			unitBatch.product = piUnitBatch.Product;
			unitBatch.uid = piUnitBatch.UniqueID;

			recordCount++;

			return unitBatch;
		}

		private List<SubBatch> buildSubBatches(PIUnitBatch piUnitBatch) {
			List<SubBatch> subBatches = new List<SubBatch>();
			foreach(PISubBatch piSubBatch in piUnitBatch.PISubBatches) {
				SubBatch subBatch = buildSubBatch(piSubBatch);
				subBatches.Add(subBatch);
			}
			return subBatches;
		}

		private SubBatch buildSubBatch(PISubBatch piSubBatch) {
			SubBatch subBatch = createSubBatchFromPISubBatch(piSubBatch);
			recurseSubBatch(piSubBatch, subBatch);
			return subBatch;
		}

		private SubBatch createSubBatchFromPISubBatch(PISubBatch piSubBatch) {
			SubBatch subBatch = new SubBatch();

			subBatch.starttime = piSubBatch.StartTime.LocalDate.ToString(dateFormat);
			if (null != piSubBatch.EndTime) {
				//end time may be not available
				subBatch.endtime = piSubBatch.EndTime.LocalDate.ToString(dateFormat);
			} else {
				subBatch.endtime = "";
			}

			subBatch.name = piSubBatch.Name;
			if (null != piSubBatch.PIHeading) {
				subBatch.headinguid = piSubBatch.PIHeading.UniqueID;
			}
			subBatch.uid = piSubBatch.UniqueID;

			recordCount++;

			return subBatch;
		}

		private void recurseSubBatch(PISubBatch piSubBatch, SubBatch subBatch) {
			if (null == piSubBatch.PISubBatches || 0 == piSubBatch.PISubBatches.Count) {
				return;
			}

			foreach(PISubBatch piSubBatchChld in piSubBatch.PISubBatches) {
				SubBatch subBatchChild = createSubBatchFromPISubBatch(piSubBatchChld);
				subBatch.subBatches.Add(subBatchChild);
				recurseSubBatch(piSubBatchChld, subBatchChild);
			}

			return;
		}

		/*
		private SubBatchItem buildSubBatchItem(PISubBatch piSubBatchItem) {
			SubBatchItem subBatchItem = new SubBatchItem();
			subBatchItem.starttime = piSubBatchItem.StartTime.LocalDate.ToString(dateFormat);
			if (null != piSubBatchItem.EndTime) {
				//end time may be not available
				subBatchItem.endtime = piSubBatchItem.EndTime.LocalDate.ToString(dateFormat);
			} else {
				subBatchItem.endtime = "";
			}
			subBatchItem.name = piSubBatchItem.Name;
			subBatchItem.uid = piSubBatchItem.UniqueID;
			return subBatchItem;
		}
		*/

	}
}
