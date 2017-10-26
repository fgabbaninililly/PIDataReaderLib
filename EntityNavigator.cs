using System;
using System.Collections.Generic;
using System.Globalization;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace PIDataReaderLib {
	public class EntityNavigator {
		string dateFormat;
		public EntityNavigator(string dateFormat) {
			this.dateFormat = dateFormat;
		}

		public List<UnitBatch> findUnitBatches(PIData piData, string batchId, string product, string startDateStr, string endDateStr) {
			List<UnitBatch> unitBatchesList = new List<UnitBatch>();
			foreach (Batch batch in piData.batches) {
				foreach(UnitBatch unitBatch in batch.unitBatches) {
					if (matchesConditions(unitBatch, batchId, product, startDateStr, endDateStr)) {
						unitBatchesList.Add(unitBatch);
					}
				}
			}
			return unitBatchesList;
		}

		private bool matchesConditions(UnitBatch unitBatch, string batchId, string product, string startDateStr, string endDateStr) {
			if (null != batchId && batchId.Length > 0 && !batchId.Equals(unitBatch.batchid)) {
				return false;
			}
			if (null != product && product.Length > 0 && !product.Equals(unitBatch.product)) {
				return false;
			}
			return true;
		}

		
	}
}
