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

			/*
			if (null != startDateStr) {
				DateTime startDate = DateTime.ParseExact(startDateStr, dateFormat, CultureInfo.InvariantCulture);
				DateTime bStartDate = DateTime.ParseExact(unitBatch.starttime, dateFormat, CultureInfo.InvariantCulture);
				if (null != endDateStr) {
					//both input dates NOT NULL

					DateTime bEndDate = DateTime.ParseExact(unitBatch.endtime, dateFormat, CultureInfo.InvariantCulture);

					if (DateTime.Compare(startDate, bStartDate) <= 0 && DateTime.Compare(endDate, bEndDate) >= 0) {
						//unit batch start is after start date, unit batch end is before end date
						return true;
					} else {
						return false;
					}

				} else {
					//end date NULL

					if (DateTime.Compare(startDate, bStartDate) <= 0) {
						//unit batch start is after start date
						return true;
					} else {
						return false;
					}
				}
			} else {
				//start date NULL
				if (null != endDate) {
					DateTime bEndDate = DateTime.ParseExact(unitBatch.endtime, dateFormat, CultureInfo.InvariantCulture);
					if (DateTime.Compare(endDate, bEndDate) >= 0) {
						return true;
					} else {
						return false;
					}
				} else {
					//both dates NULL
					return true;
				}
			}
			*/
			return true;
		}

		
	}
}
