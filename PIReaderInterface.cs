using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace PIDataReaderLib {
	public interface PIReaderInterface {
		PIData Read(string tagListCsvString, string phaseTagListCsvString, DateTime startTime, DateTime endTime);
		PIData ReadBatchTree(DateTime startTime, DateTime endTime, string modulePath);
		uint GetLastReadRecordCount();
		void ResetLastReadRecordCount();
		//DateTime getReadFinishedTime();
	}
	
}