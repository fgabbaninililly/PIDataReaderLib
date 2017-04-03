using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace PIDataReaderLib {
	public interface PIReaderInterface {
		PIData Read(string tagListCsvString, string phaseTagListCsvString, DateTime startTime, DateTime endTime);
		long GetLastReadRecordCount();
		//DateTime getReadFinishedTime();
	}
	
}