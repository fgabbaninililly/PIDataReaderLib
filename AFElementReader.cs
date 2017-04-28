using OSIsoft.AF;
using OSIsoft.AF.Asset;
using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace PIDataReaderLib {
	public class AFElementReader {
		private PISystem piSystem;
		private AFDatabase afDatabase;

		public delegate void AFReadTerminated(AFReadTerminatedEventArgs e);
		public event AFReadTerminated AFElementReader_ReadTerminated;

		public void init(string afServerName, string afDatabaseName) {
			PISystems piSystems = new PISystems();
			piSystem = piSystems[afServerName];

			if (null == piSystem) {
				throw new Exception("Failed to connect to AF Server. Server name: " + afServerName);
			}

			afDatabase = piSystem.Databases[afDatabaseName];
			if (null == afDatabase) {
				throw new Exception("Cannot connect to AF Database. Database name: " + afDatabaseName);
			}
		}

		public AFData read(List<string> elementPaths) {
			AFData afData = new AFData();
			Stopwatch swatch = Stopwatch.StartNew();
			AFKeyedResults<string, AFElement> elementsMap = AFElement.FindElementsByPath(elementPaths, null);
			swatch.Stop();

			foreach (AFElement ele in elementsMap.Results.Values) {
				DIAAFElement diaEle = AFElementBuilder.build(ele);
				afData.afelements.Add(diaEle);
			}
			AFReadTerminatedEventArgs ea = new AFReadTerminatedEventArgs(swatch.Elapsed.TotalSeconds, elementPaths.Count);
			AFElementReader_ReadTerminated(ea);
			return afData;
		}
	}

	public class AFReadTerminatedEventArgs : EventArgs {
		public AFReadTerminatedEventArgs(double elapsedTimeSec, int elementCount) {
			this.elapsedTimeSec = elapsedTimeSec;
			this.elementCount = elementCount;
		}
		public double elapsedTimeSec;
		public int elementCount;
	}
}
