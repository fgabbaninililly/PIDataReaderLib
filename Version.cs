using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace PIDataReaderLib {
	/*
	 * Changelog
	 *  
	 * 
	 * */

	public class Version {
		public static readonly string version = "2.3.0";
		public static string getVersion() {
			return String.Format("Using PIDataReaderLib v{0}.", version);
		}
	}
}
