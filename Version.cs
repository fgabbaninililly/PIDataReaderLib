﻿using System;
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
		public static readonly string version = "2.3.5";
		public static string getVersion() {
			return String.Format("PIDataReader Foundation Library v{0}.", version);
		}
	}
}
