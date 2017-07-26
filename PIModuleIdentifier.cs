using PISDK;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;


namespace PIDataReaderLib {
	class PIModuleIdentifier {
		public static PIModule getModuleFromPath(string path, PISDK.Server piSDKServerInstance) {
			char[] separator = {'\\'};

			if (path.StartsWith("\\")) {
				path = path.TrimStart('\\');
			}

			string[] modules = path.Split(separator);
			List<string> moduleList = new List<string>();
			moduleList.AddRange(modules);

			PIModule module = null;
			try { 
				module = piSDKServerInstance.PIModuleDB.PIModules[moduleList.First()];
				moduleList.RemoveAt(0);
				module = recurseModule(module, moduleList, piSDKServerInstance);
			} catch (Exception e) {
				throw new Exception("Unable to find a valid module from path: " + path + ". Details " + e.Message);
			}
			if (null != module) {
				return module;
			} else {
				throw new Exception("Unable to find a valid module from path: " + path);
			}
		}


		private static PIModule recurseModule(PIModule inModule, List<string> moduleList, PISDK.Server piSDKServerInstance) {
			if (moduleList.Count == 0) {
				return inModule; 
			}

			//Console.WriteLine("Analyzing module: '" + inModule.Name + "'");
			//Console.WriteLine("Searching module named: '" + moduleList.First() + "' within submodule list...");
			try { 
				PIModule module = inModule.PIModules[moduleList.First()];
				//Console.WriteLine("Module named: '" + moduleList.First() + "' found.");
				moduleList.RemoveAt(0);
				return recurseModule(module, moduleList, piSDKServerInstance);
			} catch(Exception e) {
				throw new Exception ("Unable to find module named: '" + moduleList.First() + "' within submodule list of " + inModule.Name, e);
			}
						
		}
	}
}
