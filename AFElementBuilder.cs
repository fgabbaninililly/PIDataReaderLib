using NLog;
using OSIsoft.AF.Asset;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace PIDataReaderLib {
	class AFElementBuilder {

		private static Logger logger = LogManager.GetCurrentClassLogger();

		public static DIAAFElement build(AFElement afEle) {
			DIAAFElement diaAFEle = new DIAAFElement();
			diaAFEle.name = afEle.Name;
			diaAFEle.path = afEle.GetPath();
			foreach (AFAttribute afAttr in afEle.Attributes) {
				DIAAFAttribute diaAFAttr = build(afAttr);
				diaAFEle.afattributes.Add(diaAFAttr);
			}
			return diaAFEle;
		}

		private static DIAAFAttribute build(AFAttribute afAttr) {
			DIAAFAttribute diaAFAttr = new DIAAFAttribute();
			try {
				diaAFAttr.name = afAttr.Name;
			} catch (Exception) {
				logger.Error("Unable to read name for attribute");
			}

			try {
				diaAFAttr.value = afAttr.GetValue().Value.ToString();
			} catch (Exception) {
				logger.Error("Unable to read value for attribute: {0}.", afAttr.Name);
			}

			try {
				diaAFAttr.description = afAttr.Description;
			} catch (Exception) {
				logger.Error("Unable to read description for attribute: {0}.", afAttr.Name);
			}

			try {
				diaAFAttr.type = afAttr.Type.Name;
			} catch (Exception) {
				logger.Error("Unable to read type for attribute: {0}.", afAttr.Name);
			}

			try {
				diaAFAttr.uom = afAttr.DefaultUOM.Name;
			} catch (Exception) {
				logger.Error("Unable to read UOM for attribute: {0}.", afAttr.Name);
			}
			return diaAFAttr;
		}
	}
}
