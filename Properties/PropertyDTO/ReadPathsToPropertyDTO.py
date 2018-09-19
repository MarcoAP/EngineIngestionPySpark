from Enums.PropertiesCatalogEnum import PropertiesCatalogEnum
from Properties.LoadProperties import LoadProperties

class ReadPathsToPropertyDTO(object):

    def getWindowsPathsTo (self):
        return LoadProperties(PropertyEnum=PropertiesCatalogEnum.RPF).getProperty("Windows-Paths-To", "path")

    def getLinuxPathsTo (self):
        return LoadProperties(PropertyEnum=PropertiesCatalogEnum.RPF).getProperty("Linux-Paths-To", "path")

    def getMacOSPathsTo (self):
        return LoadProperties(PropertyEnum=PropertiesCatalogEnum.RPF).getProperty("MacOS-Paths-To", "path")