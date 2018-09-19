from Enums.PropertiesCatalogEnum import PropertiesCatalogEnum
from Properties.LoadProperties import LoadProperties

class ReadPathsFromPropertyDTO(object):

    def getWindowsPathsFrom (self):
        return LoadProperties(PropertyEnum=PropertiesCatalogEnum.RPF).getProperty("Windows-Paths-From", "path")

    def getLinuxPathsFrom (self):
        return LoadProperties(PropertyEnum=PropertiesCatalogEnum.RPF).getProperty("Linux-Paths-From", "path")

    def getMacOSPathsFrom (self):
        return LoadProperties(PropertyEnum=PropertiesCatalogEnum.RPF).getProperty("MacOS-Paths-From", "path")