from ConfigParser import ConfigParser

class LoadProperties(object):

    def __init__(self, PropertyEnum):
        self.__propertiesCatalog = PropertyEnum

    def getProperty(self, key, value):
        config = ConfigParser()
        config.read(self.__propertiesCatalog)
        return config.get(key, value)
