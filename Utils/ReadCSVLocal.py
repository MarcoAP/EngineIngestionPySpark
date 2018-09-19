from pandas import pandas as pd

class ReadCSVLocal(object):

    def readCsvLocalDefault(self):
        pass

    def readCsvLocalWithPandas(self, dir=None):
        try:
            csv = pd.read_csv(dir)


        except IOError:
            print