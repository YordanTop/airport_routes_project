from abc import abstractmethod

import pandas as pd


class DataSetReader:


    def __int__(self, path=' '):
        self._path = path

    def Get_Path(self):
        return self._path

    def Set_Path(self,setPath):
        self._path = setPath



    def ReadFile(self, dataSetCols):

         result = pd.read_csv(self._path, sep=',', header=None, names=dataSetCols)
         return result

    @abstractmethod
    def Clean(self, DataCols, MatchingDataSet):
        pass

class AirportReader(DataSetReader):

    def __int__(self,path):
        DataSetReader.__int__(self,path)

    def Clean(self, DataCols, MatchingDataSet):

        ds = self.ReadFile(DataCols)
        mds = MatchingDataSet

        #Removing the empty rows
        #dsi Data Set Index
        for i,dsi in ds.iterrows():
                if(dsi['IATA'] == "\\N"):
                    ds.drop(index=i,inplace=True,axis=0)
                    continue

        ds.to_csv(self._path,index=False)
        return ds

class RoutesReader(DataSetReader):

    def __int__(self,path):
        DataSetReader.__int__(self,path)

    def Clean(self, DataCols, MatchingDataSet):
        ds = self.ReadFile(DataCols)
        mds = MatchingDataSet

        # Removing the empty rows
        # dsi Data Set Index

        for i, dsi in ds.iterrows():
            for j, value in dsi.items():
                if (value == "\\N"):
                    ds.drop(index=i, inplace=True, axis=0)
                    break

        # Removing the none paired IATA

        ds = ds[ds['Source airport'].isin(mds['IATA'])]

        ds.to_csv(self._path, index=False)
        return ds

if __name__ == '__main__':

    airportData = AirportReader()
    airportData.Set_Path('./AirportDataSet/airports.dat')


    routesData = RoutesReader()
    routesData.Set_Path('./AirportDataSet/routes.dat')

    DataCols =[
            [
            'Airport ID',
            'City',
            'Country',
            'IATA',
            'ICAO',
            'Latitude',
            'Longitude',
            'Altitude',
            'Timezone',
            'DST',
            'TZ database timezone',
            'Type',
            'Source'
        ],
        [
            'Airline',
            'Airline ID',
            'Source airport',
            'Source airport ID',
            'Destination airport',
            'Destination airport ID',
            'Codeshare',
            'Stops',
            'Equipment'
        ]

    ]


    routes = routesData.Clean(DataCols[1],airportData.ReadFile(DataCols[0]))
    airports = airportData.Clean(DataCols[0],routesData.ReadFile(DataCols[1]))
    print(routes)
    print(airports)