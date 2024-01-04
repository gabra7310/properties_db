from bs4 import BeautifulSoup as bs
import requests
import json
import time
import os
import pandas as pd
import datetime

class Scraper():
    def __init__(self):
        self.url = 'https://glue-api.vivareal.com/v2/listings?'
        self.headers = {'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_5) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/83.0.4103.106 Safari/537.36',
                        'X-Domain':'www.vivareal.com.br',
                        'Referer':'https://www.vivareal.com.br/'}
        
    def extract(self, size, from_, delay):
        # size : Quantidade de dados
        # from_ : Paginação dos dados
        # delay : Tempo de delay até o próximo load

        req_url = self.url + f'addressCity=S%C3%A3o%20Paulo&addressLocationId=BR%3ESao%20Paulo%3ENULL%3ESao%20Paulo&addressNeighborhood=&addressState=S%C3%A3o%20Paulo&addressCountry=Brasil&addressStreet=&addressZone=&addressPointLat=-23.555771&addressPointLon=-46.639557&business=SALE&facets=amenities&unitTypes=APARTMENT&unitSubTypes=UnitSubType_NONE%2CDUPLEX%2CLOFT%2CSTUDIO%2CTRIPLEX&unitTypesV3=APARTMENT&usageTypes=RESIDENTIAL&listingType=USED&parentId=null&categoryPage=RESULT&images=webp&size={size}&from={from_}&q=&developmentsSize=5&__vt=control&levels=CITY%2CUNIT_TYPE&ref=&pointRadius=&isPOIQuery='

        pages = size // 100

        for i in range(pages):
            size = 100
            start = from_ + 100 * i
            end = start + 100
            print(f'{start}-{end}')
            
            req_url = self.url + f'addressCity=S%C3%A3o%20Paulo&addressLocationId=BR%3ESao%20Paulo%3ENULL%3ESao%20Paulo&addressNeighborhood=&addressState=S%C3%A3o%20Paulo&addressCountry=Brasil&addressStreet=&addressZone=&addressPointLat=-23.555771&addressPointLon=-46.639557&business=SALE&facets=amenities&unitTypes=APARTMENT&unitSubTypes=UnitSubType_NONE%2CDUPLEX%2CLOFT%2CSTUDIO%2CTRIPLEX&unitTypesV3=APARTMENT&usageTypes=RESIDENTIAL&listingType=USED&parentId=null&categoryPage=RESULT&images=webp&size={size}&from={start}&q=&developmentsSize=5&__vt=control&levels=CITY%2CUNIT_TYPE&ref=&pointRadius=&isPOIQuery='    
            req = requests.get(req_url, headers=self.headers)

            data = req.json()
            name = str(f'{start}-{end}')

            with open(f'data/json/{name}.json', 'w') as open_file:
                json.dump(data, open_file)
            
            time.sleep(delay)
        return None
    
    def iter_try(self, i, feature):
        try:
            var = i['listing'][feature]
        except:
            var = 'None'

        return var

    def load_to_csv(self, data_folder):
        filespaths = [f"{data_folder}/{i}"  for i in os.listdir(f"{data_folder}/")]
        df = pd.DataFrame()

        for file in filespaths:
            with open(file, 'r') as open_file:
                list = json.load(open_file)
                for i in list['search']['result']['listings']:
                    id = self.iter_try(i, 'id')
                    usableAreas = self.iter_try(i, 'usableAreas')
                    createdAt = self.iter_try(i, 'createdAt')
                    displayAddressGeolocation = self.iter_try(i, 'displayAddressGeolocation')
                    parkingSpaces = self.iter_try(i, 'parkingSpaces')
                    suites = self.iter_try(i, 'suites')
                    bathrooms = self.iter_try(i, 'bathrooms')
                    bedrooms = self.iter_try(i, 'bedrooms')
                    pricingInfos = self.iter_try(i, 'pricingInfos')
                    amenities = self.iter_try(i, 'amenities')
                    unitFloor = self.iter_try(i, 'unitFloor')
                   
                    dict_iter = {
                        'id' : id,
                        'usableAreas' : usableAreas,
                        'createdAt' : createdAt,
                        'displayAddressGeolocation' : displayAddressGeolocation,
                        'parkingSpaces' : parkingSpaces,
                        'suites' : suites,
                        'bathrooms' : bathrooms,
                        'bedrooms' : bedrooms,
                        'pricingInfos' : pricingInfos,
                        'amenities' : amenities,
                        'unitFloor' : unitFloor,
                    }
                    
                    df_iter = pd.DataFrame.from_dict(dict_iter, orient='index').T
                    df = pd.concat([df, df_iter])
            os.remove(file)    
        timestamp = str(int(datetime.datetime.now().timestamp()))
        df.to_csv(f'./data/data-{timestamp}.csv')

        return None
    
sc = Scraper()
sc.extract(10000,0,3)
sc.load_to_csv('data/json')

