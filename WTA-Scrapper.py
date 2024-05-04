from collections import defaultdict
import pandas as pd
from algoliasearch.search_client import SearchClient
from datetime import datetime
from enum import Enum
import base64
import hashlib
import requests
from bs4 import BeautifulSoup
import pandas as pd
from numpy import arange
import pickle
import math

class WTAScrapper():
    def scrapeHikeUrls(self):

        hike_urls = []
        url_base = 'https://www.wta.org/go-outside/hikes?b_start:int='

        url_pages = arange(0, 5000, 30)

        print("Extracting Trail Pages...")

        for p in list(range(len(url_pages))):
            
            url = url_base + str(url_pages[p])
            req = requests.get(url)
            soup  = BeautifulSoup(req.text, 'lxml')
            
            hikeDivs = soup.find_all('h3' , attrs={'class': 'listitem-title'}) 

            hikes = []
            for hikeDiv in hikeDivs:
                hikes.extend(hikeDiv.find_all('a' ,  href=True))    
            
            if len(hikes) == 0:
                break
            
            for i in list(arange(0, len(hikes), 1)):
                hike_urls.append(hikes[i]['href'])
            
            print("\rScraping Trail URLs: "  + str(len(hike_urls))  + " found")
        
        print("...Done")

        return hike_urls

    def scrapeHikeData(self, url):
        response = requests.get(url)
        hike = dict()

        if response.status_code == 200:
            hikeHtml = BeautifulSoup(response.content, 'lxml')

            #Trail_Name
            key = 'Trail_Name'
            try:
                target = hikeHtml.find('h1', attrs={'class': 'documentFirstHeading'})
                value = target.text.strip()
                hike[key] = value
            except:
                pass

            #Area
            key = 'Area'
            try:
                target = hikeHtml.find('div', attrs={'class': 'wta-icon-headline__text'})
                value = target.text.strip()
                hike[key] = value
            except:
                pass

            #Distance, Gain, Highest_Point, Difficulty
            hikestats = hikeHtml.find_all('div', attrs={'class':'hike-stats__stat', 'id':''})
            for hikestat in hikestats:
                try:
                    text = hikestat.find('dt').text.split('\n')
                    key = list(filter(len, text))[0].strip()
                    value = hikestat.find('dd').text.strip()
                    hike[key] = value

                except:
                    pass

            #Rating
            key = 'Rating'
            try:
                target = hikeHtml.find('div', attrs={'class':'current-rating'})
                value = target.text.strip()
                hike[key] = value
            except:
                pass

            #Rating Count
            key = 'Rating_Count'
            try:
                target = hikeHtml.find('div', attrs={'class':'rating-count'})
                value = target.text.strip()
                hike[key] = value
            except:
                pass

            #Permits/Passes
            key = 'Permits'
            try:
                target = hikeHtml.find('div', attrs={'class':'alert'})
                target = target.find('a', attrs={'href':'https://www.wta.org/go-outside/passes'})
                value = target.text.strip()
                hike[key] = value
            except:
                pass

            #GPS Coordinates
            try:
                target = hikeHtml.find('div', attrs={'class':'latlong'})
                targets = target.find_all('span')
                if len(targets) == 2:
                    key = 'Lat'
                    value = targets[0].text.strip()
                    hike[key] = value

                    key = 'Long'
                    value = targets[1].text.strip()
                    hike[key] = value
            except:
                pass

            #Cover Photo
            key = 'Cover_Photo'
            try:
                target = hikeHtml.find('img', attrs={'class':'wta-ratio-figure__image'})
                value = target['src'].strip()
                hike[key] = value
            except:
                pass

            #Url
            hike['Url'] = url
        
        return hike

    def dumpFile(self, filepath, data):
        try:
            with open(filepath, 'wb') as file:
                return pickle.dump(data, file)
        except:
            return None

    def loadFile(self, filepath):
        try:
            with open(filepath, 'rb') as file:
                return pickle.load(file)
        except:
            return None
            
    def scrape(self):

        urldumpfile = 'WTA_Hike_Urls.pickle'
        traildumpfile = 'WTA_Hike_Details.pickle'

        if not (hike_urls := self.loadFile(urldumpfile)):
            hike_urls = self.scrapeHikeUrls()
            self.dumpFile(urldumpfile, hike_urls)
            print(f"Found {len(hike_urls)} trails")
        else:
            print(f"Loaded {len(hike_urls)} trails pages from '{urldumpfile}'")


        if not (hikes := self.loadFile(traildumpfile)):
            hikes = []
            for i, url in enumerate(hike_urls):
                hikes.append(self.scrapeHikeData(url))
                print(f"\rExtracting data from trails: {i+1} of {len(hike_urls)} complete", flush=True)
            print("...done")
            self.dumpFile(traildumpfile, hikes)
        else:
            print(f"Loaded {len(hike_urls)} trails details from '{traildumpfile}'")

        return hikes

class TrailsExporter:
     
    class CsvHeader(Enum):
        Unique_Id = 0
        Alt_Id = 1
        Trail_Name = 2
        Source = 3
        Distance = 4
        Elevation_Gain = 5
        Highest_Point = 6
        Difficulty = 7
        Est_Hike_Duration = 8
        Trail_Type = 9
        Permits = 10
        Rating = 11
        Review_Count = 12
        Area = 13
        State = 14
        Country = 15
        Latitude = 16
        Longitude = 17
        Url = 18
        Cover_Photo = 19
        Parsed_Date = 20

    def getTrailAttribute(self, hike, attribute):

        truncate = lambda f, n: math.floor(f * 10 ** n) / 10 ** n

        match attribute:
            case self.CsvHeader.Unique_Id:
                uniqueName = self.getTrailAttribute(hike, self.CsvHeader.Url).rpartition('/')[2]
                source = self.getTrailAttribute(hike, self.CsvHeader.Source)
                latitude = self.getTrailAttribute(hike, self.CsvHeader.Latitude)
                longitude = self.getTrailAttribute(hike, self.CsvHeader.Longitude)

                joinKey = '&'.join([uniqueName, source, str(truncate(latitude, 3)), str(truncate(longitude, 3))])
                id = hashlib.sha1(joinKey.encode("UTF-8")).hexdigest()
                return id[:12]

            case self.CsvHeader.Alt_Id:
                return None

            case self.CsvHeader.Trail_Name:
                return hike.get('Trail_Name', None)

            case self.CsvHeader.Source:
                return 'WTA'

            case self.CsvHeader.Distance:
                distance = hike.get('Length', None)
                distance = float(distance.partition('miles')[0].strip()) if distance else None
                return distance

            case self.CsvHeader.Elevation_Gain:
                gain = hike.get('Elevation Gain', None)
                gain = float(gain.replace(',', '').partition('feet')[0].strip()) if gain else None
                return gain

            case self.CsvHeader.Highest_Point:
                highest = hike.get('Highest Point', None)
                highest = float(highest.replace(',', '').partition('feet')[0].strip()) if highest else None
                return highest

            case self.CsvHeader.Difficulty:
                difficultyMap = {'Hard':5, 'Moderate/Hard':4, 'Moderate':3, 'Easy/Moderate':2, 'Easy':1}
                difficulty = hike.get('Calculated Difficulty', None)
                return difficultyMap.get(difficulty, None)
                
            case self.CsvHeader.Est_Hike_Duration:
                return None

            case self.CsvHeader.Permits:
                return hike.get('Permits', None)

            case self.CsvHeader.Trail_Type:
                type = hike.get('Length', None)
                type = type.partition(',')[2].strip() if type else None
                return type

            case self.CsvHeader.Rating:
                rating = hike.get('Rating', None)
                rating = float(rating.partition('out of')[0].strip()) if rating else None
                return rating

            case self.CsvHeader.Review_Count:
                count = hike.get('Rating_Count', None)
                count = int(count.partition('vote')[0].removeprefix('(').strip()) if count else None
                return count
            
            case self.CsvHeader.Area:
                area = hike.get('Area', None)
                area = area.partition('>')[0].strip() if area else None
                return area

            case self.CsvHeader.State:
                return "Washington"

            case self.CsvHeader.Country:
                return "United States"

            case self.CsvHeader.Latitude:
                latitude = hike.get('Lat', None)
                return float(latitude) if latitude else None

            case self.CsvHeader.Longitude:
                longitude = hike.get('Long', None)
                return float(longitude) if longitude else None

            case self.CsvHeader.Url:
                return hike.get('Url', None)

            case self.CsvHeader.Cover_Photo:
                return hike.get('Cover_Photo', None)

            case self.CsvHeader.Parsed_Date:
                return datetime.now().date().isoformat()

    def exportToCsv(self, hikes, exportPath):

        csvHeaders = defaultdict(list)

        for hike in hikes:
            for header in self.CsvHeader:
                csvHeaders[header.name].append(self.getTrailAttribute(hike, header))

        hike_data = pd.DataFrame(csvHeaders)
        hike_data.to_csv(exportPath, index = False)

if __name__ == "__main__":

    scrapper = WTAScrapper()
    exporter = TrailsExporter()
    hikes = scrapper.scrape()
    hikes = filter(lambda x: all(k in x for k in ('Url', 'Long', 'Lat')), hikes)

    exporter.exportToCsv(hikes, f"./data/WTA_Data_Full_{datetime.now().date()}.csv")

