# WTA Trail Scrapper

This script extracts all trails and their associated information from WTA.org and stores in CSV format. 

Data extracted will have the following rows:

- `Unique_Id` 
- `Alt_Id` (Unused)
- `Trail_Name` 
- `Source` 
- `Distance` 
- `Elevation_Gain` 
- `Highest_Point` 
- `Difficulty` 
- `Est_Hike_Duration` (Unused)
- `Trail_Type` 
- `Permits`
- `Rating` 
- `Review_Count` 
- `Area` 
- `State` 
- `Country` 
- `Latitude` 
- `Longitude` 
- `Url` 
- `Cover_Photo` 
- `Parsed_Date` 

Intermediary data will be stored in *.pickle files. To have a complete full run, delete these files and run: `python3 WTA-Scrapper.py`