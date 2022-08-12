from requests import get
from bs4 import BeautifulSoup
from selenium import webdriver
import time
import pandas as pd

def make_get_request(url):
    r = get(url)
    if not r.ok:
        raise Exception("Get request was not successful")
    return r

def retail_station_scrape(r, driver):
    def station_website(link):
        """helper function to scrape data from the individual website.
        it will return a dictionary containing the timestamp and the statuses of the hydrogen"""

        # gets updated pump information
        driver.get(link)
        time.sleep(0.5) #gives the webpage enough time to load the message
        html = driver.page_source
        soup = BeautifulSoup(html, features="lxml")
        
        #pump_status is the wrapper containing all the pump status information
        pump_status = soup.find('div', {'class': 'pump-status'})
        pump_dict = {}

        # finds the date and timestamp when the information was last updated
        # format of timestamp: ['Wednesday', '6/15/2022', '2:34', 'PM']
        try:
            timestamp = pump_status.find('div', {'class': 'last-update'}).contents[1].replace(',', '').split()
            pump_dict['date'] = timestamp[1]
            pump_dict['time'] = timestamp[2] + timestamp[3]
        except IndexError:
            return station_website(link)

        #finds station message, if any
        if soup.find('div', {'class':'info-text'}) == None:
            pump_dict['alert'] = None
        else:
            pump_dict['alert'] = soup.find('div', {'class':'info-text'}).contents[0].strip()

        #finds the h35 information, if any
        if pump_status.find('div', {'class': 'h35status'}) == None:
            pump_dict['h35-status'] = None
            pump_dict['h35-inventory'] = None
        else:
            pump_dict['h35-status'] = pump_status.find('div', {'class': 'h35status'}).find('span').contents[0]
            #float(pump_status.find('div', {'class':'h35capacity'}).find('span').contents[0].split()[0])
            pump_dict['h35-inventory'] = pump_status.find('div', {'class':'h35capacity'}).find('span').contents[0]
        #finds the h70 information, if any
        if pump_status.find('div', {'class': 'h70status'}) == None:
            pump_dict['h70-status'] = None
            pump_dict['h70-inventory'] = None
        else:
            pump_dict['h70-status'] = pump_status.find('div', {'class': 'h70status'}).find('span').contents[0]
            pump_dict['h70-inventory'] = pump_status.find('div', {'class':'h70capacity'}).find('span').contents[0]

        return pump_dict

    soup = BeautifulSoup(r.content, 'html.parser')
    rs_dict = {}

    #finds the retail stations and adds it to the dictionary
    retail_stations = soup.findAll('tr', attrs={"class":"retail"})
    for retail_station in retail_stations:
        #name_wrapper is the HTML element that contains the name of the station
        name_wrapper = retail_station.find('td', attrs={"class":"name"})
        
        #ind link is the link to the individual station
        ind_link = url+name_wrapper.find('a', href=True)['href']
        station_dict = station_website(ind_link)
        station_dict['legacy'] = False
        
        station_name = name_wrapper.find('a').contents[0].find('span').contents[0].strip()
        rs_dict[station_name] = station_dict

    #finds the nonretail stations and adds it to the dictionary
    legacy_stations = soup.findAll('tr', attrs={"class":"nonretail"})
    for legacy_station in legacy_stations:
        #name_wrapper is the HTML element that contains the name of the station
        name_wrapper = legacy_station.find('td', attrs={"class":"name"})
        
        #ind link is the link to the individual station
        ind_link = url+name_wrapper.find('a', href=True)['href']
        station_dict = station_website(ind_link)
        station_dict['legacy'] = True
        
        station_name = name_wrapper.find('a').contents[0].find('span').contents[0].strip()
        rs_dict[station_name] = station_dict

    return rs_dict

def update(master_file, rs_dict, scrape_time):
    try:
        h70status = pd.read_excel(master_file, sheet_name="H70 Status")
        h70avail = pd.read_excel(master_file, sheet_name="H70 Availability")
        h35status = pd.read_excel(master_file, sheet_name="H35 Status")
        h35avail = pd.read_excel(master_file, sheet_name="H35 Availability")
        alerts = pd.read_excel(master_file, sheet_name="Alerts")
    except:
        print("unable to access master file")
        return
    
    #makes space for new sheet
    h70status.insert(loc=2, column=scrape_time, value="")
    h70avail.insert(loc=2, column=scrape_time, value="")
    h35status.insert(loc=2, column=scrape_time, value="")
    h35avail.insert(loc=2, column=scrape_time, value="")
    alerts.insert(loc=2, column=scrape_time, value="")

    for station, station_dict in rs_dict.items():
        row_index = h70status.Station[h70status.Station == station].index.tolist()
        if row_index == []: # if the station isn't in the spreadsheet
            new = [station, station_dict['legacy'], station_dict['h70-status']]
            h70status = h70status.append(pd.Series(new, index=h70status.columns[:len(new)]), ignore_index=True)
            h70avail = h70avail.append(pd.Series(new, index=h70avail.columns[:len(new)]), ignore_index=True)
            h35status = h35status.append(pd.Series(new, index=h35status.columns[:len(new)]), ignore_index=True)
            h35avail = h35avail.append(pd.Series(new, index=h35avail.columns[:len(new)]), ignore_index=True)
            alerts = alerts.append(pd.Series(new, index=alerts.columns[:len(new)]), ignore_index=True)
        else:
            h70status.at[row_index[0], scrape_time] = station_dict['h70-status']
            h70avail.at[row_index[0], scrape_time] = station_dict['h70-inventory']
            h35status.at[row_index[0], scrape_time] = station_dict['h35-status']
            h35avail.at[row_index[0], scrape_time] = station_dict['h35-inventory']
            alerts.at[row_index[0], scrape_time] = station_dict['alert']

    writer = pd.ExcelWriter(master_file)
    try:
        h70status.to_excel(writer, 'H70 Status', index=False)
        h70avail.to_excel(writer, 'H70 Availability', index=False)
        h35status.to_excel(writer, 'H35 Status', index=False)
        h35avail.to_excel(writer, 'H35 Availability', index=False)
        alerts.to_excel(writer, 'Alerts', index=False)
        writer.close()
    except:
        print("writing to excel file failed")
        writer.close()

if __name__ == "__main__":
    try:
        driver = webdriver.Chrome()
        driver.minimize_window()
    except:
        print("make sure that chromedriver is in the directory")
    
    url = "https://m.cafcp.org"
    #master_file = "SOSS Web Scraping Data.xlsx"
    master_file = "K:\\ugrad\\asl\\SOSS Web Scraping Data.xlsx"
    
    
    while True:
        try:
            r = make_get_request(url)
            dict = retail_station_scrape(r, driver)
        except:
            pass
        update(master_file, dict, pd.to_datetime(time.time(), unit='s'))

        # waits 17 seconds (in order to scrape every 45 seconds)
        time.sleep(30)
    