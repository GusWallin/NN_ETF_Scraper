"""NN Scraper 

Scrapes webpage for ETF information, stores in dataframs and makes simple permutations
before it is saved to a .csv file.

good luck! 


"""

from selenium.common.exceptions import ElementClickInterceptedException
from bs4 import BeautifulSoup
import requests
import pandas as pd
from tabulate import tabulate
from selenium import webdriver
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
import time
import math
import numpy as np
from pathlib import Path

# Path to cromedriver
DRIVERPATH = r"C:\Program Files (x86)\chromedriver.exe"
file_path = Path().parent.absolute()
page = 1
csv_file_name = 'ETF_data_total.csv'

# Create selenium driver and load URL
driver = webdriver.Chrome(DRIVERPATH)
driver.get("https://www.nordnet.se/marknaden/etf-listor?sortField=name&sortOrder=asc&selectedTab=historical&page="+str(page))

# First instance of BFsoup and dataframe
soup = BeautifulSoup(driver.page_source, 'lxml')
table = soup.find_all('table')[0]
tab_data = [[cell.text for cell in row.find_all(["th", "td"])]
            for row in table.find_all("tr")]
#global variables
df = pd.DataFrame(tab_data)
page_counter = int(page)


# function to go to the next webpage by pressing "next" button
def get_next_page():
    element = WebDriverWait(driver, 5).until(
        EC.presence_of_element_located(
            (By.LINK_TEXT, "Nästa")))
    driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
    element.click()
    time.sleep(2)


# function to clean the raw data to understandable numbers.
def clean_number(number):
    str_number = str(number)
    if not str_number or len(str_number) == 0:
        return np.nan
    elif '%0,00' in str_number:
        return '0.00'
    elif '--' in str_number:
        return '-' + str_number.split(' ')[1]
    elif '++' in str_number:
        return str_number.split(' ')[1]
    else:
        return number

# function to clean the data in the dataframe


def clean_dataframe(dataf: pd.DataFrame):

    dataf.drop(columns=dataf.columns[12:], inplace=True)
    dataf.drop(columns=[0, 2, 3, 4], inplace=True)
    dataf.drop(0, axis=0, inplace=True)
    dataf.columns = ['Namn', '1 vecka', '1 mån',
                     '3 mån', 'i år', '1 år', '3 år', '5 år']
    dataf = dataf.applymap(lambda x: np.NaN if '–' in x else x)

    # clean each dataentry.
    dataf = dataf.applymap(clean_number)

    # drop rows with more than 4 Null values
    dataf.dropna(thresh=4, inplace=True)

    # gör data types till float för numerära kolumner.
    dataf = dataf.apply(pd.to_numeric, errors='ignore', axis=0)

    # Rankar alla columner och lägger ihop per rad i en ny column kallad return ranking sum.
    # Ju högre per column desto bättre, och mindre ranking total
    dataf['Return ranking sum'] = dataf.iloc[:, 1:].rank(
        0, method='min', ascending=False).sum(axis=1)

    # ger varje rad en ranking baserad på dess return ranking sum.
    # lägre 'Return ranking sum' är bättre (fler höga placeringar i ranking).
    dataf['return ranking'] = dataf.iloc[:, -
                                         1:].rank(0, method='min', ascending=True, pct=False)

    # skapar fält med return ranking sum/antalet datapunkter - Nan värden. Då vi inte har data för exempelvis 5år för alla finansiella instrument.
    dataf['avg return ranking'] = dataf['Return ranking sum'] / \
        (len(dataf.columns[1:8]) - dataf.isnull().sum(axis=1))
    dataf['avg return ranking'] = dataf['avg return ranking'].rank(
        0, method='min', ascending=True)
    # skapar kolumn som är true om EFT data aldrig är negativ.
    dataf['never returned negative'] = dataf.iloc[:, 1:8].applymap(
        lambda x: True if math.isnan(x) or x > 0 else False).all(1)

    return dataf


# this runs as long as there is an acive next button and therefore
# a next page to scrape data from.
while True:
    # try to access "nextbutton" else, exception and end.
    try:
        print('page: ' + str(page_counter))
        get_next_page()

        soup = BeautifulSoup(driver.page_source, 'lxml')
        table = soup.find_all('table')[0]

        tab_data = [[cell.text for cell in row.find_all(["th", "td"])]
                    for row in table.find_all("tr")]

        temp_df = pd.DataFrame(tab_data).iloc[1:]
        df = df.append(temp_df, ignore_index=True)

        page_counter = page_counter + 1
    except:
        print('Exception, end of pages.')
        break

# call fuction to clean the dataframe from raw data to data that we can use.
cleanframe = clean_dataframe(df)

# write the clened completed dataframe to CSV file.
cleanframe.to_csv('' + str(file_path) + '\\' + csv_file_name)

print('File: ' + csv_file_name + ' Saved with ' +
      str(len(cleanframe.index)) + ' Rows')

# the end
driver.quit()
