import requests
import time
from bs4 import BeautifulSoup
from airflow.models import Variable
from selenium import webdriver
from selenium.webdriver.firefox.firefox_binary import FirefoxBinary
from selenium.webdriver.firefox.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from datetime import datetime, timedelta
import re

from scraper import LandsScraper_opensooq
from settings import BASE_URL
from settings import HEADERS
my_scraper = LandsScraper_opensooq(BASE_URL, HEADERS)
class Checker:

    def check_tags_info(self,df,soup0):
        list_of_miss = []
        proxies = {'http': 'socks5://127.0.0.1:9050',
                   'https': 'socks5://127.0.0.1:9050'}
        url1 = df.iloc[0, 0]
        print(df.info())
        print(url1)
        response = requests.get(url1, proxies=proxies)
        soup1 = BeautifulSoup(response.content, 'lxml')
        url_profile = soup1.find('a', {'class': f"{Variable.get('scr_url_profile')}"}).get('href')
        #print(url_profile)
        if (url_profile):
            pass
        else:
            list_of_miss.append('scr_url_profile')

        value = my_scraper.retries(url1, soup1, 'div', {'class': f"{Variable.get('scr_price')}"})
        if (value):
            pass
        else:
            list_of_miss.append('scr_price')

        value = my_scraper.retries(url1, soup1, 'span', {'class': f"{Variable.get('scr_number')}"})
        if (value):
            pass
        else:
            list_of_miss.append('scr_number')

        value = my_scraper.retries(url1, soup1, 'h3', {'class': f"{Variable.get('scr_post_owner')}"})
        if (value):
            pass
        else:
            list_of_miss.append('scr_post_owner')

        poster_information_box = my_scraper.retries(url1, soup1, 'ul',
                                                    {'class': f"{Variable.get('scr_information_box')}"},
                                                    just_check=True)
        if (poster_information_box):
            pass
        else:
            list_of_miss.append('scr_information_box')

        pattern = re.compile(f"{Variable.get('scr_google_map')}")
        value = my_scraper.retries(url1, soup1, 'a', {'class': pattern}, just_check=True)
        if (value):
            pass
        else:
            list_of_miss.append('scr_google_map')

        pattern = re.compile(fr'{Variable.get("scr_description")}')
        t = r'\w+-\w+-\w+ \w+ font-17 breakWord'
        print('pattern', f"{pattern}".replace('\\\\','\\') )
        poster_des = my_scraper.retries(url1, soup1, 'div',
                                        {'class': pattern}, just_check=True)
        if (poster_des):
            print(poster_des)
            pass
        else:
            list_of_miss.append('scr_description')

        url2 = 'https://jo.opensooq.com'+url_profile
        response = requests.get(url2, proxies=proxies)
        soup2 = BeautifulSoup(response.content, 'lxml')


        value = my_scraper.retries(url2, soup2, 'li', {'class': f"{Variable.get('scr_advertise')}"})
        if (value):
            print(value)
            pass
        else:
            list_of_miss.append('scr_advertise')

        url3 = 'https://jo.opensooq.com' + url_profile + '?info=info'
        response = requests.get(url3, proxies=proxies)
        soup3 = BeautifulSoup(response.content, 'lxml')
        pattern = re.compile(f"{Variable.get('scr_rate')}")
        value = my_scraper.retries(url3, soup3, 'div', {'class': pattern})
        if (value):
            pass
        else:
            list_of_miss.append('scr_rate')

        pattern = re.compile(fr"{Variable.get('scr_time_poster')}")
        time = soup0.find_all('a', {'class': re.compile(pattern)})

        #print('pattern',Variable.get('scr_time_poster'))
        #time = soup0.find_all('div', {'class': pattern})
        if (len(time) > 0):
            print(time)
        else:
            list_of_miss.append('scr_time_poster')

        # print(list_of_miss)
        return list_of_miss

    def check_tags_numbers(self, my_numbers):
        list_of_miss = []

        numbers_pool = my_numbers.split(',')
        my_password = 'test1234'
        nmbr = numbers_pool[0]
        driver = my_scraper.create_driver()
        url = f'https://jo.opensooq.com/ar/عقارات-للبيع/أراضي-للبيع?page=1'
        # create driver

        proxies = {'http': 'socks5://127.0.0.1:9050',
                   'https': 'socks5://127.0.0.1:9050'}

        session = requests.Session()
        getOut = False
        try:
            response = session.get(url, proxies=proxies)
            response.raise_for_status()
            print("Request successful")
        except requests.exceptions.RequestException as e:
            print("Request failed:", str(e))
            count = 0
            while (response.status_code != 200 and getOut == False):
                mmax = 8
                count += 1
                sleep_time = 4
                if (response.status_code == 500):
                    mmax = 2
                    sleep_time = 2
                print(count, response.status_code, e)
                print(f'retry after {sleep_time} mins')
                time.sleep(sleep_time * 60)
                print('retry now')
                response = session.get(url, proxies=proxies)
                if (count >= mmax):
                    getOut = True
                    print('Get Out')
            if (response.status_code != 200):
                raise Exception(f"max tries exceeded, Can not request url")
        driver.get(url)
        #
        # Login button
        try:
            btn = WebDriverWait(driver, 20).until(EC.element_to_be_clickable(#p-8 ms-8 loginBtn whiteBtn width-auto font-20
                (By.XPATH, f"//button[@class='{Variable.get('nmbrs_scr_login_btn')}']"))).click()

        except:
            list_of_miss.append('nmbrs_scr_login_btn')
            return list_of_miss
        # driver.execute_script("arguments[0].click();", btn)
        #
        # change country
        try:
            btn = WebDriverWait(driver, 20).until(EC.element_to_be_clickable(
                (By.XPATH,
             f"//button[@class='{Variable.get('nmbrs_scr_country_code')}']"))).click()
        except:
            list_of_miss.append('nmbrs_scr_country_code')
            return list_of_miss
        # driver.execute_script("arguments[0].click();", btn)
        try:
            country_list = WebDriverWait(driver, 20).until(EC.visibility_of_all_elements_located(
                (By.XPATH, f"//li[@class='{Variable.get('nmbrs_scr_countries_list')}']")))
        except:
            list_of_miss.append('nmbrs_scr_countries_list')
            return list_of_miss
        target_country = country_list[16].find_elements(By.XPATH, f"//div[@class='{Variable.get('nmbrs_scr_target_country')}']")[16]
        driver.execute_script("arguments[0].click();", target_country)
        print('CLICKED', nmbr)
        #
        # input(number)
        try:
            my_scraper.input_box(driver, f"//input[@class='{Variable.get('nmbrs_scr_number_field')}']", nmbr)
        except:
            list_of_miss.append('nmbrs_scr_number_field')
            return list_of_miss
        #
        # next button
        try:
            btn = WebDriverWait(driver, 20).until(
                EC.element_to_be_clickable(
                    (By.XPATH, f"//button[@class='{Variable.get('nmbrs_scr_another_login_btn')}']"))).click()
        except:
            list_of_miss.append('nmbrs_scr_another_login_btn')
            return list_of_miss
        # driver.execute_script("arguments[0].click();", btn)
        #
        # input(password)

        try:
            #time.sleep(5)
            pro = WebDriverWait(driver, 10).until(EC.presence_of_element_located(
                (By.XPATH, f"//input[@data-id='passwordField']")))
            print(my_password)
            #self.input_box(driver, f"//input[@class='{Variable.get('nmbrs_scr_number_field')}']", my_password)
            my_scraper.input_box(driver, f"//input[@data-id='passwordField']", my_password)
            #inp_fields= driver.find_elements(By.XPATH, f"//input[@class='width-100 height-50']")
            #inp_fields[1].send_keys(my_password)

        except Exception as e:
            print('pass', e)
            pass


        try:
            element = WebDriverWait(driver, 5).until(
            EC.presence_of_element_located(
                (By.XPATH, f"//button[@class='{Variable.get('nmbrs_scr_next_btn')}']"))).click()
        except:
            list_of_miss.append('nmbrs_scr_next_btn')
            return list_of_miss
        # driver.execute_script("arguments[0].click();", element)
        print('Click Done')
        #
        #
        try:
            element2 = WebDriverWait(driver, 10).until(EC.presence_of_element_located(
            (By.XPATH, f"//button[@class='{Variable.get('nmbrs_scr_another_next_btn')}']"))).click()

            driver.quit()
        except:
            list_of_miss.append('nmbrs_scr_next_btn')
            return list_of_miss
