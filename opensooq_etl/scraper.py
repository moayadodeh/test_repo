# scraper.py
import requests
import time
from bs4 import BeautifulSoup
from airflow.models import Variable
from selenium import webdriver
from selenium.webdriver.firefox.firefox_binary import FirefoxBinary
from selenium.common.exceptions import WebDriverException
from selenium.webdriver.firefox.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from datetime import datetime, timedelta
import pandas as pd
import datetime
import re
import os
import copy
from common_helpers import create_driver

# Base class
class BaseScraper:
    ####Constructor
    def __init__(self, base_url, headers, filter_params, features):
        """
        Constructor for a Base Class
        """
        self.base_url = base_url  # base url for website like https://jo.opensooq.com/ar
        self.headers = headers  # headers to be sent with each request
        self.filter_params = filter_params  # filter parameters to include in each request,such as page numberr
        self.data = dict()  # Dictionary to store scraped data,including the features you will extract
        for key in features:
            self.data[key] = []  # Initialize empty lists for each feature

    #####
    # maximum_retries request function
    def maximum_retries(self, url, url_name, max_req=4, sleep_time=4, params=dict()):
        """
        Maximum retries for a successful request.
        """
        proxies = {'http': 'socks5://127.0.0.1:9050',
                   'https': 'socks5://127.0.0.1:9050'}
        getOut = False
        try:
            response = requests.get(url, headers=self.headers, params=params, proxies=proxies)
            response.raise_for_status()
            print("FETCH successful")
            getOut = True
        except requests.exceptions.RequestException as error:
            print("Request failed:", str(error))
            count = 0
            while (response.status_code != 200 and getOut == False):
                max_r = max_req
                count += 1
                sleep_time = sleep_time
                if (response.status_code == 500):  # requests is successful
                    max_r = 2  # for stop tries
                    sleep_time = 2
                print(f"Retry count: {count}, Status code: {response.status_code}, Error: {error}")
                print(f'retry after {sleep_time} mins')
                time.sleep(sleep_time * 60)
                print('retry now')
                response = requests.get(url, headers=self.headers, params=params, proxies=proxies)
                if (count >= max_r):
                    getOut = True
                    print('Get Out')
        if (response.status_code != 200):
            print(f"max tries exceeded, Can not request url in {url_name}")
        return response.content

    ######
    # maximum_retries driver function
    def maximum_retries_driver(self, driver, url, url_name, max_req=4, sleep_time=4):
        """
        Maximum retries for a successful request using driver.
        """
        attempt = 0
        while attempt < max_req:
            try:
                driver.get(url)
                print("Navigation successful")
                time.sleep(1)
                response = driver.page_source
                return response
            except WebDriverException as error:
                attempt += 1
                print(f"Attempt {attempt}/{max_req} failed with error: {error}")
                if attempt < max_req:
                    # Adjust retry parameters if needed (e.g., shorten sleep time after a few attempts)
                    if attempt == 1:
                        sleep_time = 2  # Reduce sleep time after the first attempt
                    print(f"Retrying in {sleep_time} minutes...")
                    time.sleep(sleep_time * 60)  # Sleep before retrying
        print(f"Maximum retries exceeded while trying to navigate to URL '{url_name}'")

    #######
    # fetch function
    def fetch(self, url_name, page=0, max_req=4, sleep_time=4, driver=None):
        """
        fetch data using a URL with parameters and filters
        """
        url = f"{self.base_url}/{self.filter_params['main_type']}/{self.filter_params['secondary_type']}"
        if driver:
            content = self.maximum_retries_driver(driver, url, url_name, max_req, sleep_time, params={"page": page})
        else:
            content = self.maximum_retries(url, url_name, max_req, sleep_time, params={"page": page})
        return content

    ##########
    # retries function to scrape elemnet
    def retries(self, url, soup, tag, class_tag=dict(), multi_search=False, just_check=False, index=0, max_retries=1):
        """
        retries to scrape elemnet
        """
        proxies = {'http': 'socks5://127.0.0.1:9050',
                   'https': 'socks5://127.0.0.1:9050'}
        data_tag = None
        # if need not just return text within tag
        if just_check:
            if class_tag and multi_search:
                data_tag = soup.find_all(tag, class_tag)
            elif multi_search:
                data_tag = soup.find_all(tag)
            elif class_tag:
                data_tag = soup.find(tag, class_tag)
            else:
                data_tag = soup.find(tag)
            if data_tag:
                return data_tag
            else:
                response = requests.get(url, proxies=proxies)
                soup = BeautifulSoup(response.content, 'lxml')
        # if need just return text within tag
        else:
            for try_extract in range(max_retries):
                if class_tag and multi_search:
                    data_tag = soup.find_all(tag, class_tag)
                    if data_tag:
                        data_tag = soup.find_all(tag, class_tag)[index]
                elif multi_search:
                    data_tag = soup.find_all(tag)
                    if data_tag:
                        data_tag = soup.find_all(tag)[index]
                elif class_tag:
                    data_tag = soup.find(tag, class_tag)
                else:
                    data_tag = soup.find(tag)
                if data_tag:
                    return data_tag.text
                else:
                    response = requests.get(url, proxies=proxies)
                    soup = BeautifulSoup(response.content, 'lxml')
        return False

    ##########
    # update error function
    def update_error_list(self, error_list, feature):
        """
        function to append an error tag to the list
        """
        error_list.append(feature)
        return error_list

    #############
    # fill missing values function
    def fill_missing_values(self, data, features_notmissing):
        """
        function to fill value "not provided" for any feature without data
        """
        for key in data.keys():
            if key not in features_notmissing:  # check if feature append in list features_notmissing if not fill it
                data[key].append('not provided')
        return data

    #############
    # fill input box function
    def input_box(self, driver, xpath, keys):
        """
        function to sent data to input box 
        """
        element = driver.find_element(By.XPATH, xpath)
        element.send_keys(keys)
        time.sleep(0.5)


#########################################################
# sub class
class LandsScraper_opensooq(BaseScraper):
    def __init__(self, base_url, headers):
        """
        Constructor for a subclass (inherited from a base class)
        """
        super().__init__(base_url, headers, filter_params={'main_type': 'عقارات-للبيع', 'secondary_type': 'أراضي-للبيع'}
                         , features=['وقت السحب', 'الرابط', 'القسم الرئيسي', 'القسم الفرعي', 'إعلان رقم',
                                     'الحي / المنطقة', 'المدينة',
                                     'مساحة الأرض', 'هل العقار مرهون؟', 'الواجهة', 'طريقة الدفع', 'مخصصة لـ', 'المعلن',
                                     'وقت الاعلان',
                                     'الرقم', 'السعر', 'خط العرض', 'خط الطول', 'رابط جوجل ماب', 'الوصف', 'مواقع قريبة',
                                     'العنوان',
                                     'صاحب الإعلان', 'الإعلانات', 'مشاهدات', 'متابِع', 'التقييم', 'صفحة المستخدم',
                                     'متوسط التقييم',
                                     '5 نجوم', '4 نجوم', '3 نجوم', 'نجمتان', 'نجمة', 'وصف عن المعلن',
                                     'الموقع الالكتروني للمعلن'])

    ###########
    def return_anchors_time(self, page=0):
        """
        function to extract time,url and base topic for each post in a page
        """
        print('---', page, '---')
        soup = BeautifulSoup(self.fetch("scrpae anchors and time", page), "html5lib")
        try:
            anchors = soup.find_all('a', {'class': re.compile(
                "\w+-\w+-\w+ \w+ postItem flex flexWrap mb-32 relative radius-8 grayHoverBg whiteBg boxShadow2 blackColor p-16 \d+")})
            time = anchors
            topic = soup.find_all('div', {
                'class': 'postDet flex flex-1 flexWrap flexSpaceBetween flexDirectionColumn overflowHidden ripple'})
            if not (time and anchors and topic):
                raise Exception(f"Time or url list is empty,you need  check return_anchors_time function")
        except Exception as error:
            raise Exception("error:", error, ", you need check return_anchors_time function")
        return (anchors, time, topic)

    ##############
    def scrape_id_url(self, startpage, endpage):
        """
        scrape id,time,url,and is sponser for each post at each page in website 
        """
        anchors_list = []
        ids = []
        time_list = []
        is_sponser = []
        page = startpage
        current_date = datetime.datetime.now().date()
        yesterday = current_date - timedelta(days=1)
        dict_time = {
            'قبل ساعتان': '2',
            'قبل ساعة': '1',
            'الآن': str(current_date),
            'قبل دقيقة': ' دقيقة 1',
            'قبل دقيقة واحدة': 'قبل دقيقة 1 ',
            'قبل دقيقتان': 'قبل 2 دقيقة',
            'أمس': str(yesterday)}
        try:
            while (page < endpage):  # pages loop
                print('---', page, '---')
                anchors, time, topic = self.return_anchors_time(page)
                for index, anchor in enumerate(anchors):
                    print(index)
                    second_topic = topic[index].find('div', {'class': "flex alignItems gap-5 darkGrayColor"})
                    print(second_topic.text)
                    #if re.search('أراضي للبيع', second_topic.text):
                    link = 'https://jo.opensooq.com' + anchor.get('href')
                    link = link.replace('ar//', 'ar/')
                    print(link)
                    anchors_list.append(link)
                    id_current = re.search('\d{1,20}', anchor.get('href')).group()
                    ids += [id_current]
                    time_poster = time[index].find('span',
                                                   {'class': "postDate absolute darkGrayColor font-14"})
                    if time_poster and '-' in time_poster.text:
                        time_poster = time_poster.text.split('-')
                        time_poster = '-'.join(time_poster[::-1])
                        time_list += [time_poster]
                        is_sponser += ['no']
                    elif (time_poster):
                        time_poster = time_poster.text
                        if time_poster and time_poster in dict_time.keys():
                            time_poster = dict_time[time_poster]
                            if '-' in time_poster:
                                time_list += [time_poster]
                                is_sponser += ['no']
                                continue
                        if ('دقائق' in time_poster) or ('دقيقة' in time_poster):
                            min_to_subtract = int(re.search('\d+', time_poster).group())
                            current_datetime = datetime.datetime.now()
                            time_poster = str((current_datetime - timedelta(minutes=min_to_subtract)).date())
                            time_list += [time_poster]
                            is_sponser += ['no']
                        else:

                            hours_to_subtract = int(re.search('\d+', time_poster).group())
                            current_datetime = datetime.datetime.now()
                            time_poster = str((current_datetime - timedelta(hours=hours_to_subtract)).date())
                            time_list += [time_poster]
                            is_sponser += ['no']
                    else:
                        time_list += ["not provided(sponser)"]
                        is_sponser += ['yes']
                page += 1
        except Exception as e:
            print("error in scrape_id_url function", e)
        print("Done..")

        df = pd.DataFrame({'url': anchors_list, 'poster_time': time_list, 'is_sponser': is_sponser, 'poster_id': ids})
        anchors_list = []
        ids = []
        time_list = []
        is_sponser = []
        return df
        ################

    def scrape_posters(self, data, url, session, features_notmissing, max_requests=4, sleep_time=4, driver=None):
        # extract error tags from airflow
        # errors=Variable.get('errors')
        # errors=errors.split(',')
        errors = []
        proxies = {'http': 'socks5://127.0.0.1:9050',
                   'https': 'socks5://127.0.0.1:9050'}
        if driver:
            content = self.maximum_retries_driver(driver, url, "scr_url_profile", max_req=4, sleep_time=4)
        else:
            content = self.maximum_retries(url, "scr_url_profile", max_req=4, sleep_time=4)
        soup = BeautifulSoup(content, 'lxml')
        url_profile = soup.find('a', {'class': f"{Variable.get('scr_url_profile')}"}).get('href')
        try:
            current_feature = "poster_id"
            poster_id = re.search('\d{1,20}', url).group()
            data['إعلان رقم'].append(poster_id)
            features_notmissing += ['إعلان رقم']
        except Exception as error:
            print("error in extract poster_id from scrape_posters function(it is not tag)")
            print("Error:", error)
            errors = self.update_error_list(errors, 'poster_id')
        try:
            current_feature = "price"
            value = self.retries(url, soup, 'div', {'class': f"{Variable.get('scr_price')}"})
            data['السعر'].append(value)
            features_notmissing += ['السعر']
        except Exception as error:
            print("error in extract price from scrape_posters function")
            print("Error:", error)
            errors = self.update_error_list(errors, 'price')
        try:
            current_feature = "phone_number"
            value = self.retries(url, soup, 'span', {'class': f"{Variable.get('scr_number')}"})
            data['الرقم'].append(value)
            features_notmissing += ['الرقم']
        except Exception as error:
            print("error in extract phone_number from scrape_posters function")
            print("Error:", error)
            errors = self.update_error_list(errors, 'phone_number')
        try:
            current_feature = "post owner"
            value = self.retries(url, soup, 'h3', {'class': f"{Variable.get('scr_post_owner')}"})
            data['صاحب الإعلان'].append(value)
            features_notmissing += ['صاحب الإعلان']
        except Exception as error:
            print("error in extract post owner from scrape_posters function")
            print("Error:", error)
            errors = self.update_error_list(errors, 'post owner')
        try:
            current_feature = "title"
            value = url.split('/')[-1]
            data['العنوان'].append(value)
            features_notmissing += ['العنوان']
        except Exception as error:
            print("error in extract title from scrape_posters function (it is not a scraped feature)")
            print("Error:", error)
            errors = self.update_error_list(errors, 'title')
        try:
            current_feature = "poster_information_box"
            poster_information_box = self.retries(url, soup, 'ul',
                                                  {'class': f"{Variable.get('scr_information_box')}"}, just_check=True)
            poster_information_box = poster_information_box.find_all('a')
            poster_features_box = self.retries(url, soup, 'ul',
                                               {'class': f"{Variable.get('scr_information_box')}"}, just_check=True)
            poster_features_box = poster_features_box.find_all('p')
            for iter_ in range(len(poster_information_box)):
                if poster_features_box[iter_].text in data.keys():
                    data[poster_features_box[iter_].text].append(poster_information_box[iter_].text)
                    features_notmissing += [poster_features_box[iter_].text]
        except Exception as error:
            print("error in extract poster_information_box from scrape_posters function")
            print("Error:", error)
            errors = self.update_error_list(errors, 'poster_information_box')
        try:
            current_feature = "google_map"
            value = self.retries(url, soup, 'a', {'class': "sc-750f6c2-0 dqtnfq map_google relative block mt-16"},
                                 just_check=True)
            data['رابط جوجل ماب'].append(f"{value.get('href')}")
            pattern = re.findall(r'\d{1,2}.\d{1,6}', value.get("href"))
            data['خط الطول'].append(pattern[0])
            data['خط العرض'].append(pattern[1])
            features_notmissing += ['رابط جوجل ماب']
            features_notmissing += ['خط الطول']
            features_notmissing += ['خط العرض']
        except Exception as error:
            print("error in extract google_map from scrape_posters function")
            print("Error:", error)
            errors = self.update_error_list(errors, 'google_map')
        try:
            current_feature = "poster_additional_chara"
            poster_additional_chara = self.retries(url, soup, 'li',
                                                   {'class': f"{Variable.get('scr_poster_additional_chara')}"},
                                                   just_check=True, multi_search=True)
            for _iter in range(len(poster_additional_chara)):
                data[poster_additional_chara[_iter].find('p').text].append(
                    poster_additional_chara[_iter].find('p', {'class': "width-75"}).text)
                features_notmissing += [poster_additional_chara[_iter].find('p').text]

        except Exception as error:
            print("error in extract poster_additional_chara from scrape_posters function")
            print("Error:", error)
            errors = self.update_error_list(errors, 'poster_additional_chara')
        try:
            current_feature = "describtion"
            poster_des = self.retries(url, soup, 'div',
                                      {'class': "sc-12d0d104-0 ihRZeE font-17 breakWord"}, just_check=True)
            poster_des = poster_des.find_all('p')
            clean_des = ''
            for line in poster_des:
                text_clean = line.text.replace('\n', '')
                text_clean = text_clean.replace('\xa0', '')
                text_clean = re.sub('\d+XX', ' ', text_clean)
                text_clean = re.sub('إضغط ليظهر الرقم', ' ', text_clean)
                clean_des += text_clean + " "
            data['الوصف'].append(clean_des)
            features_notmissing += ['الوصف']
        except Exception as error:
            print("error in extract description from scrape_posters function")
            print("Error:", error)
            errors = self.update_error_list(errors, 'describtion')
        try:
            current_feature = "image"
            values = self.retries(url, soup, 'div', {'class': "image-gallery-slides"}, just_check=True)
            dir = f"{Variable.get('s3_images_dir')}"
            if os.path.exists(dir):
                os.mkdir(f"{Variable.get('s3_images_dir')}" + poster_id)
                values = values.find_all('img')
                for index_image, image in enumerate(values):
                    with open(f"{Variable.get('s3_images_dir')}" + f"{poster_id}/image{index_image}.jpg",
                              'wb') as f:  ## PATH NEED EDIT
                        url = image.get('src')
                        response = requests.get(url)
                        f.write(response.content)
        except:
            print("error in extract image from scrape_posters function")
            errors = self.update_error_list(errors, 'image')
        # Variable.set('errors', errors)
        return (data, features_notmissing, url_profile, False)

    ################
    def scrape_advertiser_profile(self, data, url, session, features_notmissing, driver=None):
        # extract error tags from airflow
        # errors=Variable.get('errors')
        # errors=errors.split(',')
        errors = []
        url = 'https://jo.opensooq.com' + url
        data['صفحة المستخدم'].append(f"{url}")
        features_notmissing += ['صفحة المستخدم']
        proxies = {'http': 'socks5://127.0.0.1:9050',
                   'https': 'socks5://127.0.0.1:9050'}
        if driver:
            content = self.maximum_retries_driver(driver, url, "scr_advertiser_profile (base)", max_req=4, sleep_time=4)
        else:
            content = self.maximum_retries(url, "scr_advertiser_profile (base)", max_req=4, sleep_time=4)
        soup = BeautifulSoup(content, 'lxml')
        try:
            current_feature = "views"
            value = self.retries(url, soup, 'span', {'class': f"bold"}, index=0)
            data['مشاهدات'].append(value)
            features_notmissing += ['مشاهدات']
        except Exception as error:
            print("error in extract views from scrape_advertiser_profile function")
            print("Error:", error)
            errors = self.update_error_list(errors, 'views')
        try:
            current_feature = "follower"
            value = self.retries(url, soup, 'span', {'class': f"bold"}, multi_search=True, index=1)
            data['متابِع'].append(value)
            features_notmissing += ['متابِع']
        except Exception as error:
            print("error in extract follower from scrape_advertiser_profile function")
            print("Error:", error)
            errors = self.update_error_list(errors, 'follower')
        try:
            current_feature = "adv"
            value = self.retries(url, soup, 'div', {'class': 'sc-e5f63c27-0 iaHKms mt-32'}, just_check=True)
            # print(value)
            value = value.find('li', {"class": "selectedTab bold pb-8"})
            data['الإعلانات'].append(re.search("\d+", value.text).group())
            features_notmissing += ['الإعلانات']
        except Exception as error:
            print("error in extract adv from scrape_advertiser_profile function")
            print("Error:", error)
            errors = self.update_error_list(errors, 'adv')
        if driver:
            url = driver.current_url
        url += "?info=info"
        if driver:
            content = self.maximum_retries_driver(driver, url, "scr_advertiser_profile (info)", max_req=4, sleep_time=4)
        else:
            content = self.maximum_retries(url, "scr_advertiser_profile (info)", max_req=4, sleep_time=4)
        soup = BeautifulSoup(content, 'lxml')

        try:
            current_feature = "الموقع الالكتروني للمعلن"
            value = self.retries(url, soup, 'a', {'class': f"breakWord ltr"}, just_check=True)
            data['الموقع الالكتروني للمعلن'].append(value.get("href"))
            features_notmissing += ['الموقع الالكتروني للمعلن']
        except Exception as error:
            print("error in extract الموقع الالكتروني للمعلن from scrape_advertiser_profile function")
            print("Error:", error)
            errors = self.update_error_list(errors, 'الموقع الالكتروني للمعلن')
        try:
            current_feature = 'description_owner'
            value = self.retries(url, soup, 'p', {'class': f"darkGrayColor"})
            data['وصف عن المعلن'].append(value)
            features_notmissing += ['وصف عن المعلن']
        except Exception as error:
            print("error in extract description_owner from scrape_advertiser_profile function")
            print("Error:", error)
            errors = self.update_error_list(errors, 'description_owner')
        url = url.replace("?info=info", "?info=rating")
        if driver:
            content = self.maximum_retries_driver(driver, url, "scr_advertiser_profile (rating)", max_req=4,
                                                  sleep_time=4)
            print('break point')
        else:
            content = self.maximum_retries(url, "scr_advertiser_profile (rating)", max_req=4, sleep_time=4)
        soup = BeautifulSoup(content, 'lxml')
        try:
            value = self.retries(url, soup, 'div', {'class': 'flex alignItems gap-5 mt-8'})
            try:
                value = value.find('span').text
            except:
                value = re.search('\d+\.?\d*', value).group()
            if value:
                data['متوسط التقييم'].append(value)
                features_notmissing += ['متوسط التقييم']
                data['التقييم'].append(int(re.search('\d+', value).group()))
            features_notmissing += ['التقييم']
        except Exception as error:
            print("error in extract rating from scrape_advertiser_profile function")
            print("Error:", error)
            errors = self.update_error_list(errors, 'rating')
        try:
            current_feature = "stars"
            stars = self.retries(url, soup, 'div', {
                'class': "sc-cbb38c20-2 jiByPR width-100 flex gap-10 alignItems noWrap mb-8 center"}, just_check=True,
                                 multi_search=True)
            for star in stars:
                spans = star.find_all('span')
                if spans[0].text:
                    data[spans[0].text].append(spans[1].text)
                    features_notmissing += [spans[0].text]
        except Exception as error:
            print("error in extract stars from scrape_advertiser_profile function")
            print("Error:", error)
            errors = self.update_error_list(errors, 'stars')
        return (data, features_notmissing, False)

    #####################
    def main_scraper(self, scrape_df=None, counter=0, driver=None):
        current_data = copy.deepcopy(self.data)
        final_data = copy.deepcopy(self.data)
        errors_url = []
        errors_col = []
        anchor = scrape_df[0]
        time = scrape_df[1]
        #is_sponser = scrape_df[2]
        proxies = {'http': 'socks5://127.0.0.1:9050',
                   'https': 'socks5://127.0.0.1:9050'}
        try:
            print('start main')
            features_notmissing = []
            current_feature = 'وقت السحب'
            current_data['وقت السحب'].append(datetime.datetime.now())
            features_notmissing += ['وقت السحب']
            href_value = anchor
            href_value = 'https://jo.opensooq.com' + href_value.replace('ar//', 'ar/')
            href_value = re.sub(r"(https://jo\.opensooq\.com)+", r"https://jo.opensooq.com", href_value)
            current_data['الرابط'].append(f"{href_value}")
            features_notmissing += ['الرابط']
            session = requests.Session()
            current_feature = 'time'
            current_data['وقت الاعلان'].append(time)
            #current_data['هل البوست مدعوم؟'].append(is_sponser)
            features_notmissing += ['وقت الاعلان']
            features_notmissing += ['هل البوست مدعوم؟']
            if driver:
                current_data, features_notmissing, url_profile, check = \
                    self.scrape_posters(current_data, href_value, session, features_notmissing, driver)
            else:
                current_data, features_notmissing, url_profile, check = \
                    self.scrape_posters(current_data, href_value, session, features_notmissing)
            print('main 2 scrape poster is done!')
            if check:
                print(f"poster: {counter}", "Bad poster")
            if driver:
                current_data, features_notmissing, check = \
                    self.scrape_advertiser_profile(current_data, url_profile, session, features_notmissing, driver)
            else:
                current_data, features_notmissing, check = \
                    self.scrape_advertiser_profile(current_data, url_profile, session, features_notmissing)
            print('main 3 scrape advertiser_profile is done!')
            if check:
                print(f"poster: {counter}", "private poster")
            current_data = self.fill_missing_values(current_data, features_notmissing)
            print(f"poster: {counter}")
            print("Done..")
            df = pd.DataFrame(current_data)
            print('dataframe:', df.shape)
            current_data = copy.deepcopy(self.data)
            rename_features = dict()
            for index, key in enumerate((self.data).keys()):
                rename_features[key] = str(index + 1)
            df.rename(columns=rename_features, inplace=True)
            return (df)
        except Exception as error:
            print('Error:', error)
            for key in current_data.keys():
                print(key, ":", len(current_data[key]))
            current_data = copy.deepcopy(self.data)
            rename_features = dict()
            for index, key in enumerate((self.data).keys()):
                rename_features[key] = str(index + 1)
            df = pd.DataFrame(current_data)

            df.rename(columns=rename_features, inplace=True)
            print(df.info())
            return df

    #########
    def scrape_numbers_pages(self, my_numbers, pageStart=0, pageEnd=0, pages_error=[], edit=False):
        page = pageStart
        lastPage = False

        numbers_pool = my_numbers.split(',')
        my_password = 'test1234'
        numbers = {
            'إعلان رقم': [],
            'الرقم': []
        }
        errors_url = []
        error_page = []
        while page < pageEnd:
            if (edit == True):
                if (page in pages_error):
                    pass
                else:
                    page += 1
                    continue
            time.sleep(5)
            driver = create_driver()
            try:
                print(f'---{page}---')
                url = f'https://jo.opensooq.com/ar/عقارات-للبيع/أراضي-للبيع?page={page}'
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

                btn = WebDriverWait(driver, 20).until(
                    EC.element_to_be_clickable(  # p-8 ms-8 loginBtn whiteBtn width-auto font-20
                        (By.XPATH, f"//button[@class='{Variable.get('nmbrs_scr_login_btn')}']"))).click()

                # change country

                btn = WebDriverWait(driver, 20).until(EC.element_to_be_clickable(
                    (By.XPATH,
                     f"//button[@class='{Variable.get('nmbrs_scr_country_code')}']"))).click()

                country_list = WebDriverWait(driver, 20).until(EC.visibility_of_all_elements_located(
                    (By.XPATH, f"//li[@class='{Variable.get('nmbrs_scr_countries_list')}']")))

                target_country = country_list[16].find_elements(By.XPATH,
                                                                f"//div[@class='{Variable.get('nmbrs_scr_target_country')}']")[
                    16]
                driver.execute_script("arguments[0].click();", target_country)
                print('CLICKED', numbers_pool[0])
                #
                # input(number)
                self.input_box(driver, f"//input[@class='{Variable.get('nmbrs_scr_number_field')}']", numbers_pool[0])
                #
                # next button
                btn = WebDriverWait(driver, 20).until(
                    EC.element_to_be_clickable(
                        (
                            By.XPATH, f"//button[@class='{Variable.get('nmbrs_scr_another_login_btn')}']"))).click()

                pro = WebDriverWait(driver, 10).until(EC.presence_of_element_located(
                    (By.XPATH, f"//input[@data-id='passwordField']")))
                self.input_box(driver, f"//input[@data-id='passwordField']", my_password)
                element = WebDriverWait(driver, 5).until(
                    EC.presence_of_element_located(
                        (By.XPATH, f"//button[@class='{Variable.get('nmbrs_scr_next_btn')}']"))).click()

                print('Click Done')
                element2 = WebDriverWait(driver, 10).until(EC.presence_of_element_located(
                    (By.XPATH, f"//button[@class='{Variable.get('nmbrs_scr_another_next_btn')}']"))).click()


            # if error in any page
            except Exception as error:
                print("An exception occurred:", error)
                # error_page += [page]
                driver.quit()
                driver = create_driver()
                try:
                    print(f'---{page}---')
                    url = f'https://jo.opensooq.com/ar/عقارات-للبيع/أراضي-للبيع?page={page}'
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

                    btn = WebDriverWait(driver, 20).until(
                        EC.element_to_be_clickable(  # p-8 ms-8 loginBtn whiteBtn width-auto font-20
                            (By.XPATH, f"//button[@class='{Variable.get('nmbrs_scr_login_btn')}']"))).click()

                    #
                    # change country
                    btn = WebDriverWait(driver, 20).until(EC.element_to_be_clickable(
                        (By.XPATH,
                         f"//button[@class='{Variable.get('nmbrs_scr_country_code')}']"))).click()

                    country_list = WebDriverWait(driver, 20).until(EC.visibility_of_all_elements_located(
                        (By.XPATH, f"//li[@class='{Variable.get('nmbrs_scr_countries_list')}']")))

                    target_country = country_list[16].find_elements(By.XPATH,
                                                                    f"//div[@class='{Variable.get('nmbrs_scr_target_country')}']")[
                        16]
                    driver.execute_script("arguments[0].click();", target_country)
                    print('CLICKED', numbers_pool[0])
                    #
                    # input(number)
                    self.input_box(driver, f"//input[@class='{Variable.get('nmbrs_scr_number_field')}']",
                                   numbers_pool[0])

                    #
                    # next button
                    btn = WebDriverWait(driver, 20).until(
                        EC.element_to_be_clickable(
                            (
                                By.XPATH, f"//button[@class='{Variable.get('nmbrs_scr_another_login_btn')}']"))).click()

                    pro = WebDriverWait(driver, 10).until(EC.presence_of_element_located(
                        (By.XPATH, f"//input[@data-id='passwordField']")))
                    self.input_box(driver, f"//input[@data-id='passwordField']", my_password)
                    element = WebDriverWait(driver, 5).until(
                        EC.presence_of_element_located(
                            (By.XPATH, f"//button[@class='{Variable.get('nmbrs_scr_next_btn')}']"))).click()

                    print('Click Done')
                    element2 = WebDriverWait(driver, 10).until(EC.presence_of_element_located(
                        (By.XPATH, f"//button[@class='{Variable.get('nmbrs_scr_another_next_btn')}']"))).click()


                except Exception as error:
                    print("An exception occurred:", error)
                    error_page += [page]
            urls_ = WebDriverWait(driver, 10).until(EC.presence_of_element_located(
                (By.XPATH, f"//a[@class='{Variable.get('nmbrs_scr_urls')}']")))
            # extract url for posts in current page
            urls_ = driver.find_elements(By.XPATH, "//a[@class='block blackColor p-16']")
            #
            # extract buttons for number's posts in current page
            btns = driver.find_elements(By.XPATH, f"//div[@class='{Variable.get('nmbrs_scr_phone_btn')}']")
            #
            # click on all buttons in current page
            for j in range(len(btns)):
                # for btn in btns:
                driver.execute_script(f"arguments[0].click();", btns[j])
                # btns[j].click()
                time.sleep(1)
                # extract html source
                soup_driver = BeautifulSoup(driver.page_source, 'html5lib')
                # extract number affter click
                num = soup_driver.find_all('span', {'class': 'ltr inline'})
                time.sleep(1)
                # check if number contain 'XX'
                if ('XX' in num[j].text):
                    time.sleep(1)
                    driver.execute_script(f"arguments[0].click();", btns[j])
            #
            # extract page source and all numbers after click
            soup_driver = BeautifulSoup(driver.page_source, 'html5lib')
            num = soup_driver.find_all('span', {'class': 'ltr inline'})
            #
            # extract all numbers after click
            for j in range(len(btns)):
                # check if number contains 'XX'
                try:
                    poster_id = re.search('\d{1,20}', urls_[j].get_attribute('href'))
                    if not ('XX' in num[j].text):
                        print(num[j].text)
                        current_num = num[j].text
                        current_url = int(poster_id[0])
                    else:
                        raise Exception("error")
                # if contains append url for this post to errors url list
                except:
                    errors_url += [urls_[j].get_attribute('href')]
                # if not add to my dictionary
                else:
                    numbers['الرقم'].append(num[j].text)
                    numbers['إعلان رقم'].append(int(poster_id[0]))

            driver.quit()
            # next page
            page += 1
        # Create a DataFrame from the collected numbers
        df = pd.DataFrame(numbers)
        errors_df = pd.DataFrame(columns=['الرابط'], data=errors_url)
        print(error_page)
        return (df, errors_df, error_page)
###################END#######################

