from selenium.webdriver.firefox.options import Options
from selenium import webdriver


def create_driver():
    options = Options()
    # options.add_argument("--headless")
    # options.add_argument('--blink-settings=imagesEnabled=false')
    profile = webdriver.FirefoxProfile()

    # Socks5 Host SetUp:-
    myProxy = "127.0.0.1:9050"
    ip, port = myProxy.split(':')
    profile.set_preference('network.proxy.type', 1)
    profile.set_preference('network.proxy.socks', ip)
    profile.set_preference('network.proxy.socks_port', int(port))
    profile.set_preference('permissions.default.image', 2)
    options.profile = profile
    driver = webdriver.Firefox(options=options)  # ,firefox_profile=profile)
    # driver.get('https://httpbin.org/ip')
    # ip_element = driver.find_element_by_xpath('//pre')

    # Extract and print the IP address
    # ip_address = ip_element.text
    # print("Your IP address is:", ip_address)
    return driver