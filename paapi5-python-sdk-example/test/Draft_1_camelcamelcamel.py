##from webdriver_auto_update import check_driver
from selenium import webdriver
from webdriver_manager.chrome import ChromeDriverManager
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.service import Service as ChromiumService
def chrome_and_webdriver_update():
    try:
        driver = webdriver.Chrome(ChromeDriverManager(path=r"C:\\Program Files (x86)").install())
        return True, driver
    except Exception as e:
        print("Error found: ", e)
        return False

def parse_webpage(driver, url):
    print("tbd")
    driver.get(url)



# check_driver("C:\\Program Files (x86)")
# driver = webdriver.Chrome('C:\Program Files (x86)\chromedriver.exe')
# driver.get("https://camelcamelcamel.com/popular.xml?deal=1")
# assert "Python" in driver.title
# elem = driver.find_element(By.NAME, "q")
# elem.clear()
# elem.send_keys("pycon")
# elem.send_keys(Keys.RETURN)
# assert "No results found." not in driver.page_source
# driver.close()


if __name__ == "__main__":
    update, driver = chrome_and_webdriver_update()
    if (update):
        url = "https://camelcamelcamel.com/popular.xml?deal=1"
        parse_webpage(driver, url)
