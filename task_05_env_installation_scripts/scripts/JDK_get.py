#/usr/bin/env python

from sys import argv, stderr, exit
from selenium.webdriver import Firefox
from selenium.webdriver import FirefoxProfile
from selenium.webdriver.firefox.options import Options
from selenium.webdriver.common.action_chains import ActionChains
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.common.by import By
from os import getcwd 
import time

#Browser Setup

try:
    fp = FirefoxProfile()
    fp.set_preference("browser.download.dir",getcwd())
    fp.set_preference("browser.download.folderList", 2)
    fp.set_preference('browser.download.manager.showWhenStarting', False)   
    fp.set_preference('browser.download.useDownloadDir', True)
    fp.set_preference("browser.helperApps.neverAsk.saveToDisk", 'application/x-gzip')
    opts = Options()
    opts.headless = True
    browser = Firefox(firefox_profile=fp, options=opts)
    actions = ActionChains(browser)
except:
    print >> stderr, "Browser object failed to properly initialize."
    exit()

#navigate landing page
try:
    browser.get('https://www.oracle.com/java/technologies/javase-jdk8-downloads.html')
except:
    print >> stderr, "Could not reach jdk8 landing page."
    exit()

#select cookie consent iframe
try:
    WebDriverWait(browser, 20).until(EC.presence_of_element_located((
        By.XPATH, '//*[@title="TrustArc Cookie Consent Manager"]'
        )))
    browser.switch_to.frame(browser.find_element(
        By.XPATH, '//*[@title="TrustArc Cookie Consent Manager"]'
        ))

#click cookie consent button 
    WebDriverWait(browser, 20).until(EC.element_to_be_clickable((
        By.XPATH, "/html/body/div[8]/div[1]/div/div[2]/div[2]/a[1]" 
        ))).click()
except:
    print >> stderr, "Cookie consent iframe interaction failed."
    exit()

#click linux x64 button
try:
    browser.switch_to.default_content()
    browser.execute_script("window.scrollTo(0, 873)")
    actions.move_to_element(
        browser.find_element(
            By.XPATH, '//*[@data-file="//download.oracle.com/otn/java/jdk/8u251-b08/3d5a2bb8f8d4428bbe94aed7ec7ae784/jdk-8u251-linux-x64.tar.gz"]'
            )).click().perform()
except:
    print >> stderr, "Could not interact with jdk-8u251-linux-x64 button."
    exit()

#click java license agreement checkbox
try:
    actions.move_to_element(
        browser.find_elements(
            By.XPATH, '//input[@type="checkbox"]')[1]
        ).click().perform()

#click java license acceptance button
    WebDriverWait(browser, 20).until(EC.invisibility_of_element((
        By.CLASS_NAME, "oform-bttns center-bttns"
        )))
    browser.execute_script(
        "arguments[0].click();", WebDriverWait(browser, 20).until(
            EC.element_to_be_clickable((
                By.XPATH, '//*[@href="https://www.oracle.com/webapps/redirect/signon?nexturl=https://download.oracle.com/otn/java/jdk/8u251-b08/3d5a2bb8f8d4428bbe94aed7ec7ae784/jdk-8u251-linux-x64.tar.gz"]'
                ))))
except:
    print >> stderr, "Could not interact correctly with java license agreement form."
    exit()

#wait until login page loads
try:
    WebDriverWait(browser, 20).until(EC.presence_of_element_located((By.ID, "sso_username")))

#enter login information
    username = browser.find_element(By.ID, 'sso_username')
    password = browser.find_element(By.ID, 'ssopassword')
    username.send_keys(argv[1])
    password.send_keys(argv[2])
    browser.find_element(By.XPATH, "/html/body/div/div[3]/div[1]/form/div[2]/span/input").click()

except:
    print >> stderr, "oracle sso login page failed to load or could could not be correctly interacted with."
    exit()

#wait for download to finish and end browser process
time.sleep(5)
browser.get("about:downloads")

try:
    dl = browser.find_elements(By.CLASS_NAME, 'download.download-state')[0]
except:
    print >> stderr, "JDK download failed to start. If you see this error, please ensure your credentials were correctly entered."
    exit()

max_wait = time.time() + 3600
while time.time() < max_wait:
    if dl.get_attribute("state") != "0": 
        browser.close()
        del browser
        exit()
    time.sleep(0.5)
print >> stderr, "JDK download timed out."
browser.close()
del browser
exit()
