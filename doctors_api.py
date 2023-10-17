from flask import Flask, jsonify, request
import sqlite3
from bs4 import BeautifulSoup
import time
from selenium import webdriver

from selenium.webdriver.chrome.options import Options

app = Flask(__name__)
options = Options()
options.add_argument('--headless')
driver = webdriver.Chrome(options=options)

conn = sqlite3.connect('doctors.db')
cursor = conn.cursor()
cursor.execute('''
    CREATE TABLE IF NOT EXISTS doctors (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        ProviderName TEXT,
        PrimarySpecialties TEXT,
        LocationName TEXT,
        streetName TEXT,
        city TEXT,
        state TEXT,
        zipcode TEXT,
        distance TEXT,
        OfficeNumber TEXT,
        About TEXT,
        ProfileLink TEXT,
        ProfileImageLink TEXT,
        Rating_OutOf5 TEXT,
        RatingCount TEXT,
        CommentCount TEXT
    )
''')
conn.commit()
conn.close()


def get_page_data(link, driver):
    all_data = []
    driver.get(link)
    time.sleep(4)
    s1 = BeautifulSoup(driver.page_source)
    # all doctors
    divs = s1.find_all('div', {'class': 'card card-body provider'})
    print(len(divs))
    for div in divs:
        data = {}
        # getting providerName
        name = div.find('h3').find("a").text.strip()
        data['ProviderName'] = name

        # getting PrimarySpecialties
        try:
            pri_spec = div.find("p", {"class": 'prov-descr mt-05 mb-1'}).text.strip()

        except:
            try:
                pri_spec = div.find("p", {"class": 'prov-title mt-05 mb-1 mbl-hddn'}).text.strip()
            except:
                pri_spec = div.find("p", {"class": 'prov-specialties'}).text.strip()
        if "Locations" in pri_spec:
            try:
                pri_spec = div.find("p", {"class": 'prov-descr mt-05 mb-1'}).span.text.strip()
            except:
                pri_spec = None
        data['PrimarySpecialties'] = pri_spec

        # getting locName
        try:
            ln = div.find("h4", {"class": 'mt-0 semibold txt-15'}).text.strip()
            data['LocationName'] = ln
        except:
            data['LocationName'] = None

        try:
            # getting loc-address
            # street
            street = div.find("span", {"class": 'addr-span-street'}).text.strip()

            # city
            city = div.find("span", {"class": 'addr-span-city'}).text.strip()

            # state
            state = div.find("span", {"class": 'addr-span-state'}).text.strip()

            # zip
            zipcode = div.find("span", {"class": 'addr-span-zip'}).text.strip()

            data['streetName'] = street
            data['city'] = city
            data['state'] = state
            data['zipcode'] = zipcode
        except:
            # getting loc-address
            try:
                address = div.find("div", {"class": 'light mt-0 psp-only'}).text.strip().replace("\n\n", "\n").split(",")


                if len(address) > 3:
                    data['streetName'] = ",".join(address[0:1]).strip()
                    data['city'] = address[2].strip()
                    s, z = address[3].strip().split(" ")
                    data['state'] = s.strip()
                    data['zipcode'] = z.strip()
                else:
                    data['streetName'] = address[0].strip()
                    data['city'] = address[1].strip()
                    s, z = address[2].strip().split(" ")
                    data['state'] = s.strip()
                    data['zipcode'] = z.strip()
            except:
                data['streetName'] = None
                data['city'] = None
                data['state'] = None
                data['zipcode'] = None
        # getting distance
        data['distance'] = div.find("p", {"class": 'mt-0 mb-1 txt-14'}).text.strip()

        # getting Office Number
        try:
            data['OfficeNumber'] = div.find("div", {"class": 'phone-section'}).a.text.strip()
        except:
            data['OfficeNumber'] = None
        # getting about
        try:
            data['About'] = div.find("div", {"class": 'mt-1 ldp-only line-clamp-5'}).text.strip()
        except:
            data['About'] = None

        # getting profile link
        prolink = div.find("a", {"class": 'btn-primary'})['href']
        if "https://" not in prolink:
            data['ProfileLink'] = "https://www.care.piedmont.org"+ prolink
        else:
            data['ProfileLink'] = prolink

            # getting profile Image
        data['ProfileImageLink'] = div.find("img", {"class": 'provider-img'})['src']

        # rating
        try:
            rat = div.find("span", {"class": 'ratingsmd-avg'}).text.strip()
            data['Rating_OutOf5'] = rat
        except:
            data['Rating_OutOf5'] = None

        # rating-count
        try:
            ratcount = div.find("span", {"class": 'ratingsmd-rating-cnt'}).text.strip()
            data['RatingCount'] = ratcount
        except:
            data['RatingCount'] = None

        # comment count
        try:
            comcount = div.find("span", {"class": 'ratingsmd-comment-cnt'}).text.strip()
            data['CommentCount'] = comcount
        except:
            data['CommentCount'] = None

        all_data.append(data)

    return all_data

# Endpoint to run the scraper
@app.route('/scrape/<int:page_count>')
def scrape_data(page_count):
    global driver


    for page in range(1, page_count+1):
        all_data = get_page_data(f"https://www.care.piedmont.org/providers/?location=Norcross%2C+GA&page={page}", driver)
        print("Done page", page, len(all_data))

    conn = sqlite3.connect('doctors.db')
    cursor = conn.cursor()

    # Insert new records into DB
    for data in all_data:
        # Check if the record already exists (use a unique identifier)
        cursor.execute('SELECT id FROM doctors WHERE ProviderName = ? AND PrimarySpecialties = ?',
                       (data['ProviderName'], data['PrimarySpecialties']))
        existing_record = cursor.fetchone()

        if existing_record is None:
            cursor.execute('''
                    INSERT INTO doctors (ProviderName, PrimarySpecialties, LocationName, streetName, city, state, zipcode, distance, OfficeNumber, About, ProfileLink, ProfileImageLink, Rating_OutOf5, RatingCount, CommentCount)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ''', (
                data['ProviderName'], data['PrimarySpecialties'], data['LocationName'], data['streetName'],
                data['city'],
                data['state'], data['zipcode'], data['distance'], data['OfficeNumber'], data['About'],
                data['ProfileLink'],
                data['ProfileImageLink'], data['Rating_OutOf5'], data['RatingCount'], data['CommentCount']
            ))

    conn.commit()
    conn.close()

    return 'Scraping completed, and new data stored in the database'


# Endpoint to retrieve a record by ID
@app.route('/get_record/<int:record_id>')
def get_record(record_id):
    conn = sqlite3.connect('doctors.db')
    cursor = conn.cursor()
    cursor.execute('SELECT * FROM doctors WHERE id = ?', (record_id,))
    record = cursor.fetchone()
    conn.close()

    if record is not None:
        return jsonify({
            'id': record[0],
            'ProviderName': record[1],
            'PrimarySpecialties': record[2],
            'LocationName': record[3],
            'streetName': record[4],
            'city': record[5],
            'state': record[6],
            'zipcode': record[7],
            'distance': record[8],
            'OfficeNumber': record[9],
            'About': record[10],
            'ProfileLink': record[11],
            'ProfileImageLink': record[12],
            'Rating_OutOf5': record[13],
            'RatingCount': record[14],
            'CommentCount': record[15]
        })
    else:
        return 'Record not found'


if __name__ == "__main__":
    app.run()
