from flask import Flask, render_template, request, jsonify, Response, url_for, redirect
from flask_cors import CORS, cross_origin
from mongoDBOperations import MongoDBManagement
from logger_class import getLog
import wikipedia
import re
from bs4 import BeautifulSoup
import requests



db_name = 'wikipedia-page'
collection_name = 'topicdetails'
logger = getLog('wikiscrapping.py')


# initialising the flask app with the name 'app'
app = Flask(__name__)


@app.route('/', methods = ['GET'])
@cross_origin()
def homePage():
    return render_template('index.html')


@app.route('/summary', methods=['POST', 'GET'])
@cross_origin()
def index():
    if request.method == 'POST':

        # obtaining the search string entered in the form
        searchString = request.form['search']
        s1 = searchString.replace(' ','').lower()
        s2 = searchString.replace('_','').lower()
        try:
            mongoClient = MongoDBManagement(username='mangodb', password='mangodb')
            logger.info(f"connection stable with database...")
            if mongoClient.checkRecordOnQuery(db_name=db_name, collection_name=collection_name,query={"$or": [{"topic": searchString}, {"topic": s1},{ "topic": s2}]}):

                logger.info(f"checked record in database...")
                result = mongoClient.findRecordOnQuery(db_name=db_name, collection_name=collection_name,query={"$or": [{"topic": searchString}, {"topic": s1},{ "topic": s2}]})

                logger.info(f"fetch record from database for {searchString}")

                return render_template('results.html', result=result)

            else:
                result = new_topic(searchString)
                logger.info(f"scrapped data from wikipedia for {searchString}")
                mongoClient.insertRecord(db_name=db_name,
                                         collection_name=collection_name,
                                         record=result)
                logger.info(f"inserted record in database...")
                return render_template('results.html', result=result)

        except Exception as e:
            print("(app.py) - Something went wrong while rendering all the details of topic.\n" + str(e))
            return redirect(url_for('topic_suggestion'))

    else:
        return render_template('index.html')



def new_topic(searchString):
    try:
        summary = wikipedia.summary(searchString, sentences=5)
        topic_page = wikipedia.page(searchString)
        refs = topic_page.references[:5]
        pic = new_pic(topic_page)
        imgs = new_imgs(topic_page)
        st = searchString.replace(' ','').lower()
        st1 = st.replace('_','')
        result = {'topic': st1,
                  'summary': summary,
                  'refs': refs,
                  'pic':pic,
                  'imgs': imgs
                  }

        return result

    except Exception as e:
        raise Exception("(new_topic) - Something went wrong on retrieving new_topic.\n" + str(e))


def new_pic(topic_page):
    try:
        img = []
        r = requests.get(topic_page.url)
        soup = BeautifulSoup(r.content, features="html.parser")
        covers = soup.select('table.infobox a.image img[src]')
        for cover in covers:
            img.append(cover['src'])
        if len(img)<1:
            img.append('no image')
        return img
    except Exception as e:
        raise Exception("(image) - Something went wrong on retrieving image.\n" + str(e))

def new_imgs(topic_page):
    try:
        im = []
        for i in range(len(topic_page.images)):
            match = re.search('^https?://(?:[a-z0-9\-]+\.)+[a-z0-9]{2,6}(?:/[^/#?]+)+\.(?:jpg|gif|png)$',
                              topic_page.images[i])
            if match and len(im) < 5:
                im.append(topic_page.images[i])
        return im
    except Exception as e:
        raise Exception("(new_imgs) - Something went wrong on retrieving images.\n" + str(e))


@app.route('/topic_suggestion', methods=[ 'GET'])
@cross_origin()
def topic_suggestion():
    try:
        return "Unable to search the topic check for spelling mistake/ search topic without spaces.  Eg. 'web scrapping' search as webscrapping or web_scrapping"
    except:
        raise Exception("(topic_suggestion) - Something went wrong on topic_suggestion.\n" + str(e))




if __name__ == '__main__':
    app.run(debug=True)