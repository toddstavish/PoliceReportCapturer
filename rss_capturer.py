#!/usr/bin/python

import Socrata
import ConfigParser
import sys
import urllib
import time
import feedparser
from xml.dom import minidom
from googlemaps import GoogleMaps

RSS = "http://www.tampagov.net/appl_rss_feeds/rss.asp?feed=police_calls"



def create_dataset_with_columns(dataset, title = 'City Of Tampa Police Calls RSS', description = ''):
    """Creates a new Socrata dataset with columns for an RSS feed"""
    try:
        dataset.create(title, description)
    except Socrata.DuplicateDatasetError:
        print "This dataset already exists."
        return False

    dataset.add_column('Title', '', 'text', False, False, 300)
    dataset.add_column('Dispatched', '', 'date')
    #dataset.add_column('Report Number', type='number')
    dataset.add_column('Report Number', '', 'text', False, False, 300)
    dataset.add_column('Street', '', 'text', False, False, 300)
    dataset.add_column('City', '', 'text', False, False, 300)
    dataset.add_column('State', '', 'text', False, False, 300)
    dataset.add_column('Lat', '', 'text', False, False, 300)
    dataset.add_column('Long', '', 'text', False, False, 300)
    return

if __name__ == "__main__":
    # Default to Reddit for an example
    feed_url = RSS
    # Else take their feed from command line args
    if len(sys.argv) > 1:
        feed_url = sys.argv[1]
    print "Downloading RSS feed from " + str(feed_url)
    
    rss = feedparser.parse(feed_url)
    
    cfg = ConfigParser.ConfigParser()
    cfg.read('socrata.cfg')
    dataset = Socrata.Dataset(cfg)

    print "Searching for existing dataset"
    existing = dataset.find_datasets({'q':'City Of Tampa Police Calls RSS Feed Dataset',
        'for_user': dataset.username})[0]
    if existing['count'] > 0:
        print "Dataset exists, using it"
        dataset.use_existing(existing['results'][0]['id'])
    else:
        print "Creating dataset in Socrata"
        create_dataset_with_columns(dataset)

    if  dataset:
        batch_requests = []
        # Extract relevant information
        for item in rss.entries:
            data          = {}
            data['Title'] = item.title
            summary = item.summary.replace('>', ',')
            summary = summary.replace('<', ',')
            summary = summary.split(',')
            data['Dispatched'] = summary[2].replace('Dispatched: ', '')
            data['Report Number'] = summary[6].replace('Report Number: ', '')
            data['Street'] = item.links[0]['href'].split('=')[4].split('%')[0].replace('+', ' ')
            data['City'] = 'Tampa'
            data['State'] = 'FL'
            api_key = 'yourGoogleMapAPIKey'
            gmaps = GoogleMaps(api_key)
            address = data['Street'] + ", " + data['City'] + ", " + data['State']
            try:
                lat, lng = gmaps.address_to_latlng(address)
            except:
                lat = None
                lng = None
            data['Lat'] = lat
            data['Long'] = lng
            batch_requests.append(dataset.add_row_delayed(data))


        dataset._batch(batch_requests)
        print "You can now view the dataset:"
        print dataset.short_url()
    else:
        print "There was an error creating your dataset."
    
    print "\nFinished"
