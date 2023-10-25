import requests
import pandas as pd
import json

subreddit = 'jokes'
limit = 100
timeframe = 'month' #hour, day, week, month, year, all
listing = 'hot' # controversial, best, hot, new, random, rising, top

def run_reddit_etl():

    def get_reddit(subreddit,listing,limit,timeframe):
        try:
            base_url = f'https://www.reddit.com/r/{subreddit}/{listing}.json?limit={limit}&t={timeframe}'
            request = requests.get(base_url, headers = {'User-agent': 'yourbot'})
        except:
            print('An Error Occured')
        return request.json()


    def get_post_titles(r):
        '''
        Get a List of post titles
        '''
        posts = []
        for post in r['data']['children']:
            x = post['data']['title']
            posts.append(x)
        return posts


    def get_results(r):
        #Create a DataFrame Showing Title, URL, Score and Number of Comments.
        myDict = {}
        for post in r['data']['children']:
            myDict[post['data']['title']] = {'url':post['data']['url'],'score':post['data']['score'],'comments':post['data']['num_comments']}
        df = pd.DataFrame.from_dict(myDict, orient='index')
        df.to_csv("D:/Airflow_Project/Reddit_hot_100_month_jokes.csv")
        return df
    
    r = get_reddit(subreddit,listing,limit,timeframe)
    df = get_results(r)

if __name__ == '__main__':
    run_reddit_etl()