from os import access
import praw
import pandas as pd 
import json
from datetime import datetime
from airflow.models import Variable
import requests
import traceback

#can be used to scrape different elements   (just implement under submission loop)        
# print(submission.title)
# print(submission.id)
# print(submission.author)
# print(submission.created_utc)
# print(submission.score)
# print(submission.upvote_ratio)
# print(submission.url)

def read_creds_from_airflow_var():
    client_id = Variable.get("client_id")
    client_secret = Variable.get("client_secret")
    print("Secrets Retrieved")
    return client_id, client_secret

def run_reddit_etl(client_id : str, client_secret : str):
    try:
        user_agent = "Scrapper 1.0 by /u/emanuel071"
        reddit = praw.Reddit(
            client_id= client_id,
            client_secret = client_secret,
            user_agent= user_agent
            )
        print("Reddit pinged")

        # raise Exception("This is a test failure!")
    
        reddit_list = []
        for submission in reddit.subreddit('politics').hot(limit=20):
            submission_data = {"title": submission.title,
                        "id": submission.id,
                        "author": submission.author.name,
                        "created_utc": submission.created_utc,
                        "score": submission.score,
                        "upvote_ratio": submission.upvote_ratio,
                        "url": submission.url
                        }
            reddit_list.append(submission_data)
        print("Reddit data collected")
        
        return reddit_list

    except Exception as e:
        discord_alert(e, task_name="run_reddit_etl", dag_name="reddit_dag")
        return []

def discord_alert(error, task_name="unknown_task", dag_name="unknown_dag"):
    DISCORD_WEBHOOK_URL = Variable.get("discord_hook")

    error_trace = traceback.format_exc()

    # Discord allows up to 2000 characters per message; we truncate long stack traces
    if len(error_trace) > 1800:
        error_trace = error_trace[:1800] + "\n... (truncated)"

    message = {
        "content": (
            f"🚨 **Airflow Task Failed**\n"
            f"**DAG**: `{dag_name}`\n"
            f"**Task**: `{task_name}`\n"
            f"**Error**: `{str(error)}`\n"
            f"**Discord Trace**:\n```python\n{error_trace}\n```"
        )
    }


    try:
        requests.post(DISCORD_WEBHOOK_URL, json=message)
    except Exception as send_err:
        print("Failed to send Discord alert:", send_err)

############################################################################################################################
#### MAIN CODE
############################################################################################################################

def main():

    client_id, client_secret = read_creds_from_airflow_var()
    reddit_list = run_reddit_etl(client_id=client_id, client_secret=client_secret)
    df = pd.DataFrame(reddit_list)
    if len(reddit_list) > 0:
        print("Dataframe created")
        print(df.info())
        print("job done")
        # df.to_csv('./reddit_data.csv') # local file test run with notebook or cmd to see the result
    else:
        print("No data returned")
    # df.to_csv('./reddit_data.csv') # local file test run with notebook or cmd to see the result
        
    # #df.to_csv('YOUR-S3-BUCKET/reddit_data.csv')