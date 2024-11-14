import json
import requests
import boto3
from datetime import datetime
import os
import csv
from bs4 import BeautifulSoup
import uuid

# Initializing DynamoDB resource
dynamodb = boto3.resource('dynamodb')
table = dynamodb.Table('JobScraperTable')

def lambda_handler(event, context):
    url = event["html"]
    if isinstance(url, list):
        url = url[0]  # Ensuring url is a string if it's passed as a list
    bucket = "greenhub-bucket"
    file_name = "/tmp/job_details.csv"  # Defining a static filename for the CSV
    download_and_upload_csv(url, bucket, file_name)
    return {
        'statusCode': 200,
        'body': json.dumps('Job details have been scraped, saved in CSV, uploaded to S3, and stored in DynamoDB!')
    }

def download_and_upload_csv(url, bucket, file_name):
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/94.0.4606.61 Safari/537.36'
    }
    response = requests.get(url, headers=headers, timeout=30)
    if response.status_code == 200:
        html_content = response.text
    else:
        raise Exception(f"Failed to download HTML content. Status code: {response.status_code}")
    
    soup = BeautifulSoup(html_content, "html.parser")
    
    # Seletor based on example HTML structure
    jobs = soup.find_all("div", class_="sc-beqWaB gupdsY job-card")  # selector for job card divs

    # Open the CSV file in append mode to add new data to the existing file(This is to avoid rereating new sv files all the time the shedule runs)
    with open(file_name, mode='a', encoding='utf-8', newline='') as file:
        writer = csv.writer(file)
        
        # Write the header row only if the file is empty
        if file.tell() == 0:
            writer.writerow(["Job Title", "Company Name", "Location", "Date Posted", "Job Link"])
        
        for job in jobs:
            job_title_tag = job.find("div", itemprop="title")
            job_title = job_title_tag.text.strip() if job_title_tag else "N/A"
            
            company_name_tag = job.find("a", {"data-testid": "link"})
            company_name = company_name_tag.text.strip() if company_name_tag else "N/A"
            
            #  location extraction
            location_tag = job.find("meta", itemprop="address")
            location = location_tag['content'].strip() if location_tag and location_tag.has_attr('content') else "N/A"
    
            date_posted_tag = job.find("div", class_="sc-beqWaB enQFes")
            date_posted = date_posted_tag.text.strip() if date_posted_tag else "N/A"
            
            job_link_tag = job.find("a", {"data-testid": "job-title-link"}, href=True)
            job_link = job_link_tag['href'] if job_link_tag else "N/A"
            if not job_link.startswith(('http://', 'https://')):
                job_link = f"https://climatejobs.shortlist.net{job_link}"

            # Write to CSV
            writer.writerow([job_title, company_name, location, date_posted, job_link])
            
            # Store each job in DynamoDB
            job_id = str(uuid.uuid4())  # Generate a unique ID for each job
            try:
                table.put_item(
                    Item={
                        'job_id': job_id,
                        'job_title': job_title,
                        'company_name': company_name,
                        'location': location,
                        'date_posted': date_posted,
                        'job_link': job_link
                    }
                )
                print(f"Job {job_title} saved to DynamoDB with ID {job_id}.")
            except Exception as e:
                print(f"Failed to save job to DynamoDB: {e}")
    
    # Upload the single CSV file to S3
    s3_client = boto3.client('s3')
    s3_file_path = f"Jobs/job_details.csv"  # Keep the S3 path static as well
    
    try:
        s3_client.upload_file(file_name, bucket, s3_file_path)
        print(f"CSV file '{file_name}' uploaded successfully to S3 bucket '{bucket}' in the 'Jobs' folder.")
    except Exception as e:
        print(f"Failed to upload CSV file to S3: {e}")
    finally:
        os.remove(file_name)
