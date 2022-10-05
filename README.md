# Fanfic ETL poject with Airflow and AWS using Lambda and Redshift 
Some years ago I read a intresting articale about a AI model that could create new poems, this was achivable since the model had a large dataset of poems. A strange idea was then created in my head, I though **"What kind of useless or funny text would you be able to create with such a model?"**. And the answer was fanfictions, and I realized that it would be a fun project to try.

I therefore started to think about some important steps, for example:<br>
1) What **cool things** should I do with the data, create fanfictions or a classification model *(seperate dirty fanfiction from normal)*?
2) Where do I **find** the data?
3) How should I **retrive** the data?
4) How should I **store** the data?
5) How should I **analyze** the data?

The five tasks above can be achived in many different ways. So I decided to try two approaches in order to learn more, the first approach will be using Airflow to get the data from a website and then I will store it in a local database and analyze the results in a dashboard using Google data studio. 7

The first approach that focus on **airflow** can be viewed below: 

This is the data 
![first_plan](https://user-images.githubusercontent.com/56206371/194145272-c8a45e6e-da82-4fe9-98ee-2ad1499fa59e.png)




The second approach is to use **AWS and make everything in the cloud!☁️** This was a fun project because I learned a lot about lambda, glue and Redshift. 
Below is second appoach which has the AWS focus:


![lambda drawio](https://user-images.githubusercontent.com/56206371/194149541-c7b71071-56a0-4640-8501-850a9fc1aeba.png)

<br>

# The first approach: Using Airflow 

I had eariler experience with Airflow so it was fast and easy to setup. There are two main things that I use airflow for and those are 1) webscraping the website https://archiveofourown.org/ and load the data into a local postgres database and 2) use airflow to calculate several KPIs parallel and post the results 
