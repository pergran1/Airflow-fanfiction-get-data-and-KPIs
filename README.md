# Fanfic ETL project with Airflow and AWS using Lambda and Redshift 
Some years ago, I read an interesting article about an AI model that could create new poems, this was achievable since the model had a large dataset of poems. A strange idea was then created in my head, I though **"What kind of useless or funny text would you be able to create with such a model?"**. And the answer was fanfictions, and I realized that it would be a fun project to try.

I therefore started to think about some important steps, for example:
1) What **cool things** should I do with the data, create fanfictions or a classification model *(separate dirty fanfiction from normal)*?
2) Where do I **find** the data?
3) How should I **retrieve** the data?
4) How should I **store** the data?
5) How should I **analyze** the data?

The five tasks above can be achieved in many ways. So, I decided to try two approaches in order to learn more, the first approach will be using Airflow to get the data from a website and then I will store it in a local database and analyze the results in a dashboard using Google data studio. 

The first approach that focus on **airflow** can be viewed below: 

![first_plan](https://user-images.githubusercontent.com/56206371/194145272-c8a45e6e-da82-4fe9-98ee-2ad1499fa59e.png)




The second approach is to use **AWS and make everything in the cloud!☁️** This was a fun project because I learned a lot about lambda, glue and Redshift. 
Below is second approach which has the AWS focus:


![lambda drawio](https://user-images.githubusercontent.com/56206371/194149541-c7b71071-56a0-4640-8501-850a9fc1aeba.png)

<br>

# The first approach: Using Airflow 

I had earlier experience with Airflow so it was fast and easy to setup. There are two main things that I use airflow for and those are <br> 
**1)** webscraping the website https://archiveofourown.org/ and load the data into a local postgres database <br> 
**2)** Use airflow to calculate several KPIs parallel and post the results in a different table in the same postgres database. The KPIs can then easily be analyzed using any BI tool, in this project I will use Google data studio. 

The code for the Airflow dag and functions to webscrape the fanfiction-website is in **this GitHub repo**.

The raw data (with a little bit cleanup when downloading it) is stored in a postgresdatabase and looks like this: 

![fanfict_data](https://user-images.githubusercontent.com/56206371/194169721-131a87d8-e8b7-4bf8-b5b9-f84ef5ba2104.PNG)

Since it is possible that the data will grow very large over time and is not summarized in any way means that it would put a lot of pressure if the dashboard is based solely on this data. I therefore use Airflow again to execute SQL files in order to create KPIs and store those in another table.

The whole approach in Airflow looks like this: 


![airflow_all](https://user-images.githubusercontent.com/56206371/194409476-7a9c5919-031f-4ea2-8050-1ad67da4b4a3.PNG)

It is very simple to add more KPIs to the table and Airflow, just add sql files that follow the same structure and columns!

This is a example of the KPI table:

![kpi_table](https://user-images.githubusercontent.com/56206371/194410341-40d34a7b-0ff9-4438-91ad-017f0ee6dcd4.PNG)





