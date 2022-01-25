# OnlineStore

# Getting Started


## Installation and Run the Project

Create a new anaconda environment

```
$ conda create --name myenv
$ activate myenv
$ conda install jupyter
```
Install the requirements.txt to the enviroment
```
$ conda install --file requirements.txt

```

or create environment from the requirements.txt 
```

$ conda create --name myenv --file requirements.txt

```

## Set up Big Query on Google Cloud

1. Follow the [Instructions] to create a project and Enable the BigQuery API for the specific project.
2. Follow the [Authentication Instructions] to create a service account.
3. A json file is created in order to use it for the project authentication.


## Project Description

The sample dataset provides an obfuscated Google Analytics 360 dataset that can be accessed via BigQuery. It’s a great way to look at business data and experiment and learn the benefits of analyzing Google Analytics 360 data in BigQuery.


# Sources
## Big Query

- [General Documentation]
- [Running Parametrized Queries]
- [Query multiple tables using a wildcard table]
- [Export Guide]
- [How to query and calculate Google Universal Analytics data in BigQuery]

## Other sources
- [Conversion rate]

[Instructions]: <https://cloud.google.com/bigquery/docs/quickstarts/quickstart-cloud-console>
[Authentication Instructions]: <https://cloud.google.com/docs/authentication/getting-started>

[General Documentation]: <https://cloud.google.com/bigquery/docs>
[Running Parametrized Queries]: <https://cloud.google.com/bigquery/docs/parameterized-queries#python>
[Query multiple tables using a wildcard table]: <https://cloud.google.com/bigquery/docs/querying-wildcard-tables>
[Export Guide]: <https://support.google.com/analytics/topic/3416089?hl=en&ref_topic=2430414>
[How to query and calculate Google Universal Analytics data in BigQuery]: <https://towardsdatascience.com/how-to-query-and-calculate-google-analytics-data-in-bigquery-cab8fc4f396>
[Conversion rate]: <https://support.google.com/analytics/answer/6014873?hl=en#conversion-rate&zippy=%2Cin-this-article>





