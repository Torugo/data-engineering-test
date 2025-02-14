# Raizen DE Test

To resolve the test the idea was to use airflow to orchestrate an ETL pipeline and save the data in the desired format. But some challenges were found during this process.


## How to run

``docker-compose up -d``

It will launch all services necessary to run this airflow installation, webserver, scheduler, redis, backend database and worker.

## Data Source
The provided database has changed since the challenge was written, and the address is different, we should use [this](https://www.gov.br/anp/pt-br/centrais-de-conteudo/dados-estatisticos/de/vdpb/vendas-combustiveis-m3.xls) instead the one in the original readme, the source of the data has changed too. It still is an xls file, with some different fields and configurations, and it still isn't an xlsx file.

When using Python the lib OpenPyXL is capable of opening excel pivot tables saved in xlsx format and extract the raw/cached data from it, since our source is not in xlsx but in xls, was necessary to find a way to convert it to a more modern format in order to make our ETL fully automated. 

The first idea was to convert the xls file to xlsx using a dockerized Windows machine, but after some research, it seems to be not simple or reliable. With more research two solutions showed up, extract de content of the file as a zip and read its internal metadata in order to be able to extract the raw data, or convert it to ods format and read the data as sheets. To not overengineer I preferred to use the ods solution.

In order to convert the file from xls to ods, LibreOffice was used, fot it was created a [Dockerfile](libreoffice/Dockerfile) with all the dependencies, it will also be our airflow worker.

## ETL
With the worker configured, and the file converted, in order to automate this steps, two bash operators were created, one to download the data using curl and save it to a fixed path(could be an S3 path) and the second one making the conversion from xls to ods using the following command:

``soffice --headless --convert-to ods vendas-combustiveis-m3-2022.xls``

Using the converted file, some exploration was made. It was possible to see that the sheet ‘DPCache_m3 -1’(which was not present in the file originally provided) has all the necessary data for our objective. and just was necessary to drop the 'ETANOL HIDRATADO (m3)' data. 

After the exploration some steps were defined to transform our data from the pivot table into a more accessible format. The initial idea was to use Spark, but since it is not so simple to create and configure a spark environment Pandas was chosen to be used, and since the data is not huge, there isn’t any problem with it.

Some transformations were made in the data, probably the biggest transformation made was converting months columns into a variable and storing in a single column, [pandas melt](https://pandas.pydata.org/docs/reference/api/pandas.melt.html) was used saving a lot of code and work. Other minor transformations were made too, like normalizing the months and UFs.

Before saving a step of validation was implemented, in order to make a sanity check if the sum of the volumes when grouping the data was equal to the one provided in the raw data.

In order to save the data the chosen format was parquet, since it has a lot of advantages when working on a data lake environment, and has compression and partition compatibilities. The data was partitioned in ‘product’ and ‘year_month’ in order to create a useful partition without generating small partitions that could compromise the performance on read.

After all this process was possible to develop a solution that can run automatically without any human intervention. Download the data, extract the meaningful part, store it in an intermediate layer, transform it again and save in a more usable format.

## Problems found during the development

- Data source in a bad format, if the data was provided in xlsx the code would be much simpler;
- There are too many options to install libreoffice, get the right packages to make de conversion possible took some time;
- Pyarrow is not capable to save more than 1024 files from a single dataset, was necessary to use fastparquet as pandas backend for parquet;
- Airflow problems when serializing data between workers. There is a bug when using airflow with python 3.7 and 3.8, some data is pickled into the database, and mismatch of versions may cause the scheduler to die.

## Improvements

- Extract data from xls metadata. This solution looks more robust but is necessary to investigate better;
- Use spark instead of pandas. Spark is more scalable and when using a bigger data source pandas may not handle it;
- Make use of some merge strategy instead of rewriting the whole data for each new run.


ps.: There is a [notebook](discover.ipynb) with some debugging that can help understand better the development process.

