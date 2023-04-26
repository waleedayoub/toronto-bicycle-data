/* Create tables for each year of ridership data */

CREATE OR REPLACE EXTERNAL TABLE `toronto_bikeshare.t2016_ext`
OPTIONS (
  format = 'parquet',
  uris = ['gs://dtc-toronto-bikeshare_possible-lotus-375803/data/toronto/ridership/2016/tor_trips_2016_*.parquet']
);

/* create external table */
CREATE OR REPLACE EXTERNAL TABLE `toronto_bikeshare.t2017_ext`
OPTIONS (
  format = 'parquet',
  uris = ['gs://dtc-toronto-bikeshare_possible-lotus-375803/data/toronto/ridership/2017/*.parquet']
);

/* create external table */
CREATE OR REPLACE EXTERNAL TABLE `toronto_bikeshare.t2018_ext`
OPTIONS (
  format = 'parquet',
  uris = ['gs://dtc-toronto-bikeshare_possible-lotus-375803/data/toronto/ridership/2018/*.parquet']
);

/* create external table */
CREATE OR REPLACE EXTERNAL TABLE `toronto_bikeshare.t2019_ext`
OPTIONS (
  format = 'parquet',
  uris = ['gs://dtc-toronto-bikeshare_possible-lotus-375803/data/toronto/ridership/2019/*.parquet']
);

/* create external table */
CREATE OR REPLACE EXTERNAL TABLE `toronto_bikeshare.t2020_ext`
OPTIONS (
  format = 'parquet',
  uris = ['gs://dtc-toronto-bikeshare_possible-lotus-375803/data/toronto/ridership/2020/*.parquet']
);

/* create external table */
CREATE OR REPLACE EXTERNAL TABLE `toronto_bikeshare.t2021_ext`
OPTIONS (
  format = 'parquet',
  uris = ['gs://dtc-toronto-bikeshare_possible-lotus-375803/data/toronto/ridership/2021/*.parquet']
);

/* create external table */
CREATE OR REPLACE EXTERNAL TABLE `toronto_bikeshare.t2022_ext`
OPTIONS (
  format = 'parquet',
  uris = ['gs://dtc-toronto-bikeshare_possible-lotus-375803/data/toronto/ridership/2022/*.parquet']
);