# eo-stac-harvester

## Introduction 
This repository contains the codes to implement the Earth Observations (EO) Harvester and transform the Spatial-Temporal Asset Catalogs (STAC) to [GeoCore](https://canadian-geospatial-platform.github.io/geocore/docs/geocore-format/). The EO collections include [Sentinel-1](https://radiantearth.github.io/stac-browser/#/external/www.eodms-sgdot.nrcan-rncan.gc.ca/stac/collections/sentinel-1?.language=en), [RADARSAT Constellation Mission (RCM)](https://radiantearth.github.io/stac-browser/#/external/www.eodms-sgdot.nrcan-rncan.gc.ca/stac/collections/rcm), National Air Photo Library (NAPL). 

## Architecture 
AWS Serverless Scatter-Gather Pattern is used to harvest and transform EO STAC items, it is a powerful approach for optimizing large data processing and enhancing performance. Below is the serverless scatter-gather architecture used for the Sentinel-1 collection. 
![ServerlessScatterGather](https://github.com/Canadian-Geospatial-Platform/eo-stac-harvester/assets/103012417/eb295881-20ee-43e6-aaf7-901fa5d3cfc4)
