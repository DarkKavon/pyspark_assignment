# CODAC Assignment

## Table of contents
* [Description](#desc)
* [How to use](#htu)
* [Technology](#tech)
* [Author](#auth)

<a href="#desc"></a>
## Description
This tool takes two comma-separated files and list of expected countries. Program removes PII, filters countries and joins both datasets, then it saves result dataset in project location.

<a href="#htu"></a>
## How to use 
To use run ```main.py``` with three arguments:
* clients' data
* financial data         
* list of countries 

Example:
```
python main.py "file/path/to/client/dataset" "file/path/to/financial/dataset" "[list,of,expected,Scountries]"
```

<a href="#tech"></a>
## Tech
* Python == 3.9
* Spark == 3.3.0

<a href="#auth"></a>
## Author
* Kacper Wandel