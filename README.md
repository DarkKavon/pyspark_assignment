# PySpark Assignment

## Table of contents
* [Description](#desc)
* [How to use](#htu)
* [Files requirements](#filesreq)
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

<a href="#filesreq"></a>
## Files requirements

### Clients' dataset
* Comma-separated file
* Expected schema given below:
|Column name|Type|
|id|Integer|
|first_name|String|
|last_name|String|
|email|String|

### Financial dataset
* Comma-separated file
* Expected schema given below:
|Column name|Type|
|id|Integer|
|btc_a|String|
|cc_t|String|
|cc_n|Long|

<a href="#tech"></a>
## Technology
* Python == 3.9
* Spark == 3.3.0

<a href="#auth"></a>
## Author
* Kacper Wandel