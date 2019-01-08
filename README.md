# Spreaker Data Access Layer



## Entities and Responsabilities


### Model

A model is responsible to hold data and to provide methods to manipulate the data itself. These methods CANNOT manipulate data on related models (relations). If an operation need to manipulate data on multiple models, it MUST be implemented outside the model, for example in a Service.

What IS:
* Hold and export data related to a single entity
* Hold related models
* Provide methods to operate on model's data itself
* Provide methods to set() and get() related models

What is NOT:
* Do not provide methods to operate on related model's data


### Repository

A repository is responsible to provide methods to fetch models from the data storage. It SHOULD be a *pure* class **without state**, so its methods SHOULD be *static*.

What IS:
* Provide methods to fetch models from the data storage
* Hide which data storage is used to get data (ex. database)


### Data Storage

A data storage, like the name suggests, is the place where data is stored. It could be a relation database, a key-value store, a file, etc. The specific data storage implementation provides methods used by the Repository to manipulate data on the storage.


### Database Manager

The database manager is a **specific Data Storage** that operates on PostgreSQL.


### Relation Manager

The relation manager is a general purpose component that is able to *combine* and *map* relations to models. It SHOULD be used


## Tests

### Setup database on localhost

```
CREATE ROLE daluser LOGIN ENCRYPTED PASSWORD 'md5f3155ffd1dcb04324e36ef239773fbee' VALID UNTIL 'infinity';
CREATE DATABASE dal_test WITH ENCODING='UTF8' OWNER=daluser;
CREATE DATABASE dal_test_1 WITH ENCODING='UTF8' OWNER=daluser;
CREATE DATABASE dal_test_2 WITH ENCODING='UTF8' OWNER=daluser;
```

```
cd /workspace/www-site/lib/vendor/spreaker/dal/test
./runAllTests.sh
```