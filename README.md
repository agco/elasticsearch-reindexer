ElasticSearch reindexer
=======================

This app allows you to reindex ElasticSearch data. Usually you want to do it when mapping changes or some discrepancy between ES and Mongo happens.

Reindexing process can be done in 2 modes:

* Reindex from ElasticSearch
* Reindex from Mongo

##Process in general

You should read the data from alias instead of specific index. This way we can do the index data into a new index while users still use the old one.
After reindexing is complete we switch aliast to point from old index to the new one.

Reindexing consists of following phases:
* Index documents from existing storage
* Index new arrivals (stuff that was added to database/ES after reindexing started)
* Switch the alias

##Reindex from ElasticSearch

This is usefull if you are sure that your index is in sync with Mongo database, but you need to change mapping.

##Reindex from Mongo

This is usefull if your index is out of sync with Mongo database.


##Where to start

Start the app, go to '/' url (i.e. your-app.heroku.com/, or localhost:9000/).
You start with a html form where you need to provide:
 
 * ElasticSearch URL
 * Type to reindex (currenlty only single type is supported)
 * Old index name
 * New index name
 * Alias name
 * ElasticSearch mapping
 * MongoDB URL (if you want to reindex from Mongo)
