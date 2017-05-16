# List of steps to perform a research

## Obtain a dataset

### What we have now

- vacancies ([download](https://drive.google.com/open?id=0B2604_FOPBUEVWdkOWtwTTF0VW8) json (~3GB))
- resumes (still downloading)

To perform data handling on a larger dataset, just copy _vacancies.json_ several times and load them to Spark.

### Structure of vacancies database

A _vacancies.json_ file contains a list of items in form of:

`{vacancy}
{vacancy}
{vacancy}
...`

where `{vacancy}` presented in form of documented [JSON HH](https://github.com/hhru/api/blob/master/docs_eng/vacancies.md) item 
(follow the link to explore the structure).

### How to import dataset from MongoDb:

`mongoexport --db database --collection col --out col.json`

where you should replace database and col with corresponding names of database and collection.
On Windows, you should go to the MongoDB's _bin_ directory and launch the command from command prompt this way:

`.\mongoexport.exe --db database --collection col --out col.json`

In your working directory, you'll see file col.json, this is the result of a collection export.

## Task

We want to explore, which amount of money every skill, required for a vacancy, presents in a salary.
For that, we should perform the following steps.

## Research steps

As you have Spark installed and working, the following steps should be performed to complete a research.
Every item of the following is explained in detail below.

1. Build a Bag-Of-Words model for every vacancy.
2. Transform every Bag-Of-Words to numeric feature vector.
3. Form vacancies dataset with required fields.
[3a.] (This issue is optional). Perform PCA to reduce feature space of a dataset.
4. Clusterize vacancies using obtained Bag-Of-Words feature vectors (e.g. dataset).
5. For every vacancy:
5.1 Retrieve a skill set.
5.2 Compute a relevance.
5.3 For every skill in the skill set:
5.4. Compute a relative weight of skill.
6. Compute Pareto optimality for the most frequent skill sets.

Below a detailed explanation of every item is given.

## Build a Bag-Of-Words model for every vacancy

Bag-Of-Words - it's a vector of strings, which contains the most frequent words
 