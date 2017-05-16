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

1. Build a Bag-Of-Words model for all vacancies.
2. Transform text representation of every vacancy to numeric feature vector.
3. Form vacancies dataset with required fields.
[3a.] (This issue is optional). Perform PCA to reduce feature space of a dataset.
4. Clusterize vacancies using obtained Bag-Of-Words feature vectors (e.g. dataset).
5. For every vacancy: Retrieve a skill set.
7. For every vacancy: Compute a relevance.
8. For every skill in the each vacancy's skill set:
9. Compute a relative weight of skill.
10. Compute Pareto optimality for the most frequent skill sets.

Below a detailed explanation of every item is given.

## Build a Bag-Of-Words model for all vacancies

Bag-Of-Words - it's a vector of unique words, which are contained in the text representation among all vacancies.
In our case, the fields `name`, `description` and `key_skills` of a vacancy will be
taken to construct text representation. Consider, that Head Hunter data is in Russian, so
to handle Russian text there is a need to search libraries, which maintain Russian.
**Remember, that items below describe only required functionality, but not the 
actual code steps of the algorithm. Required logic can be implemented by several ways, and it's up to you.**

1. Remove unnecessary characters from text string for single vacancy.
2. Remove stop words. Stop words refer to the most common words in a language, which 
care no sence for text meaning (e.g. at all, usually, is, are, at least...)
3. Stemming - transform every word to its original grammatic form 
(e.g. _am, is, are_ will correspond to _be_).
4. Split text string into words.
[4a.] (Optional) Compute n-grams to complement list of words.
[5.](Optional) A threshold to the Bag-Of-Words length, if needed,
 is computed as 2/3 of the word list or 3/4 of the word list.
 
## Transform text representation of every vacancy to numeric feature vector

To transform vacancies to feature vectors, we need to transform vector
of words for every vacancy to word frequency vector among all Bag-Of-Words.

**Example:**

`(1) John likes to watch movies. Mary likes movies too.`
`(2) John also likes to watch football games.`
`Bag-Of-Words: [
    "John",
    "likes",
    "to",
    "watch",
    "movies",
    "also",
    "football",
    "games",
    "Mary",
    "too"
]`
`Feature vectors`
`(1) [1, 2, 1, 1, 2, 0, 0, 0, 1, 1]
(2) [1, 1, 1, 1, 0, 1, 1, 1, 0, 0]`

