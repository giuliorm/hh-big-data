# hh-big-data
A research on the vacancies and resumes data from Head Hunter. 
A relation of skill set and salary is estimated,
for that some intermediate steps are performed.
Used technologies & libs: Scala, Spark, Hadoop, MongoDB, Apache Lucene.

# Issues

- If you encounter exceptions from BSON mongo serializer, consider to execute
`db.collection.reIndex()` in the mongodb shell, it should

# Prerequisites

To make the code run properly, you should do the following (if not):

1) [download compiled winutils.exe](https://social.msdn.microsoft.com/forums/azure/en-US/28a57efb-082b-424b-8d9e-731b1fe135de/please-read-if-experiencing-job-failures?forum=hdinsight)

2) put this into the directory `c:\winutils\bin`

3) add this line to your code:
`System.setProperty("hadoop.home.dir", "c:\\winutils\\")`

# How to use git

## Quick instruction

1. [Download](https://git-scm.com/downloads) and install git
2. Enter command `git clone https://github.com/giuliorm/hh-crawler`
in terminal
3. Checkout to your branch `git checkout <branch_name>`,
<branch_name> is `vacancies`, `resumes`, `companies` respectively
3. Load actual changes from remote repo by `git pull origin master`
4. Make changes
5. Commit changes `git commit -m <commit message>`, <commit message> -
describe what you're committing
6. Push changes to git `git push`. Make sure you're on your branch 
by typing `git branch`
7. You're done, your changes now in the repository

## Long instruction

1. [Download](https://git-scm.com/downloads) and install git
1. Clone this repository to the desired folder by terminal command: 
`git clone https://github.com/giuliorm/hh-crawler`
1. Git structure consists of branches (like on a tree). 
The `master` branch is the main branch, but not the branch you'll 
develop in. After cloning repository to your local computer, 
change branch to the branch, which corresponds to your service. 
To change branch enter the command `git checkout <branch name>` to
terminal, where **branch name** is the name of your branch, e.g. 
    1. `vacancies` - for the vacancies service
    1. `companies` - for the companies service
    1. `resumes` - for the resumes service
1. Before you make any changes in any files, make sure your master branch
is actual. To do that, enter the command `git pull origin master`, 
 where `git pull` is a command, `origin` - the name of this repository
 as remote repository, `master` - name of the branch.
it will download all the changes from the server. You should do it
every time before you develop anything.

1. After you've finished your developing procces,you should commit
all the changes you've made. Then enter `git commit` command or select all
files, then select Git -> Commit File, it you're working in the IDE.
Enter the commit message, for example, `Authentication enabled`, which
will tell the rest, what kind of changes you've made. You can make
several commits, before `git push` (this command actually load your
changes to the remote server)

1. After you've commited everything, you can push to repository via 
`git push` command. It will load all your changes on the sever to the
specified branch. 

1. **Never load anything to `master` branch by yourself.** In commercial
systems master contains only code, that actually 'works' and
performs well without mistakes, crushes or bugs. **Also your branch mustn't
have conflicts with master branch.**



