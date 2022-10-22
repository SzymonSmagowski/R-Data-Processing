install.packages("sqldf")
install.packages("microbenchmark")
install.packages("data.table")
library(sqldf)
library(data.table)

#installing packages
#check function - checking every field of data frame, if one is false then result is false

check <- function(a){
  c = all(any(a==FALSE))
  r= all(any(a==FALSE,2))
  return (!(c&r))
}

#changing directory
getwd()
setwd("/Users/Szymon/Desktop/rstudio")

# loading csv files to data frames
Badges<-read.csv(file = 'badges.csv')
Users<-read.csv(file = 'Users.csv')
Posts<-read.csv(file = 'Posts.csv')
Votes<-read.csv(file = 'Votes.csv')

# setting every good solution

BadgesT1Good<-sqldf("SELECT
      Name,
      COUNT(*) AS Number,
      MIN(Class) AS BestClass
      FROM Badges
      GROUP BY Name
      ORDER BY Number DESC
      LIMIT 10")

T2Good <- sqldf("SELECT Location, COUNT(*) AS Count
FROM (
                SELECT Posts.OwnerUserId, Users.Id, Users.Location
                FROM Users
                JOIN Posts ON Users.Id = Posts.OwnerUserId
)
WHERE Location NOT IN ('')
GROUP BY Location
ORDER BY Count DESC
LIMIT 10")

T3Good <- sqldf("SELECT
Users.AccountId,
                Users.DisplayName,
                Users.Location,
                AVG(PostAuth.AnswersCount) as AverageAnswersCount
                FROM
                (
                SELECT
                AnsCount.AnswersCount,
                Posts.Id,
                Posts.OwnerUserId
                FROM (
                SELECT Posts.ParentId, COUNT(*) AS AnswersCount
                FROM Posts
                WHERE Posts.PostTypeId = 2
                GROUP BY Posts.ParentId
                ) AS AnsCount
                JOIN Posts ON Posts.Id = AnsCount.ParentId
                ) AS PostAuth
                JOIN Users ON Users.AccountId=PostAuth.OwnerUserId
                GROUP BY OwnerUserId
                ORDER BY AverageAnswersCount DESC
                LIMIT 10")

T4Good <- sqldf("SELECT
Posts.Title,
                UpVotesPerYear.Year,
                MAX(UpVotesPerYear.Count) AS Count
                FROM (
                SELECT
                PostId,
                COUNT(*) AS Count,
                STRFTIME('%Y', Votes.CreationDate) AS Year
                FROM Votes
                WHERE VoteTypeId=2
                GROUP BY PostId, Year
                ) AS UpVotesPerYear
                JOIN Posts ON Posts.Id=UpVotesPerYear.PostId
                WHERE Posts.PostTypeId=1
                GROUP BY Year
                ORDER BY Year ASC")

T5Good<-sqldf("SELECT
Posts.Title,
              VotesByAge2.OldVotes
              FROM Posts
              JOIN (
              SELECT
              PostId,
              MAX(CASE WHEN VoteDate = 'new' THEN Total ELSE 0 END) NewVotes,
              MAX(CASE WHEN VoteDate = 'old' THEN Total ELSE 0 END) OldVotes,
              SUM(Total) AS Votes
              FROM (
              SELECT
              PostId,
              CASE STRFTIME('%Y', CreationDate)
              WHEN '2021' THEN 'new'
              WHEN '2020' THEN 'new'
              ELSE 'old'
              END VoteDate,
              COUNT(*) AS Total
              FROM Votes
              WHERE VoteTypeId IN (1, 2, 5)
              GROUP BY PostId, VoteDate
              ) AS VotesByAge
              GROUP BY VotesByAge.PostId
              HAVING NewVotes=0
              ) AS VotesByAge2 ON VotesByAge2.PostId=Posts.ID
              WHERE Posts.PostTypeId=1
              ORDER BY VotesByAge2.OldVotes DESC
              LIMIT 10")

#===================================================================================================

# in the beginning of every task there is convertion of data frame to data table

badges <- data.table(Badges)

# grouping by Name, creating new columns that calculates rows and min
res<- badges[,list(Number=.N,BestClass=min(Class)),by="Name"]
# setting up order
res <- res[order(-Number),,]
# getting head of it
res <- head(res,10)

# we have to treat it as data frame to check if its good
check(as.data.frame(res)==BadgesT1Good)


#===================================================================================================
users <- data.table(Users)
posts <- data.table(Posts)

# we select spiecified fields from both tables
postsIds <- posts[,list(OwnerUserId),]

usersSmaller <- users[,list(Id,Location),]

# we join innerly by Id and OwnerUserId
innerTable2 <- usersSmaller[postsIds,on=list(Id==OwnerUserId),nomatch=0]

# filtering by location, grouping and counting rows, ordering and getting head
outerTable2<- innerTable2[Location!='',,]
outerTable2<-outerTable2[,list(Count=.N),by="Location"]
outerTable2<-outerTable2[order(-Count),,]
outerTable2<-head(outerTable2,10)

check(as.data.frame(outerTable2)==T2Good)

#===================================================================================================

# we get wanted fields, filter it, group by and getting number of rows
AnsCount <- posts[,list(ParentId,PostTypeId),]
AnsCount <- AnsCount[PostTypeId==2,,]
AnsCount <- AnsCount[,list(AnswerCount=.N),by="ParentId"]

# getting desired fields and joining
postsIdOwner <- posts[,list(Id,OwnerUserId),]

PostAuth<- postsIdOwner[AnsCount,on=list(Id==ParentId),nomatch=0]

# selecting fields
users3<-users[,list(AccountId,DisplayName,Location),]

# joining again
outerTable3 <- PostAuth[users3,on=list(OwnerUserId==AccountId),nomatch=0]

# grouping, ordering, getting head and rename
outerTable3 <- outerTable3[,list(AverageAnswersCount=mean(AnswerCount)),by=list(OwnerUserId,DisplayName,Location)]
outerTable3<- outerTable3[order(-AverageAnswersCount,-OwnerUserId),,]
outerTable3<-head(outerTable3,10)
setnames(outerTable3,"OwnerUserId","AccountId")

check(as.data.frame(outerTable3)==T3Good)

#===================================================================================================

votes<-data.table(Votes)

#creating additional column and getting columns we want, filtering, grouping and counting rows
UpVotesPerYear<-votes[,list(PostId,VoteTypeId,Year=strftime(CreationDate,format = '%Y')),]
UpVotesPerYear<-UpVotesPerYear[VoteTypeId==2,,]
UpVotesPerYear<-UpVotesPerYear[,list(Count=.N),by=list(PostId,Year)]

# seelcting columns and filtering rows
postsTitleId<-posts[PostTypeId==1,list(Title,Id),]

# joining tables
outerTable4 <- postsTitleId[UpVotesPerYear,on=list(Id==PostId),nomatch=0]

# selecting columns
postsHelper<-outerTable4[,list(Title,Year,Count),]

# grouping and getting max
outerTable4 <-postsHelper[,list(Count=max(Count)),by="Year"]

# ordering output
outerTable4 <- outerTable4[order(Year),,]

# joining tables again
outerTable4 <- outerTable4[postsHelper,on=list(Year,Count),nomatch=0]

#
outerTable4 <- outerTable4[,list(Title,Year,Count),]

check(as.data.frame(outerTable4)==T4Good)

#===================================================================================================

# filtering votes, creating new columns
VotesByAgeTable<- votes[VoteTypeId==1 | VoteTypeId==2 | VoteTypeId==5,,]
VotesByAgeTable<-VotesByAgeTable[,VoteDate := fcase(strftime(CreationDate,format = '%Y')=='2021','new',strftime(CreationDate,format = '%Y')=='2020','new',default = 'old'),]

# grouping by and counting rows
VotesByAgeTable <- VotesByAgeTable[,list(Total=.N),by=list(PostId,VoteDate)]

# grouping by and creating new columns with cases
VotesByAgeTable2 <- VotesByAgeTable[,list(NewVotes=max(fcase(VoteDate=='new',Total,default = as.integer(0))),OldVotes=max(fcase(VoteDate=='old',Total,default = as.integer(0))),Votes=sum(Total)),by=list(PostId)]

# filtering
VotesByAgeTable2 <- VotesByAgeTable2[NewVotes==0,,]

# filtering posts and getting columns we need
postsTask5 <- posts[PostTypeId==1,list(Title,Id),]

# joining tables
outerTable5 <- VotesByAgeTable2[postsTask5,on=list(PostId==Id),nomatch=0]

# ordering table and getting columns we want
outerTable5<-outerTable5[order(-OldVotes),list(Title,OldVotes),]

# getting 10 rows
outerTable5<-head(outerTable5,10)


check(as.data.frame(outerTable5)==T5Good)

