install.packages("sqldf")
install.packages("dplyr")
library(weatherData)
library(sqldf)
library(dplyr)
#installing packages
#check function - checking every field of data frame, if one is false then result is false

check <- function(a){
  c = all(any(a==FALSE))
  r= all(any(a==FALSE,2))
  return (!(c&r))
}
#dplyr solution:
#setting working directory
getwd()
setwd("/Users/Szymon/Desktop/rstudio")

#loading data from file
Badges<-read.csv(file = 'badges.csv')

#good result
BadgesT1Good<-sqldf("SELECT
      Name,
      COUNT(*) AS Number,
      MIN(Class) AS BestClass
      FROM Badges
      GROUP BY Name
      ORDER BY Number DESC
      LIMIT 10")

#Here firstly I group by name from Badges, then summarise rows and create new column of minimum from Class,
#then arrange in correct way and get 10 first rows
BadgesT1<-Badges %>%
  group_by(Name) %>%
  summarise(Number=n(),BestClass=min(Class)) %>%
  arrange(desc(Number)) %>%
  head(10)

#checking result
check(BadgesT1==BadgesT1Good)
#===================================================================================================
Users<-read.csv(file = 'users.csv')
Posts<-read.csv(file = 'posts.csv')
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

#Inner variable symbolise inner brackets from sql result
#Here I select columns from user, and just by variable
InnerT2 <- Users %>%
  select(Id,Location) %>%
  inner_join(Posts,by=c("Id"="OwnerUserId"))

#Here there is simle group by, summarise rows, filter by condition, arrange by count from the greatest and get head
T2Dplyr <- InnerT2 %>%
  group_by(Location) %>%
  summarise(Count=n()) %>%
  filter(!Location %in% '') %>%
  arrange(desc(Count)) %>%
  head(10)


check(T2Dplyr==T2Good)
#===================================================================================================
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

#Like a name suggest this is inner inner bracket from sql
#First grouping by Parent id, filter results and summarise
InnerInnerT3 <- Posts %>%
  group_by(ParentId) %>%
  filter(PostTypeId==2) %>%
  summarise(AnswersCount=n())

#then joining by Id and Parent Id
InnerT3 <- InnerInnerT3 %>%
  inner_join(select(Posts, Id, OwnerUserId),by=c("ParentId"="Id"))

#Now joining as well, grouping by all of the variables - since the result does not change,
#summarise and arrange like always
T3Dplyr <- InnerT3 %>%
  inner_join(select(Users,DisplayName,AccountId,Location),by=c("OwnerUserId"="AccountId"))%>%
  group_by(OwnerUserId,DisplayName,Location)%>%
  summarise(AverageAnswersCount=mean(AnswersCount))%>%
  arrange(desc(AverageAnswersCount),desc(OwnerUserId)) %>%
  head(10)

T3Dplyr<-rename(T3Dplyr,AccountId=OwnerUserId)

check(T3Dplyr==T3Good)

#===================================================================================================
Votes<- read.csv(file = 'votes.csv')
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

# filter by VoteTypeId, adding one column, grouping by and summarise rows
T4Inner<-Votes %>%
  filter(VoteTypeId=='2') %>%
  mutate(Year=strftime(CreationDate,format = '%Y')) %>%
  group_by(PostId,Year)%>%
  summarise(Count=n())

#inner join by PostId and Id, setting output
T4Helper <- T4Inner %>%
  inner_join(select(Posts,Title,PostTypeId,Id)%>%filter(PostTypeId==1),by=c("PostId"="Id"))%>%
  select(-PostTypeId)

#Ungrouping and setting output
T4Helper2 <- T4Helper%>%
  ungroup()%>%
  select(-PostId)

# adding additional column with group by
T4Helper3 <- T4Helper2 %>%
  group_by(Year,Title)%>%
  mutate(Count=max(Count))
  
# inner join, grouping by year, summarise and arrange
T4almost <- T4Inner %>%
  inner_join(select(Posts,Title,PostTypeId,Id),by=c("PostId"="Id")) %>%
  filter(PostTypeId==1) %>%
  group_by(Year)%>%
  summarise(Count=max(Count))%>%
  arrange(Year)

# joining helper variables
T4Dplyr <- T4almost%>%
  inner_join(T4Helper3)

# setting up result
T4Dplyr <- T4Dplyr%>%
  select(Title,Year,Count)
check(T4Good==T4Dplyr)

#===================================================================================================
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

# Filter by VoteTypeId, adding a column, grouping by and summarise rows
VotesByAge<-Votes%>%
  filter(VoteTypeId==1|VoteTypeId==2|VoteTypeId==5)%>%
  mutate(VoteDate=case_when(strftime(CreationDate,format = '%Y')=='2021' ~ 'new',strftime(CreationDate,format = '%Y')=='2020' ~ 'new',TRUE ~ 'old'))%>%
  group_by(PostId,VoteDate)%>%
  summarise(Total=n())
# adding 2 columns, grouping, summarising, filtering and arranging
VotesByAge2<- VotesByAge%>%
  mutate(NewVotes=case_when(VoteDate=='new'~Total,TRUE~as.integer(0)))%>%
  mutate(OldVotes=case_when(VoteDate=='old'~Total,TRUE~as.integer(0)))%>%
  group_by(PostId)%>%
  summarise(Votes=sum(Total),NewVotes=max(NewVotes),OldVotes=max(OldVotes))%>%
  filter(NewVotes==0)%>%
  arrange(desc(OldVotes))

# Filtering, joining, selecting and arranging
T5Dplyr <- Posts%>%
  filter(PostTypeId==1)%>%
  inner_join(select(VotesByAge2,PostId,OldVotes),by=c("Id"="PostId"))%>%
  select(Title,OldVotes)%>%
  arrange(desc(OldVotes))%>%
  head(10)
check(T5Dplyr==T5Good)