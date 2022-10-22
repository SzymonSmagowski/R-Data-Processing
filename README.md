# R-Data-Processing
Changing SQL queries to R language on 3 ways:
1. dplyr
2. data table
3. data frame

Queries may be overcomplicated, so my task is also to simplify them.
# Queries:
# -- 1 --
SELECT
Name,
COUNT(*) AS Number,
MIN(Class) AS BestClass
FROM Badges
GROUP BY Name
ORDER BY Number DESC
LIMIT 10
# -- 2 --
SELECT Location, COUNT(*) AS Count
FROM (
SELECT Posts.OwnerUserId, Users.Id, Users.Location
FROM Users
JOIN Posts ON Users.Id = Posts.OwnerUserId
)
WHERE Location NOT IN ('')
GROUP BY Location
ORDER BY Count DESC
LIMIT 10
# -- 3 --
SELECT
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
LIMIT 10
# -- 4 --
SELECT
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
ORDER BY Year ASC
# -- 5 --
SELECT
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
LIMIT 10
