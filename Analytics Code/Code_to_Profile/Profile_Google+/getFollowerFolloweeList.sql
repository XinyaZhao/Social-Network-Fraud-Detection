create external table followers(follower string) 
	location '/user/xz1863/proj/gplus';

create table friendship(target string, follower string);
create table friendship_etl(target string, follower string)

insert into table friendship  
	select split(split(INPUT__FILE__NAME,'gplus/')[1],'[.]')[0],* 
	from followers;

select * from friendship limit 10;

/* data cleansing */ 
insert into table friendship_etl
	select * from friendship 
	where friendship.target is not NULL and friendship.follower is not NULL;

select * from friendship_etl limit 10;

insert overwrite directory '/user/xz1863/project/gplus/'
	row format delimited fields terminated by '\t' 
	lines terminated by '\n' 
	stored as TEXTFILE 
	select * from friendship_etl;

