TRUNCATE {{params.agg_table_name}};
INSERT INTO {{params.agg_table_name}} SELECT * FROM

(SELECT d.title,d.name from

( SELECT c.title,c.name, RANK () OVER (PARTITION BY c.title ORDER BY c.age DESC)  as age_rank

 FROM
(SELECT b.title , a.name , a.birth_year,
 CASE WHEN a.age_decider = 'BBY' THEN CAST(LEFT(a.birth_year,-3) as FLOAT)
								 ELSE CAST(LEFT(a.birth_year,-3) as FLOAT)*-1 end as age

FROM (SELECT name,birth_year,RIGHT(birth_year,3) as age_decider,
replace(unnest(string_to_array ((replace(replace(replace(films,'''',''),'[',''),']','')),',')),' ','')
as film

	  FROM {{params.people_table_name}}) a
INNER JOIN {{params.films_table_name}} as b on a.film=b.url
WHERE a.birth_year != 'unknown') c )d

WHERE age_rank =1) e





