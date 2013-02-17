register lib/*.jar
register ../BagPCard/target/BagPCard-1.0-SNAPSHOT.jar

%default input article_label.avro
%default output article_similarity.avro

article = LOAD '$input' using org.apache.pig.piggybank.storage.avro.AvroStorage() as (title:chararray, labels: {(value: chararray,score: chararray)});

-- Emit label and article title
label_article = FOREACH article GENERATE title as title, FLATTEN(labels.value) as label;

-- Group by label
label_similarity = GROUP label_article by label;

-- filter group with only one article
filter_label_similarity = FILTER label_similarity BY COUNT_STAR(label_article) > 1;

-- Create title similarity bag
title_similarity = FOREACH filter_label_similarity GENERATE label_article.title as titles;

-- Create title pair
title_pair = FOREACH title_similarity GENERATE FLATTEN(PCARD(titles)) as (titlep:chararray, titles:chararray);

-- count each pair
grouped_pair = GROUP title_pair BY (titlep, titles);
count_pair = FOREACH grouped_pair GENERATE group as pair, COUNT_STAR(title_pair) as occurence; 

-- recreate strut with title, title similaire and count
pair = FOREACH count_pair GENERATE FLATTEN(pair.$0) as title, FLATTEN(pair.$1) as titles, occurence as occurence;

-- Filter need a least two common label
filtered_pair = FILTER pair BY occurence > 2;

-- ordered pair
ordered_pair = ORDER filtered_pair BY title ASC, occurence DESC;

-- Order by title and occurence

DUMP ordered_pair;
