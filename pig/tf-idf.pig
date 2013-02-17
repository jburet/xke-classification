register ../stopword-udf/target/stopword-udf-1.0-SNAPSHOT.jar

%default input ../data/

-- Load data from input dir
article = LOAD '$input/' using PigStorage('\n', '-tagsource') as (filename:chararray, line:chararray);

words = FOREACH article GENERATE filename, FLATTEN(TOKENIZE(line)) AS word;

-- transform all to lower case
tokens = FOREACH words GENERATE filename, LOWER(word) as token;
 
-- Filter withspace, and after stop word etc...
filtered_empty_words = FILTER tokens BY token MATCHES '\\w+';
filtered_words = FILTER filtered_empty_words by NotStopWord(token);

-- word count by documents
doc_word_totals = FOREACH (GROUP filtered_words by (filename, token)) GENERATE
                    FLATTEN(group) as (filename, token), 
                    COUNT_STAR(filtered_words) as doc_total;


-- calculate size of each document
pre_term_counts = FOREACH (GROUP doc_word_totals by filename) generate
                    group AS filename,
                    FLATTEN(doc_word_totals.(token, doc_total)) as (token, doc_total), 
                    SUM(doc_word_totals.doc_total) as doc_size;


-- Calculate the TF
term_freqs = foreach pre_term_counts generate filename as filename,
               token as token,
               ((double)doc_total / (double)doc_size) AS term_freq;


-- count document using each token
token_usages = foreach (group term_freqs by token) generate
                 FLATTEN(term_freqs) as (filename, token, term_freq),
                 COUNT_STAR(term_freqs) as num_docs_with_token;

--calculate nb of doc (can be a job parameter instead)
just_ids = FOREACH article GENERATE filename;
ndocs = FOREACH (GROUP just_ids all) GENERATE COUNT(just_ids) as total_docs;

-- and tf-idf
tfidf_all = FOREACH token_usages {
  idf = LOG((double)ndocs.total_docs/(double)num_docs_with_token);
  tf_idf = (double)term_freq * idf;
  GENERATE filename as filename,
    token as score,
    (chararray)tf_idf as value:chararray;
};

-- take 5 most important term by docs
label_by_article = FOREACH (GROUP tfidf_all BY filename) {
  sorted = ORDER tfidf_all BY value DESC;
  top_5_topics = LIMIT sorted 5;
  GENERATE group, top_5_topics.(score, value);
}

DUMP label_by_article;
--DUMP filtered_words
