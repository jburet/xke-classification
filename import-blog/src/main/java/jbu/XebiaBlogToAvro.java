package jbu;

import org.apache.avro.Schema;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.util.Utf8;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;

import java.io.*;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.URLConnection;

public class XebiaBlogToAvro {

    public static void main(String... args) {
        try {
            // configure out file
            Schema schema = Schema.parse(new File("import-blog/Schema.avsc"));
            GenericDatumWriter<GenericRecord> w = new GenericDatumWriter<GenericRecord>(schema);
            DataFileWriter<GenericRecord> dataFileWriter = new DataFileWriter<GenericRecord>(w);
            dataFileWriter.create(schema, new File("import-blog/article.avro"));

            // crawl blog for id

            BufferedReader bin = null;
            int page = 1;
            while (true) {
                try {
                    URL currentUrl = new URL("http://blog.xebia.fr/wp-json-api/get_recent_posts/?count=10&page=" + page);
                    URLConnection conn = currentUrl.openConnection();
                    bin = new BufferedReader(new InputStreamReader(conn.getInputStream()));
                    JSONParser jp = new JSONParser();
                    JSONObject res = (JSONObject) jp.parse(bin);
                    if (!res.containsKey("posts")) {
                        break;
                    }
                    JSONArray posts = (JSONArray) res.get("posts");
                    if (posts.size() == 0) {
                        break;
                    }
                    System.out.println("Page : "+page+" "+posts.size()+" article to import");
                    for (Object postObj : posts) {
                        JSONObject post = (JSONObject) postObj;
                        Long id = (Long) post.get("id");

                        // Download article and write to avro
                        writePost(schema, dataFileWriter, id);
                    }
                    page++;
                } catch (ParseException e) {
                    e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
                } finally {
                    if (bin != null) {
                        try {
                            bin.close();
                        } catch (IOException e) {
                            e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
                        }
                    }
                }
            }


            dataFileWriter.close();


        } catch (MalformedURLException e) {
            e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
        } catch (IOException e) {
            e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
        }
    }

    private static void writePost(Schema schema, DataFileWriter<GenericRecord> dataFileWriter, Long id) {
        System.out.println("Download post : " + id);
        BufferedReader bin = null;
        try {
            URL currentUrl = new URL("http://blog.xebia.fr/wp-json-api/get_post/?post_id=" + id);
            URLConnection conn = currentUrl.openConnection();
            bin = new BufferedReader(new InputStreamReader(conn.getInputStream()));
            JSONParser jp = new JSONParser();
            JSONObject res = (JSONObject) jp.parse(bin);
            JSONObject post = (JSONObject) res.get("post");

            String content = JSONObject.escape((String) post.get("content"));
            String title = (String) post.get("slug");
            GenericRecord article = new GenericData.Record(schema);
            article.put("title", new Utf8(title));
            article.put("content", new Utf8(content));

            dataFileWriter.append(article);

            try {
                Thread.sleep(200);
            } catch (InterruptedException e) {
                e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
            }

        } catch (MalformedURLException e) {
            e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
        } catch (IOException e) {
            e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
        } catch (ParseException e) {
            e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
        } finally {
            if (bin != null) {
                try {
                    bin.close();
                } catch (IOException e) {
                    e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
                }
            }
        }
    }

}
