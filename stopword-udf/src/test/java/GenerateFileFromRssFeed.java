import org.junit.Test;

import javax.xml.stream.XMLInputFactory;
import javax.xml.stream.XMLStreamException;
import javax.xml.stream.XMLStreamReader;
import javax.xml.stream.events.XMLEvent;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URL;
import java.net.URLConnection;

public class GenerateFileFromRssFeed {

    @Test
    public void generate() throws IOException, XMLStreamException {
        XMLInputFactory xmlif = XMLInputFactory.newInstance();

        xmlif.setProperty("javax.xml.stream.isCoalescing", Boolean.TRUE);
        xmlif.setProperty("javax.xml.stream.isReplacingEntityReferences", Boolean.TRUE);
        URL url = new URL("http://blog.xebia.fr/feed/");
        URLConnection urlConnection = url.openConnection();
        BufferedReader reader = new BufferedReader(new InputStreamReader(urlConnection.getInputStream()));
        XMLStreamReader xmlsr = xmlif.createXMLStreamReader(reader);
        int eventType;

        while (xmlsr.hasNext()) {

            eventType = xmlsr.next();
            switch (eventType) {
                case XMLEvent.START_ELEMENT:
                    System.out.println(xmlsr.getName());
                    break;
                case XMLEvent.CHARACTERS:
                    String chaine = xmlsr.getText();
                    if (!xmlsr.isWhiteSpace()) {
                        System.out.println("\t->\"" + chaine + "\"");
                    }
                    break;
                default:
                    break;
            }
        }


        reader.close();
    }

}
