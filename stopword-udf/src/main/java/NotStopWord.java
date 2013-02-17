import org.apache.pig.FilterFunc;
import org.apache.pig.data.Tuple;

import java.io.IOException;
import java.util.HashSet;
import java.util.Set;


public class NotStopWord extends FilterFunc {

    private static final Set<String> words = new HashSet<String>();

    static {
        words.add("au");
        words.add("aux");
        words.add("avec");
        words.add("ce");
        words.add("ces");
        words.add("dans");
        words.add("de");
        words.add("des");
        words.add("du");
        words.add("elle");
        words.add("en");
        words.add("et");
        words.add("eux");
        words.add("il");
        words.add("je");
        words.add("la");
        words.add("le");
        words.add("leur");
        words.add("lui");
        words.add("ma");
        words.add("mais");
        words.add("me");
        words.add("même");
        words.add("mes");
        words.add("moi");
        words.add("mon");
        words.add("ne");
        words.add("nos");
        words.add("notre");
        words.add("nous");
        words.add("on");
        words.add("ou");
        words.add("par");
        words.add("pas");
        words.add("pour");
        words.add("qu");
        words.add("que");
        words.add("qui");
        words.add("sa");
        words.add("se");
        words.add("ses");
        words.add("son");
        words.add("sur");
        words.add("ta");
        words.add("te");
        words.add("tes");
        words.add("toi");
        words.add("ton");
        words.add("tu");
        words.add("un");
        words.add("une");
        words.add("vos");
        words.add("votre");
        words.add("vous");
        words.add("été");
        words.add("étée");
        words.add("étées");
        words.add("étés");
        words.add("étant");
        words.add("suis");
        words.add("es");
        words.add("est");
        words.add("sommes");
        words.add("êtes");
        words.add("sont");
        words.add("serai");
        words.add("seras");
        words.add("sera");
        words.add("serons");
        words.add("serez");
        words.add("seront");
        words.add("serais");
        words.add("serait");
        words.add("serions");
        words.add("seriez");
        words.add("seraient");
        words.add("étais");
        words.add("était");
        words.add("étions");
        words.add("étiez");
        words.add("étaient");
        words.add("fus");
        words.add("fut");
        words.add("fûmes");
        words.add("fûtes");
        words.add("furent");
        words.add("sois");
        words.add("soit");
        words.add("soyons");
        words.add("soyez");
        words.add("soient");
        words.add("fusse");
        words.add("fusses");
        words.add("fût");
        words.add("fussions");
        words.add("fussiez");
        words.add("fussent");
        words.add("ayant");
        words.add("eu");
        words.add("eue");
        words.add("eues");
        words.add("eus");
        words.add("ai");
        words.add("as");
        words.add("avons");
        words.add("avez");
        words.add("ont");
        words.add("aurai");
        words.add("auras");
        words.add("aura");
        words.add("aurons");
        words.add("aurez");
        words.add("auront");
        words.add("aurais");
        words.add("aurait");
        words.add("aurions");
        words.add("auriez");
        words.add("auraient");
        words.add("avais");
        words.add("avait");
        words.add("avions");
        words.add("aviez");
        words.add("avaient");
        words.add("eut");
        words.add("eûmes");
        words.add("eûtes");
        words.add("eurent");
        words.add("aie");
        words.add("aies");
        words.add("ait");
        words.add("ayons");
        words.add("ayez");
        words.add("aient");
        words.add("eusse");
        words.add("eusses");
        words.add("eût");
        words.add("eussions");
        words.add("eussiez");
        words.add("eussent");
        words.add("ceci");
        words.add("celà");
        words.add("cet");
        words.add("cette");
        words.add("ici");
        words.add("ils");
        words.add("les");
        words.add("leurs");
        words.add("quel");
        words.add("quels");
        words.add("quelle");
        words.add("quelles");
        words.add("sans");
        words.add("soi");
        words.add("top");
        words.add("class");
        words.add("for");
        words.add("while");
        words.add("if");
        words.add("result");
        words.add("sync");
        words.add("framework");
        words.add("var");
        words.add("int");
        words.add("double");
        words.add("float");
        words.add("boolean");
        words.add("_blank");
        words.add("not");
        words.add("public");
        words.add("final");
        words.add("new");
        words.add("tous");
        words.add("tout");
        words.add("si");
        words.add("by");
        words.add("plus");
        words.add("permet");
        words.add("quelque");
        words.add("quelques");
        words.add("chaque");
        words.add("plus");
        words.add("moins");
        words.add("height");
        words.add("width");
        words.add("src");
        words.add("style");
        words.add("0px");
        words.add("1pm");

    }


    @Override
    public Boolean exec(Tuple input) throws IOException {
        if (input.get(0) != null) {
            String word = ((String) input.get(0)).trim().toLowerCase();
            // Remove word starting with number
            if (word.matches("[0-9].*")) {
                return false;
            }
            // remove one letter word
            if (word.length() <= 1) {
                return false;
            }
            return !words.contains(word);
        }
        return false;
    }
}
