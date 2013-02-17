import org.apache.pig.EvalFunc;
import org.apache.pig.data.*;
import org.apache.pig.impl.logicalLayer.schema.Schema;

import java.io.IOException;
import java.util.Arrays;

public class PCARD extends EvalFunc<DataBag> {
    @Override
    public DataBag exec(Tuple input) throws IOException {
        // tuple must contain two bag
        if (input.isNull(0) || DataType.BAG != input.getType(0)) {
            throw new IOException("Usage PCARD(bag, bag)");
        }

        DataBag bag1 = (DataBag) input.get(0);

        DataBag res = new DefaultDataBag();
        for (Tuple t1 : bag1) {
            for (Tuple t2 : bag1) {
                if (!t1.equals(t2)) {
                    res.add(TupleFactory.getInstance().newTuple(Arrays.asList(new Object[]{t1, t2})));
                }
            }
        }
        return res;
    }

    @Override
    public Schema outputSchema(Schema input) {
        return super.outputSchema(input);
    }
}