package org.apache.edwardyoon;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

import java.util.Map;

public class ExclamationBolt extends BaseRichBolt {
    // To output tuples from this bolt to the next stage bolts, if any
    OutputCollector _collector;

    @Override
    public void prepare(
            Map map,
            TopologyContext topologyContext,
            OutputCollector         collector)
    {
        // save the output collector for emitting tuples
        _collector = collector;
    }

    @Override
    public void execute(Tuple tuple)
    {
        //**** ADD COMPONENT ID

        /*
         * Use component id to modify behavior
         */

        // get the column word from tuple
        String word = tuple.getString(0);

        // build the word with the exclamation marks appended
        StringBuilder exclamatedWord = new StringBuilder();
        exclamatedWord.append(word).append("!!!");

        // emit the word with exclamations
        _collector.emit(tuple, new Values(exclamatedWord.toString()));
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer)
    {
        // tell storm the schema of the output tuple for this spout

        // tuple consists of a single column called 'exclamated-word'
        declarer.declare(new Fields("exclamated-word"));
    }
}