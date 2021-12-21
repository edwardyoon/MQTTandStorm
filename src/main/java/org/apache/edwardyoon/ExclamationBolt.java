/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
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
