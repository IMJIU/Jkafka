package kafka.controller.channel;

import kafka.api.RequestOrResponse;
import kafka.func.ActionP;
import kafka.func.ActionP2;

/**
 * @author zhoulf
 * @create 2017-11-06 18:09
 **/
public class CallbackBuilder {
    public ActionP<RequestOrResponse> leaderAndIsrResponseCbk;
    public ActionP<RequestOrResponse> updateMetadataResponseCbk;
    public ActionP2<RequestOrResponse, Integer> stopReplicaResponseCbk;

    public CallbackBuilder leaderAndIsrCallback(ActionP<RequestOrResponse> cbk) {
        leaderAndIsrResponseCbk = cbk;
        return this;
    }

    public CallbackBuilder updateMetadataCallback(ActionP<RequestOrResponse> cbk) {
        updateMetadataResponseCbk = cbk;
        return this;
    }

    public CallbackBuilder stopReplicaCallback(ActionP2<RequestOrResponse, Integer> cbk) {
        stopReplicaResponseCbk = cbk;
        return this;
    }

    public Callbacks build() {
        return new Callbacks(leaderAndIsrResponseCbk, updateMetadataResponseCbk, stopReplicaResponseCbk);
    }
}