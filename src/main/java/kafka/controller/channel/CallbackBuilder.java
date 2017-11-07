package kafka.controller.channel;

import kafka.api.RequestOrResponse;
import kafka.func.ActionP;

/**
 * @author zhoulf
 * @create 2017-11-06 18:09
 **/
public class CallbackBuilder {
    ActionP<RequestOrResponse> leaderAndIsrResponseCbk;
    ActionP<RequestOrResponse> updateMetadataResponseCbk;
    ActionP<RequestOrResponse> stopReplicaResponseCbk;

    public CallbackBuilder leaderAndIsrCallback(ActionP<RequestOrResponse> cbk) {
        leaderAndIsrResponseCbk = cbk;
        return this;
    }

    public CallbackBuilder updateMetadataCallback(ActionP<RequestOrResponse> cbk) {
        updateMetadataResponseCbk = cbk;
        return this;
    }

    public CallbackBuilder stopReplicaCallback(ActionP<RequestOrResponse> cbk) {
        stopReplicaResponseCbk = cbk;
        return this;
    }

    public Callbacks build() {
        return new Callbacks(leaderAndIsrResponseCbk, updateMetadataResponseCbk, stopReplicaResponseCbk);
    }
}