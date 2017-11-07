package kafka.controller.channel;

import kafka.api.RequestOrResponse;
import kafka.func.ActionP;

/**
 * @author zhoulf
 * @create 2017-11-06 18:09
 **/
public class Callbacks {
    public ActionP<RequestOrResponse> leaderAndIsrResponseCallback;
    public ActionP<RequestOrResponse> updateMetadataResponseCallback;
    public ActionP<RequestOrResponse> stopReplicaResponseCallback;

    public Callbacks(ActionP<RequestOrResponse> leaderAndIsrResponseCallback, ActionP<RequestOrResponse> updateMetadataResponseCallback, ActionP<RequestOrResponse> stopReplicaResponseCallback) {
        this.leaderAndIsrResponseCallback = leaderAndIsrResponseCallback;
        this.updateMetadataResponseCallback = updateMetadataResponseCallback;
        this.stopReplicaResponseCallback = stopReplicaResponseCallback;
    }


}