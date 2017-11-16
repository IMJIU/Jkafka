package kafka.controller.channel;

import kafka.api.RequestOrResponse;
import kafka.func.ActionP;
import kafka.func.ActionP2;

/**
 * @author zhoulf
 * @create 2017-11-06 18:09
 **/
public class Callbacks {
    public ActionP<RequestOrResponse> leaderAndIsrResponseCallback;
    public ActionP<RequestOrResponse> updateMetadataResponseCallback;
    public ActionP2<RequestOrResponse, Integer> stopReplicaResponseCallback;

    public Callbacks(ActionP<RequestOrResponse> leaderAndIsrResponseCallback,
                     ActionP<RequestOrResponse> updateMetadataResponseCallback,
                     ActionP2<RequestOrResponse, Integer> stopReplicaResponseCallback) {
        this.leaderAndIsrResponseCallback = leaderAndIsrResponseCallback;
        this.updateMetadataResponseCallback = updateMetadataResponseCallback;
        this.stopReplicaResponseCallback = stopReplicaResponseCallback;
    }


}