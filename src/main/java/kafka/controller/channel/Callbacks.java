package kafka.controller.channel;

import kafka.api.RequestOrResponse;
import kafka.func.ActionP;
import kafka.func.ActionP2;

/**
 * @author zhoulf
 * @create 2017-11-06 18:09
 **/
public class Callbacks {
    public ActionP2<RequestOrResponse,Object> leaderAndIsrResponseCallback;
    public ActionP2<RequestOrResponse,Object> updateMetadataResponseCallback;
    public ActionP2<RequestOrResponse,Object> stopReplicaResponseCallback;

    public Callbacks(ActionP2<RequestOrResponse,Object> leaderAndIsrResponseCallback,
                     ActionP2<RequestOrResponse,Object> updateMetadataResponseCallback,
                     ActionP2<RequestOrResponse,Object> stopReplicaResponseCallback) {
        this.leaderAndIsrResponseCallback = leaderAndIsrResponseCallback;
        this.updateMetadataResponseCallback = updateMetadataResponseCallback;
        this.stopReplicaResponseCallback = stopReplicaResponseCallback;
    }


}