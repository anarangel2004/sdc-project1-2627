package broadcast;

import pt.unl.fct.di.novasys.babel2.channels.BabelMessage;


public interface BrbPayloadPolicy {

    String instanceId(BabelMessage payload);

    String valueId(BabelMessage payload);

    boolean isValid(BabelMessage payload);
}